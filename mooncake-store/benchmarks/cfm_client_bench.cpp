// CFM benchmark that models the vLLM KV-cache call path in the embedded CFM
// architecture.
//
// CFM is a component of every SubMaster; there is no standalone CFM Master and
// no credential. A reporting client observes keys (KV blocks) and sends metric
// batches over the SubMaster's regular coro_rpc endpoint. The SubMaster merges
// reports into its local runtime, then every merged report drives a local
// analysis -> decision -> execution cycle (eviction/prefetch/promotion/
// admission through the storage-safe handlers) on the keys it owns.
//
// This benchmark exercises that path in the following modes:
//   - embedded (default): an in-process SubMaster runtime plays the owning
//     CFM component. Reports are delivered in-process; the runtime's
//     report-driven worker executes policy per report, and a final manual
//     Execute emulates the production high-watermark trigger, so the benchmark
//     prints report latency and the resulting eviction/prefetch/admission
//     handler activity.
//   - remote (--cfm_endpoint=host:port): reports go over coro_rpc to a single
//     SubMaster CFM receiver.
//   - remote via etcd (--cfm_endpoint=etcd://connstring): resolves the cluster
//     like a Store client, then buckets each key to its owning SubMaster.
//     The receiving side is not observable here, so only client-side latency
//     is reported.
//
// Remote policy execution is only observable when the SubMaster actually owns
// the reported keys: its eviction/promotion/prefetch handlers operate on real
// replicas, so a report-only run leaves the master-side counters at zero. When
// --master_server and --num_keys are provided, a real-data seeding stage runs
// first ("先种子后仿真"): it writes a batch of real KV objects through
// RealClient (keys share the simulated KvKey naming and tenant) and reads a
// subset back to simulate access heat, then the simulated vLLM request stream
// reports on those same keys.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <numa.h>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "cvm/cvm_types.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"
#include "io_pattern/cfm_ownership_client.h"
#include "io_pattern/cfm_protocol.h"
#include "io_pattern/cfm_service.h"
#include "io_pattern/rpc_transport.h"
#include "io_pattern/runtime.h"
#include "real_client.h"
#include "types.h"
#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#endif

namespace {

using Clock = std::chrono::steady_clock;
using mooncake::ErrorCode;
using mooncake::TenantId;
using mooncake::toString;
using namespace mooncake::io_pattern;

DEFINE_uint64(requests, 20, "Number of vLLM-style inference requests");
DEFINE_uint64(prompt_tokens, 1024, "Input tokens in each inference request");
DEFINE_uint64(output_tokens, 128, "Decode tokens in each inference request");
DEFINE_uint64(tokens_per_block, 16, "Tokens represented by one KV block");
DEFINE_uint64(num_layers, 32, "Transformer layers represented per request");
DEFINE_uint64(kv_block_bytes, 256 * 1024,
              "Bytes in one layer/block KV-cache object");
DEFINE_uint64(num_sessions, 4, "Independent vLLM request sessions");
DEFINE_uint64(shared_prefix_tokens, 512,
              "Per-session prompt prefix reused by later requests");
DEFINE_uint64(report_capacity, 262144,
              "Maximum queued IO Pattern observations before reporting");
DEFINE_uint64(report_flush_wait_ms, 1100,
              "Grace period for report drain before local policy evaluation");
DEFINE_double(memory_used_ratio, 0.95,
              "Reported L1 memory use ratio; >= 0.90 triggers eviction");
DEFINE_string(tenant, "vllm-benchmark", "Tenant id");
DEFINE_string(node_id, "vllm-submaster-0",
              "CFM node/submaster id that owns the reported keys");
DEFINE_string(cfm_endpoint, "",
              "Remote SubMaster endpoint: either host:port or an HA entry "
              "(e.g. etcd://host:2379;host2:2379). Empty uses an embedded "
              "in-process SubMaster CFM component");
DEFINE_string(cfm_cluster_namespace, "",
              "CVM cluster namespace for etcd entry resolution; defaults to "
              "MC_STORE_CLUSTER_ID or mooncake_cluster (same rule as the "
              "etcd leader coordinator). When the cluster was started with a "
              "non-default cluster_id, pass the same value here");

// Real Store client parameters used by the optional real-data seeding stage.
// Flag names and defaults mirror stress_cluster_bench.cpp so an existing
// cluster invocation can be reused as-is. Seeding makes the SubMaster hold
// real replicas for the reported keys, which is what the IO Pattern eviction /
// promotion / prefetch handlers operate on (without real objects they remain
// no-ops even when reports trigger policy cycles).
DEFINE_string(master_server, "",
              "Master server address (host:port) for RealClient writes; empty "
              "disables real-data seeding");
DEFINE_string(local_hostname, "localhost",
              "Local hostname (with optional port, e.g. node1:12345)");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server URL for RealClient setup");
DEFINE_string(protocol, "tcp", "Transport protocol: tcp, rdma, ub");
DEFINE_string(device_name, "", "RDMA/UB device name (comma-separated)");
DEFINE_uint64(global_segment_size, 16ULL * 1024 * 1024 * 1024,
              "Global segment size in bytes (per store node)");
DEFINE_uint64(local_buffer_size, 512ULL * 1024 * 1024,
              "Local client buffer size in bytes");
DEFINE_bool(enable_ssd_offload, false,
            "Enable LOCAL_DISK offload when seeding real keys (requires the "
            "SubMaster to run with offload enabled)");
DEFINE_string(ssd_offload_path, "", "SSD offload directory path");
DEFINE_uint64(num_keys, 0,
              "Number of real keys to write during seeding (0 = disabled)");
DEFINE_uint64(value_size, 4ULL * 1024 * 1024,
              "Size in bytes of each seeded real KV object");
DEFINE_uint64(replica_num, 1, "Number of replicas for each seeded object");
DEFINE_bool(hard_pin, false, "Pin seeded objects (disable eviction of them)");
DEFINE_uint64(seed_get_keys, 0,
              "How many of the seeded keys to read back with get_into "
              "(0 = half of num_keys)");
// Report RPC timeout. A merged report can carry hundreds of thousands of
// observations; the old fixed 500 ms budget caused report_metric_batch RPC
// failures on large batches. Also bounds how long the ownership client waits
// per SubMaster delivery.
DEFINE_uint64(cfm_rpc_timeout_ms, 5000,
              "Timeout for each CFM report RPC (report_snapshot / "
              "report_metric_batch) in milliseconds");
// Client-side collector/analysis key budget. The default 100k cap drops
// observations once the simulated request stream exceeds it (seen as nonzero
// \"report drops\"); raise it to cover the whole run when reporting many keys.
DEFINE_uint64(max_analysis_keys, 100000,
              "Max merged keys kept/analyzed by the client runtime and the "
              "embedded SubMaster runtime (raise with the request stream size)");

uint64_t SteadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch())
            .count());
}

double ToMicroseconds(Clock::duration duration) {
    return std::chrono::duration<double, std::micro>(duration).count();
}

size_t BlockCount(uint64_t tokens) {
    return static_cast<size_t>((tokens + FLAGS_tokens_per_block - 1) /
                               FLAGS_tokens_per_block);
}

std::string KvKey(size_t session, size_t request, size_t layer, size_t block,
                  bool is_shared_prefix) {
    const auto owner = is_shared_prefix ? std::string("prefix")
                                        : std::string("request-") +
                                              std::to_string(request);
    return "vllm/" + FLAGS_node_id + "/session-" +
           std::to_string(session) + "/" + owner + "/layer-" +
           std::to_string(layer) + "/block-" + std::to_string(block);
}

// Sends reports straight into an embedded SubMaster's CFM component. This is
// the ownership-addressed path collapsed to the single owning SubMaster of a
// benchmark run, exercised without network.
class EmbeddedCfmTransport final : public CfmRpcTransport {
   public:
    explicit EmbeddedCfmTransport(std::shared_ptr<CfmService> service)
        : service_(std::move(service)) {}

    bool Send(std::string_view method, std::string_view payload,
              std::chrono::milliseconds) override {
        return service_ && service_->Send(method, payload, FLAGS_node_id);
    }

   private:
    std::shared_ptr<CfmService> service_;
};

#ifdef STORE_USE_ETCD
// Resolves the CVM cluster namespace used by --cfm_endpoint when it carries an
// etcd:// backend. Mirrors EtcdLeaderCoordinator::ResolveClusterNamespace:
// explicit flag wins, then MC_STORE_CLUSTER_ID, then mooncake_cluster.
std::string ResolveCvmNamespace() {
    if (!FLAGS_cfm_cluster_namespace.empty()) {
        return FLAGS_cfm_cluster_namespace;
    }
    const char* env_cluster_id = std::getenv("MC_STORE_CLUSTER_ID");
    if (env_cluster_id != nullptr && std::strlen(env_cluster_id) > 0) {
        return env_cluster_id;
    }
    return mooncake::DEFAULT_CLUSTER_ID;
}

// Key that stores the leader address for single-leader HA.
// Mirrors EtcdLeaderCoordinator::BuildMasterViewKey.
std::string BuildMasterViewKey(const std::string& cluster_namespace) {
    std::string normalized = cluster_namespace;
    if (!normalized.empty() && normalized.back() == '/') {
        normalized.pop_back();
    }
    return "mooncake-store/" + normalized + "/master_view";
}
#endif  // STORE_USE_ETCD

// If --cfm_endpoint names a single SubMaster directly ("host:port") this
// returns an ownership resolver that routes every key to it. If it is an
// etcd:// entry, it resolves the cluster like a Store client: a present
// leader master_view yields a single target; otherwise the CVM
// /cvm/<ns>/masters registry plus slot ownership is used to bucket keys to
// their owning SubMaster. Returns an empty resolver on any resolution failure
// (the caller aborts instead of hanging).
SubmasterEndpointResolver ResolveCfmEndpointOwnership() {
    const std::string entry = FLAGS_cfm_endpoint;
    const size_t scheme_pos = entry.find("://");
    if (scheme_pos == std::string::npos) {
        // Plain host:port -> every observed key belongs to this single
        // SubMaster (the equivalent of the old single-endpoint remote mode).
        const std::string endpoint = entry;
        return [endpoint](const TenantId&, const std::string&)
                   -> std::optional<std::string> { return endpoint; };
    }
#ifndef STORE_USE_ETCD
    LOG(FATAL) << "cfm_endpoint entry '" << entry
               << "' requires a build with STORE_USE_ETCD; pass host:port "
                  "instead";
    return {};
#else
    const std::string scheme = entry.substr(0, scheme_pos);
    if (scheme != "etcd") {
        LOG(FATAL) << "cfm_endpoint backend '" << scheme
                   << "' is not supported; use host:port or etcd://connstring";
        return {};
    }
    const std::string connstring = entry.substr(scheme_pos + 3);
    const std::string cluster_namespace = ResolveCvmNamespace();

    ErrorCode err = mooncake::EtcdHelper::ConnectToEtcdStoreClient(connstring);
    if (err != ErrorCode::OK) {
        LOG(FATAL) << "cfm_endpoint: failed to connect etcd '" << connstring
                   << "': " << toString(err);
        return {};
    }

    // Single-leader HA: leader master_view holds the master address.
    const std::string view_key = BuildMasterViewKey(cluster_namespace);
    std::string leader_address;
    mooncake::EtcdRevisionId revision = 0;
    err = mooncake::EtcdHelper::Get(view_key.data(), view_key.size(),
                                    leader_address, revision);
    if (err == ErrorCode::OK && !leader_address.empty()) {
        LOG(INFO) << "cfm_endpoint: single-leader HA via " << view_key
                  << " -> " << leader_address;
        const std::string endpoint = std::move(leader_address);
        return [endpoint](const TenantId&, const std::string&)
                   -> std::optional<std::string> { return endpoint; };
    }
    if (err != ErrorCode::OK && err != ErrorCode::ETCD_KEY_NOT_EXIST) {
        LOG(FATAL) << "cfm_endpoint: failed to read " << view_key << ": "
                   << toString(err);
        return {};
    }

    // CVM multi-submaster: masters registry + slot ownership.
    std::vector<mooncake::cvm::MasterRegistration> masters;
    mooncake::ViewVersionId version = 0;
    err = mooncake::cvm::EtcdViewStore::LoadAllMasters(cluster_namespace,
                                                       masters, version);
    if (err != ErrorCode::OK) {
        LOG(FATAL) << "cfm_endpoint: LoadAllMasters failed for namespace '"
                   << cluster_namespace << "': " << toString(err);
        return {};
    }

    std::map<std::string, std::string> address_by_master;  // id -> host:port
    std::vector<std::string> primary_ids;
    for (const auto& reg : masters) {
        if (reg.role ==
                static_cast<int32_t>(mooncake::cvm::MasterRole::kPrimary) &&
            !reg.address.empty()) {
            address_by_master[reg.master_id] = reg.address;
            primary_ids.push_back(reg.master_id);
        }
    }
    if (primary_ids.empty()) {
        LOG(FATAL) << "cfm_endpoint: no primary SubMaster registered under "
                      "/cvm/"
                   << cluster_namespace << "/masters";
        return {};
    }
    std::sort(primary_ids.begin(), primary_ids.end());

    // Prefer the authoritative slot owner table published by CvmController;
    // fall back to the consistent-hash ring used by the masters themselves.
    std::unordered_map<uint16_t, std::string> owner_by_slot;
    std::vector<mooncake::cvm::SlotOwner> slot_owners;
    const ErrorCode slot_err = mooncake::cvm::EtcdViewStore::LoadAllSlotOwners(
        cluster_namespace, slot_owners, version);
    if (slot_err == ErrorCode::OK) {
        for (const auto& owner : slot_owners) {
            if (owner.state ==
                    static_cast<int32_t>(mooncake::cvm::SlotState::kStable) &&
                !owner.primary_master_id.empty()) {
                owner_by_slot[owner.slot] = owner.primary_master_id;
            }
        }
    }
    const bool has_owner_table = !owner_by_slot.empty();
    LOG(INFO) << "cfm_endpoint: CVM namespace '" << cluster_namespace
              << "' has " << primary_ids.size() << " primary submaster(s), "
              << (has_owner_table ? owner_by_slot.size() : 0)
              << " slot owners"
              << (has_owner_table ? "" : " (falling back to hash ring)");

    return [address_by_master = std::move(address_by_master),
            primary_ids = std::move(primary_ids),
            owner_by_slot = std::move(owner_by_slot), has_owner_table](
               const TenantId& tenant,
               const std::string& key) -> std::optional<std::string> {
        const uint16_t slot = mooncake::cvm::KeySlot(tenant, key);
        std::string owner;
        if (has_owner_table) {
            const auto it = owner_by_slot.find(slot);
            if (it != owner_by_slot.end()) owner = it->second;
        }
        if (owner.empty()) {
            owner = mooncake::cvm::ResolveSlotOwnerOnRing(primary_ids, slot);
        }
        const auto address = address_by_master.find(owner);
        if (address == address_by_master.end()) return std::nullopt;
        return address->second;
    };
#endif
}

class LatencyStats final {
   public:
    void Record(double value_us) { values_us_.push_back(value_us); }

    double Percentile(double percentile) const {
        if (values_us_.empty()) return 0.0;
        const double rank = percentile / 100.0 * (values_us_.size() - 1);
        const auto lower = static_cast<size_t>(rank);
        const auto upper = std::min(lower + 1, values_us_.size() - 1);
        const double fraction = rank - lower;
        return values_us_[lower] * (1.0 - fraction) +
               values_us_[upper] * fraction;
    }

    void Finalize() { std::sort(values_us_.begin(), values_us_.end()); }

    double Mean() const {
        if (values_us_.empty()) return 0.0;
        return std::accumulate(values_us_.begin(), values_us_.end(), 0.0) /
               values_us_.size();
    }

   private:
    std::vector<double> values_us_;
};

struct MetricReportSnapshot {
    uint64_t calls{0};
    uint64_t failures{0};
    uint64_t observations{0};
    LatencyStats latency;
};

class MetricReportStats final {
   public:
    void Record(const MetricBatch& batch, double latency_us, bool success) {
        std::lock_guard lock(mutex_);
        ++calls;
        if (!success) ++failures;
        observations +=
            batch.inference.size() + batch.accesses.size() + batch.storage.size();
        latency.Record(latency_us);
    }

    MetricReportSnapshot Finalize() {
        std::lock_guard lock(mutex_);
        latency.Finalize();
        return {.calls = calls,
                .failures = failures,
                .observations = observations,
                .latency = latency};
    }

   private:
    std::mutex mutex_;
    uint64_t calls{0};
    uint64_t failures{0};
    uint64_t observations{0};
    LatencyStats latency;
};

struct RequestData {
    IoPatternSnapshot snapshot;
    std::vector<InferenceMetrics> inference;
    std::vector<AccessRecord> accesses;
};

RequestData BuildRequest(size_t request_index) {
    const size_t session = request_index % FLAGS_num_sessions;
    const uint64_t total_tokens = FLAGS_prompt_tokens + FLAGS_output_tokens;
    const size_t blocks = BlockCount(total_tokens);
    const size_t shared_blocks =
        std::min(blocks, BlockCount(FLAGS_shared_prefix_tokens));
    const bool prefix_is_cached = request_index >= FLAGS_num_sessions;
    const uint64_t now_ns = SteadyNowNs();

    RequestData request;
    request.snapshot.generated_at_ns = now_ns;
    request.inference.reserve(blocks * FLAGS_num_layers);
    request.accesses.reserve(blocks * FLAGS_num_layers);
    request.snapshot.keys.reserve(blocks * FLAGS_num_layers);
    const auto tenant = TenantId(FLAGS_tenant);
    const auto session_id = "vllm-session-" + std::to_string(session);

    for (size_t layer = 0; layer < FLAGS_num_layers; ++layer) {
        for (size_t block = 0; block < blocks; ++block) {
            const bool is_shared_prefix = block < shared_blocks;
            const bool is_hit = is_shared_prefix && prefix_is_cached;
            const ObjectRef object{
                .tenant_id = tenant,
                .key = KvKey(session, request_index, layer, block,
                             is_shared_prefix)};
            const auto block_end = std::min<uint64_t>(
                total_tokens, (block + 1) * FLAGS_tokens_per_block);
            const auto block_tokens = static_cast<uint32_t>(
                block_end - block * FLAGS_tokens_per_block);

            InferenceMetrics inference{
                .object = object,
                .session_id = session_id,
                .layout = CacheLayout::kLayerFirst,
                .layout_group = static_cast<uint32_t>(layer),
                .prefix_depth = static_cast<uint32_t>(shared_blocks),
                .prefix_fanout = static_cast<uint32_t>(FLAGS_num_sessions),
                .match_length = is_hit
                                    ? static_cast<uint32_t>(
                                          FLAGS_shared_prefix_tokens)
                                    : 0U,
                .continuous_prefix_length = is_hit
                                                ? static_cast<uint32_t>(
                                                      FLAGS_shared_prefix_tokens)
                                                : 0U,
                .token_count = block_tokens,
                .recompute_cost = is_hit ? 0.0F : static_cast<float>(block_tokens),
                .request_priority = 1};
            AccessRecord access{
                .object = object,
                .observed_at_ns = now_ns,
                .block_size = FLAGS_kv_block_bytes,
                .latency_us = is_hit ? 20U : 200U,
                .tier = CacheTier::kL1Host,
                .operation = is_hit ? IoOperation::kGet : IoOperation::kPut,
                .is_hit = is_hit,
                .write_batch_size = is_hit
                                        ? 0U
                                        : static_cast<uint32_t>(FLAGS_num_layers),
                .overwrite = !is_hit && is_shared_prefix};
            request.inference.push_back(inference);
            request.accesses.push_back(access);
            request.snapshot.keys.push_back(
                KeyMetrics{.object = object,
                           .session_id = session_id,
                           .last_access_time_ns = now_ns,
                           .access_count_window = 1,
                           .block_size = FLAGS_kv_block_bytes,
                           .token_count = block_tokens,
                           .prefix_depth = static_cast<uint32_t>(shared_blocks),
                           .prefix_fanout = static_cast<uint32_t>(FLAGS_num_sessions),
                           .match_length = inference.match_length,
                           .continuous_prefix_length =
                               inference.continuous_prefix_length,
                           .write_batch_size = access.write_batch_size,
                           .write_frequency =
                               access.operation == IoOperation::kPut ? 1U : 0U,
                           .write_object_size = FLAGS_kv_block_bytes,
                           .recompute_cost = inference.recompute_cost,
                           .overwrite_ratio = access.overwrite ? 1.0F : 0.0F,
                           .replica_tiers = CacheTierBit(CacheTier::kL1Host),
                           .layout = CacheLayout::kLayerFirst,
                           .layout_group = static_cast<uint32_t>(layer),
                           .request_priority = 1,
                           .active = is_hit,
                           .write_burst = !is_hit});
        }
    }
    request.snapshot.storage.push_back(
        StorageMetric{.source_id = FLAGS_node_id,
                      .observed_at_ns = now_ns,
                      .tier = CacheTier::kL1Host,
                      .read_bandwidth_bytes_per_sec = 20ULL * 1024 * 1024 * 1024,
                      .write_bandwidth_bytes_per_sec = 10ULL * 1024 * 1024 * 1024,
                      .read_latency_us = 20,
                      .write_latency_us = 200,
                      .used_bytes = static_cast<uint64_t>(
                          FLAGS_memory_used_ratio * 1024 * 1024 * 1024),
                      .capacity_bytes = 1024ULL * 1024 * 1024,
                      .rpc_latency_us = 100,
                      .memory_used_ratio =
                          static_cast<float>(FLAGS_memory_used_ratio)});
    return request;
}

void PrintObservability(std::string_view name,
                        const IoPatternObservabilitySnapshot& metrics) {
    std::cout << "\n  " << name << " IO Pattern metrics\n"
              << "    collect max latency:  " << metrics.collect_latency_us
              << " us\n"
              << "    analyze max latency:  " << metrics.analyze_latency_us
              << " us\n"
              << "    policy decisions:     " << metrics.policy_decisions
              << " (" << std::fixed << std::setprecision(2)
              << metrics.policy_decision_qps << " qps)\n"
              << "    strategy hit rate:    " << metrics.strategy_hit_rate * 100
              << "%\n"
              << "    false positive rate:  "
              << metrics.false_positive_rate * 100 << "%\n"
              << "    degraded:             " << metrics.degrade_count << "\n"
              << "    report drops:         " << metrics.report_drop_count
              << "\n";
}

bool ValidateFlags() {
    return FLAGS_requests != 0 && FLAGS_prompt_tokens + FLAGS_output_tokens != 0 &&
           FLAGS_tokens_per_block != 0 && FLAGS_num_layers != 0 &&
           FLAGS_kv_block_bytes != 0 && FLAGS_num_sessions != 0 &&
           FLAGS_report_capacity != 0 && FLAGS_memory_used_ratio >= 0.0 &&
           FLAGS_memory_used_ratio <= 1.0;
}

// Real-data seeding stage (optional). The IO Pattern runtime on the SubMaster
// only executes storage handlers against replicas that actually exist, so a
// report-only benchmark leaves master-side eviction/promotion/prefetch counters
// at zero. When --master_server and --num_keys are provided this stage writes a
// batch of real KV objects through RealClient (same key/tenant naming as the
// simulated requests, so the analysis snapshot and the real metadata overlap)
// and reads a subset back to simulate access heat.
struct SeedStats {
    uint64_t written{0};
    uint64_t write_failures{0};
    uint64_t reads{0};
    uint64_t read_failures{0};
};

SeedStats RunRealSeedStage() {
    SeedStats stats;
    if (FLAGS_master_server.empty() || FLAGS_num_keys == 0 ||
        FLAGS_value_size == 0) {
        return stats;
    }
    LOG(INFO) << "Real-data seed stage: master_server=" << FLAGS_master_server
              << " protocol=" << FLAGS_protocol
              << " keys=" << FLAGS_num_keys
              << " value_size=" << FLAGS_value_size
              << " replica_num=" << FLAGS_replica_num
              << " offload=" << (FLAGS_enable_ssd_offload ? "yes" : "no");

    auto client = mooncake::RealClient::create();
    const size_t block_bytes = std::max<size_t>(FLAGS_value_size, 4096);
    char* buffer = reinterpret_cast<char*>(numa_alloc_local(block_bytes));
    if (buffer == nullptr) {
        LOG(ERROR) << "numa_alloc_local failed for seed buffer of "
                   << block_bytes << " bytes";
        return stats;
    }
    std::memset(buffer, 0xA5, block_bytes);
    int ret = client->setup_real(
        FLAGS_local_hostname, FLAGS_metadata_server, FLAGS_global_segment_size,
        FLAGS_local_buffer_size, FLAGS_protocol, FLAGS_device_name,
        FLAGS_master_server, nullptr, "", FLAGS_enable_ssd_offload,
        FLAGS_ssd_offload_path, FLAGS_tenant);
    if (ret != 0) {
        LOG(ERROR) << "setup_real failed: " << ret;
        numa_free(buffer, block_bytes);
        return stats;
    }
    ret = client->register_buffer(buffer, block_bytes);
    if (ret != 0) {
        LOG(ERROR) << "register_buffer failed: " << ret;
        numa_free(buffer, block_bytes);
        return stats;
    }

    // Write keys that share the simulated KvKey naming so later reports and
    // the real metadata address the same objects. Enumerate the same
    // (session, request, layer, block) space as BuildRequest() and stop after
    // --num_keys objects, so the seeded set is exactly the head of the
    // reported key universe (real replicas exist for the keys policy will
    // select).
    mooncake::ReplicateConfig config;
    config.replica_num = static_cast<size_t>(FLAGS_replica_num);
    config.with_hard_pin = FLAGS_hard_pin;
    const uint64_t total_tokens = FLAGS_prompt_tokens + FLAGS_output_tokens;
    const size_t blocks = BlockCount(total_tokens);
    const size_t shared_blocks =
        std::min(blocks, BlockCount(FLAGS_shared_prefix_tokens));
    uint64_t seeded = 0;
    for (size_t request_index = 0;
         request_index < FLAGS_requests && seeded < FLAGS_num_keys;
         ++request_index) {
        const size_t session = request_index % FLAGS_num_sessions;
        for (size_t layer = 0; layer < FLAGS_num_layers && seeded < FLAGS_num_keys;
             ++layer) {
            for (size_t block = 0; block < blocks && seeded < FLAGS_num_keys;
                 ++block) {
                const bool is_shared_prefix = block < shared_blocks;
                const std::string key =
                    KvKey(session, request_index, layer, block, is_shared_prefix);
                const int put_ret =
                    client->put_from(key, buffer, FLAGS_value_size, config);
                if (put_ret == 0) {
                    ++seeded;
                } else {
                    ++stats.write_failures;
                    LOG(WARNING) << "put_from failed for seed key " << key
                                 << ": " << put_ret;
                }
            }
        }
    }
    stats.written = seeded;

    // Simulate reads: exercise a hot subset through the real data path so the
    // SubMaster records real GET access heat (promotion-on-hit when offloaded).
    // Re-enumerate the same key space in the same order and read the first
    // read_count keys.
    const uint64_t read_count = FLAGS_seed_get_keys == 0
                                    ? seeded / 2
                                    : std::min<uint64_t>(FLAGS_seed_get_keys,
                                                         seeded);
    uint64_t read_keys = 0;
    for (size_t request_index = 0;
         request_index < FLAGS_requests && read_keys < read_count;
         ++request_index) {
        const size_t session = request_index % FLAGS_num_sessions;
        for (size_t layer = 0;
             layer < FLAGS_num_layers && read_keys < read_count; ++layer) {
            for (size_t block = 0; block < blocks && read_keys < read_count;
                 ++block) {
                const bool is_shared_prefix = block < shared_blocks;
                const std::string key =
                    KvKey(session, request_index, layer, block, is_shared_prefix);
                const int64_t got = client->get_into(key, buffer, FLAGS_value_size);
                if (got >= 0) {
                    ++stats.reads;
                } else {
                    ++stats.read_failures;
                }
                ++read_keys;
            }
        }
    }

    client->unregister_buffer(buffer);
    numa_free(buffer, block_bytes);
    LOG(INFO) << "Real-data seed stage done: written=" << stats.written
              << " write_failures=" << stats.write_failures
              << " reads=" << stats.reads
              << " read_failures=" << stats.read_failures;
    return stats;
}

}  // namespace

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!ValidateFlags()) {
        LOG(ERROR) << "All numeric size/count flags must be positive and "
                      "--memory_used_ratio must be within [0, 1]";
        return 1;
    }

    std::atomic<uint64_t> eviction_commands{0};
    std::atomic<uint64_t> prefetch_commands{0};
    std::atomic<uint64_t> admission_commands{0};

    // The SubMaster-side CFM component (embedded mode) or the ownership
    // resolver used by the remote reporter.
    std::shared_ptr<CfmService> embedded_service;
    std::shared_ptr<IoPatternRuntime> cfm_runtime;
    std::shared_ptr<CfmOwnershipClient> ownership_client;
    std::shared_ptr<CfmRpcChannel> embedded_channel;
    std::string deployment_description;
    if (FLAGS_cfm_endpoint.empty()) {
        deployment_description = "embedded SubMaster (local CFM)";
        IoPatternRuntime::Config cfm_config;
        // Merged reports drive local analysis -> decision -> execution (same
        // worker the production SubMaster runs), so handler counters below
        // reflect report-triggered policy execution, not only the manual
        // watermark evaluation at the end of the run.
        cfm_config.report_driven_execution = true;
        cfm_config.max_analysis_keys = FLAGS_max_analysis_keys;
        cfm_runtime = std::make_shared<IoPatternRuntime>(
            IoPatternRuntime::Handlers{
                .eviction = [&eviction_commands](const EvictionPlan&) {
                    ++eviction_commands;
                    return ErrorCode::OK;
                },
                .prefetch = [&prefetch_commands](const PrefetchPlan&) {
                    ++prefetch_commands;
                    return ErrorCode::OK;
                },
                .admission = [&admission_commands](const AdmissionResult&) {
                    ++admission_commands;
                    return ErrorCode::OK;
                }},
            std::move(cfm_config));
        embedded_service = std::make_shared<CfmService>(cfm_runtime);
        auto transport =
            std::make_shared<EmbeddedCfmTransport>(embedded_service);
        embedded_channel = std::make_shared<CfmRpcChannel>(
            std::move(transport), std::make_shared<CfmBinaryCodec>(),
            CfmRpcConfig{.timeout =
                             std::chrono::milliseconds(FLAGS_cfm_rpc_timeout_ms)});
    } else {
        const auto resolver = ResolveCfmEndpointOwnership();
        ownership_client = std::make_shared<CfmOwnershipClient>(
            resolver, std::chrono::milliseconds(FLAGS_cfm_rpc_timeout_ms));
        deployment_description = "remote SubMaster(s) via CFM coro_rpc";
    }

    // Real-data seeding runs before the simulated request stream ("先种子后仿
    // 真"): the SubMaster must hold real replicas for reported keys before the
    // report-driven policy cycle can execute eviction/promotion/prefetch
    // against them. Only meaningful with a real SubMaster endpoint
    // (--cfm_endpoint) plus RealClient parameters; otherwise it is a no-op.
    const SeedStats seed_stats = RunRealSeedStage();

    IoPatternRuntime::Config source_config;
    source_config.report_capacity = FLAGS_report_capacity;
    source_config.max_analysis_keys = FLAGS_max_analysis_keys;
    source_config.collector.max_total_keys = FLAGS_max_analysis_keys;
    MetricReportStats metric_reports;
    const auto report_metric_batch = [&](const MetricBatch& batch) -> bool {
        const auto started = Clock::now();
        const bool success =
            ownership_client ? ownership_client->ReportMetricBatch(batch) == ErrorCode::OK
                             : (embedded_channel && embedded_channel->SendMetricBatch(batch));
        metric_reports.Record(batch, ToMicroseconds(Clock::now() - started),
                              success);
        return success;
    };
    source_config.report_sink = report_metric_batch;
    auto source_runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        source_config);

    const auto send_snapshot = [&](const IoPatternSnapshot& snapshot) -> bool {
        return ownership_client
                   ? ownership_client->ReportSnapshot(snapshot) == ErrorCode::OK
                   : (embedded_channel && embedded_channel->SendSnapshot(snapshot));
    };

    LatencyStats report_latency;
    uint64_t failed_reports = 0;
    uint64_t total_blocks = 0;
    const auto benchmark_start = Clock::now();
    for (size_t request_index = 0; request_index < FLAGS_requests;
         ++request_index) {
        auto request = BuildRequest(request_index);
        total_blocks += request.accesses.size();
        for (size_t i = 0; i < request.inference.size(); ++i) {
            source_runtime->ReportInferenceMetrics(request.inference[i]);
            source_runtime->RecordAccess(request.accesses[i].object.key,
                                         request.accesses[i]);
        }
        source_runtime->RecordStorageMetric(request.snapshot.storage.front());

        const auto report_start = Clock::now();
        const bool sent = send_snapshot(request.snapshot);
        report_latency.Record(ToMicroseconds(Clock::now() - report_start));
        if (!sent) ++failed_reports;
    }
    const auto submission_seconds =
        std::chrono::duration<double>(Clock::now() - benchmark_start).count();

    // Stop joins the reporter worker and performs its final flush. No new
    // metric batch can reach the SubMaster after this returns.
    source_runtime->StopReports();
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_report_flush_wait_ms));

    // The report-driven worker executes one cycle per merged report. Wait for
    // it to drain before reading handler counters / snapshots so the printed
    // numbers are deterministic.
    if (cfm_runtime && cfm_runtime->report_driven_execution()) {
        cfm_runtime->WaitForReportDrivenIdle();
    }

    // Embedded mode: when the report-driven worker is disabled, evaluate and
    // execute policy once locally (the pre-worker high-watermark trigger that
    // the production EvictionThreadFunc runs). With report_driven_execution
    // enabled the runtime already executes a full cycle per merged report, so
    // this extra pass is skipped to keep the printed handler counters equal to
    // report-triggered executions only.
    if (cfm_runtime && !cfm_runtime->report_driven_execution() &&
        !cfm_runtime->Snapshot().keys.empty()) {
        const auto capacity = 1024ULL * 1024 * 1024;
        const auto target =
            static_cast<uint64_t>((FLAGS_memory_used_ratio - 0.80F) *
                                  static_cast<float>(capacity));
        const auto status = cfm_runtime->Execute(
            CacheTier::kL1Host,
            target > 0 ? target : capacity / 10, TraceHistory{});
        if (status.eviction != ErrorCode::OK &&
            status.prefetch != ErrorCode::OK && status.degraded) {
            LOG(WARNING) << "Local CFM evaluation degraded";
        }
    }

    const auto end_to_end_seconds =
        std::chrono::duration<double>(Clock::now() - benchmark_start).count();

    const auto source_snapshot = source_runtime->Snapshot();
    const auto source_metrics =
        source_runtime->ObservabilitySnapshot(end_to_end_seconds);
    source_runtime.reset();
    report_latency.Finalize();
    const auto metric_report_snapshot = metric_reports.Finalize();
    const auto cfm_snapshot =
        embedded_service ? embedded_service->Snapshot() : IoPatternSnapshot{};
    const auto cfm_metrics =
        embedded_service ? embedded_service->Observability(end_to_end_seconds)
                         : IoPatternObservabilitySnapshot{};

    std::cout << "\n============================================================\n"
              << "CFM CLIENT BENCHMARK (vLLM inference request model)\n"
              << "============================================================\n"
              << "  CFM deployment:          " << deployment_description << "\n"
              << "  Real-data seeding:       written=" << seed_stats.written
              << " (failures=" << seed_stats.write_failures
              << "), reads=" << seed_stats.reads
              << " (failures=" << seed_stats.read_failures << ")\n"
              << "  Requests:                " << FLAGS_requests << "\n"
              << "  Tokens/request:          "
              << FLAGS_prompt_tokens + FLAGS_output_tokens << " (prompt="
              << FLAGS_prompt_tokens << ", decode=" << FLAGS_output_tokens
              << ")\n"
              << "  KV blocks/request:       " << BlockCount(
                     FLAGS_prompt_tokens + FLAGS_output_tokens) * FLAGS_num_layers
              << " (layers=" << FLAGS_num_layers << ")\n"
              << "  Total KV blocks:         " << total_blocks << "\n"
              << "  Request submission time: " << std::fixed
              << std::setprecision(2) << submission_seconds << " s\n"
              << "  Submission requests/sec: "
              << FLAGS_requests / submission_seconds << "\n"
              << "  End-to-end time:         " << end_to_end_seconds << " s\n"
              << "\n  CFM SendSnapshot latency\n"
              << "    failed reports:        " << failed_reports << "\n"
              << "    mean:                  " << report_latency.Mean() << " us\n"
              << "    p50 / p90 / p99:       " << report_latency.Percentile(50)
              << " / " << report_latency.Percentile(90) << " / "
              << report_latency.Percentile(99) << " us\n"
              << "\n  CFM report_metric_batch latency\n"
              << "    calls / failures:      " << metric_report_snapshot.calls
              << " / " << metric_report_snapshot.failures << "\n"
              << "    observations:          "
              << metric_report_snapshot.observations
              << "\n"
              << "    mean:                  "
              << metric_report_snapshot.latency.Mean()
              << " us\n"
              << "    p50 / p90 / p99:       "
              << metric_report_snapshot.latency.Percentile(50) << " / "
              << metric_report_snapshot.latency.Percentile(90) << " / "
              << metric_report_snapshot.latency.Percentile(99) << " us\n"
              << "\n  Local policy handlers executed\n"
              << "    evictions:             " << eviction_commands << "\n"
              << "    prefetches:            " << prefetch_commands << "\n"
              << "    admissions:            " << admission_commands << "\n"
              << "\n  IO Pattern snapshots\n"
              << "    client keys / storage: " << source_snapshot.keys.size() << " / "
              << source_snapshot.storage.size() << "\n";
    if (embedded_service) {
        std::cout << "    CFM keys / storage:    " << cfm_snapshot.keys.size()
                  << " / " << cfm_snapshot.storage.size() << "\n";
    } else {
        // Remote execution cannot be read back by the client; observe the
        // receiving SubMaster's own master admin metrics (`io_pattern_report_*`
        // in `/metrics` and the periodic "Master Admin Metrics" log) and its
        // [IO-PATTERN-REPORT-CYCLE] log lines.
        std::cout << "    CFM keys / storage:    remote endpoint (see SubMaster "
                     "master admin metrics)\n";
    }
    PrintObservability("Client", source_metrics);
    if (embedded_service) PrintObservability("CFM", cfm_metrics);
    std::cout << "============================================================\n";
    return failed_reports == 0 ? 0 : 2;
}
