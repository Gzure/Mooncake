// Copyright 2025 Mooncake Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file batchload_bench.cpp
 * @brief Single-node BatchLoad (read) performance benchmark for Mooncake
 *        store SSD-offload layer.
 *
 * Benchmarks the read path (StorageBackendInterface::BatchLoad) against either
 * the bucket backend (many keys packed into 256MB bucket files) or the
 * file-per-key backend (one file per key). The write path (BatchOffload) is
 * only used to stream-prefill TB-scale data onto SSD as setup and is NOT timed.
 *
 * WORKFLOW (two phases, single backend + single thread count per run):
 *
 *   Phase 1 - Prefill (setup, untimed):
 *     When --prefill_size_tb > 0, N threads concurrently BatchOffload
 *     --prefill_size_tb of 4MB keys onto SSD. Buffers are streamed and
 *     discarded (TB cannot reside in memory). Progress is reported every
 *     100GB. When --prefill_size_tb == 0, prefill is skipped and the existing
 *     on-disk dataset is reused for the read test.
 *
 *   Phase 2 - Read (measured):
 *     --num_threads threads each perform --num_operations random BatchLoad
 *     calls (batch_size keys per call) drawn from the prefilled key space.
 *     Only the BatchLoad call itself is timed. Verification is optional.
 *
 * METRICS:
 *   - Throughput (GB/s, primary): total bytes read / wall-clock seconds
 *   - Latency (ms): per BatchLoad call, avg / p99 / p99.99
 *
 * REUSE PATTERN (same TB dataset across multiple read configs):
 *
 *   # First run: prefill 1TB (slow, once)
 *   ./batchload_bench --backend=bucket --prefill_size_tb=1 --num_threads=1
 *
 *   # Later runs: reuse SSD data, vary thread count (fast)
 *   ./batchload_bench --backend=bucket --prefill_size_tb=0 \
 *                     --dataset_size_tb=1 --num_threads=8
 *
 * NOTE: This benchmark is Linux-oriented (posix_memalign, unistd.h), matching
 * the existing storage_backend_bench.cpp, since the SSD-offload layer targets
 * Linux.
 */

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "storage_backend.h"

namespace fs = std::filesystem;

// ============================================================================
// Command-line Flags
// ============================================================================

// === Backend / Mode ===
DEFINE_string(backend, "bucket",
              "Storage backend mode: bucket | keyper_key. "
              "bucket = BucketStorageBackend (keys packed into 256MB bucket "
              "files, supports io_uring batch read). "
              "keyper_key = StorageBackendAdaptor / FilePerKey (one file per "
              "key, serial read).");

// === Data shape ===
DEFINE_uint64(value_size, 4 * 1024 * 1024,
              "Value size in bytes per key (default: 4MB)");
DEFINE_uint64(batch_size, 32,
              "Number of keys per BatchLoad / BatchOffload call (default: 32)");
DEFINE_uint64(capacity_gb, 2048,
              "Backend storage capacity limit in GB (default: 2048 = 2TB). "
              "Must be >= prefill/dataset size.");

// === Bucket-specific (only effective when backend=bucket) ===
DEFINE_uint64(bucket_keys_limit, 500,
              "Max keys allowed in a single bucket (only effective for bucket "
              "mode). When reusing a prefilled dataset, keep this identical to "
              "the value used during prefill.");

// === Prefill (setup phase, untimed) ===
DEFINE_uint64(prefill_size_tb, 0,
              "TB of data to stream-prefill onto SSD (setup, untimed). "
              "0 = skip prefill and reuse existing on-disk dataset for the "
              "read test. >0 = prefill this many TB via concurrent "
              "BatchOffload.");
DEFINE_uint64(prefill_threads, 4,
              "Number of threads for the concurrent prefill phase (default: 4)");
DEFINE_string(storage_path, "/tmp/mooncake_batchload_bench",
              "Storage directory path for backend data files");

// === Dataset (read-test sampling range) ===
DEFINE_uint64(dataset_size_tb, 1,
              "Size of the on-disk TB dataset used as the read-test sampling "
              "range. Ignored when prefill_size_tb > 0 (then equals "
              "prefill_size_tb). In reuse mode (prefill_size_tb == 0) this must "
              "match the amount originally prefilled.");

// === Read test (the measured workload) ===
DEFINE_uint64(num_threads, 1,
              "Number of concurrent BatchLoad reader threads (default: 1)");
DEFINE_uint64(num_operations, 1000,
              "Number of BatchLoad operations per thread (default: 1000)");
DEFINE_uint64(warmup_operations, 50,
              "Number of untimed warmup operations per thread (default: 50)");

// === Verification ===
DEFINE_bool(verify, true,
            "Verify read-back data against deterministic pattern (default: "
            "true)");
DEFINE_bool(fail_fast, true,
            "Exit immediately on first checksum mismatch (default: true)");

// === Cleanup ===
DEFINE_bool(skip_cleanup, true,
            "Skip deleting the storage directory after the run (default: "
            "true), so the TB dataset can be reused across runs. Set to false "
            "to wipe storage_path on exit.");

// ============================================================================
// Constants
// ============================================================================

namespace {
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;
constexpr size_t TB = 1024 * GB;

// 4KB page alignment (matches storage_backend_bench; potential future O_DIRECT)
constexpr size_t kAlignment = 4096;

// LCG constants (same as storage_backend_bench DataGenerator)
constexpr uint64_t kLcgMult = 6364136223846793005ULL;
constexpr uint64_t kLcgIncr = 1442695040888963407ULL;

inline size_t AlignUp(size_t size, size_t alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
}

// On-the-fly key generation: "kvcache_block_<index>". No pre-storage needed,
// so TB-scale key spaces cost zero memory and stay consistent across phases.
inline std::string KeyOf(size_t index) {
    return "kvcache_block_" + std::to_string(index);
}
}  // namespace

// ============================================================================
// Thread-Local Statistics (no mutex in hot path)
// ============================================================================

struct ThreadStats {
    std::vector<double> latencies;
    size_t bytes = 0;
    size_t operations = 0;
    size_t errors = 0;
    size_t checksum_failures = 0;

    void Reserve(size_t expected_ops) { latencies.reserve(expected_ops); }
    void RecordLatency(double latency_ms) { latencies.push_back(latency_ms); }
    void RecordBytes(size_t b) { bytes += b; }
    void RecordOperation() { ++operations; }
    void RecordError() { ++errors; }
    void RecordChecksumFailure() { ++checksum_failures; }
};

// Aggregated stats; merge happens after the benchmark completes.
// Specialized for this bench: percentiles up to p99.99, throughput in GB/s.
class BenchmarkStats {
   public:
    void InitThreads(size_t num_threads, size_t expected_ops_per_thread) {
        thread_stats_.resize(num_threads);
        for (auto& ts : thread_stats_) ts.Reserve(expected_ops_per_thread);
    }

    ThreadStats& GetThreadStats(size_t thread_id) {
        return thread_stats_[thread_id];
    }

    void StartTimer() { start_time_ = std::chrono::steady_clock::now(); }
    void StopTimer() { end_time_ = std::chrono::steady_clock::now(); }

    double GetElapsedSeconds() const {
        return std::chrono::duration<double>(end_time_ - start_time_).count();
    }

    void Clear() { thread_stats_.clear(); }

    // Merge all thread stats and sort latencies for percentile computation.
    void Finalize() {
        merged_latencies_.clear();
        total_bytes_ = 0;
        total_operations_ = 0;
        total_errors_ = 0;
        total_checksum_failures_ = 0;

        for (const auto& ts : thread_stats_) {
            merged_latencies_.insert(merged_latencies_.end(),
                                     ts.latencies.begin(), ts.latencies.end());
            total_bytes_ += ts.bytes;
            total_operations_ += ts.operations;
            total_errors_ += ts.errors;
            total_checksum_failures_ += ts.checksum_failures;
        }
        std::sort(merged_latencies_.begin(), merged_latencies_.end());
    }

    void PrintStatistics() const {
        if (merged_latencies_.empty()) {
            std::cout << "  No data recorded.\n";
            return;
        }

        double wall_sec = GetElapsedSeconds();
        size_t n = merged_latencies_.size();

        double min_lat = merged_latencies_.front();
        double max_lat = merged_latencies_.back();
        double mean_lat =
            std::accumulate(merged_latencies_.begin(), merged_latencies_.end(),
                            0.0) /
            n;
        double p99 = GetPercentile(99.0);
        double p9999 = GetPercentile(99.99);

        // Throughput from wall clock so multi-threaded aggregate bandwidth is
        // measured correctly (sum-of-latencies would overcount for N threads).
        double throughput_gbps =
            (wall_sec > 0) ? (static_cast<double>(total_bytes_) / GB) / wall_sec
                           : 0.0;
        double ops_per_sec =
            (wall_sec > 0) ? total_operations_ / wall_sec : 0.0;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "\nResults:\n";
        std::cout << "  Wall clock:    " << wall_sec << " s\n";
        std::cout << "  Threads:       " << thread_stats_.size() << "\n";
        std::cout << "  Operations:    " << total_operations_;
        if (total_errors_ > 0) std::cout << " (errors: " << total_errors_ << ")";
        std::cout << "\n";
        std::cout << "  Samples:       " << n << " latency measurements\n";
        std::cout << "  Total data:    "
                  << (static_cast<double>(total_bytes_) / GB) << " GB read\n";
        std::cout << "  Throughput:    " << throughput_gbps << " GB/s ("
                  << (throughput_gbps * 1024) << " MB/s)\n";
        std::cout << "  Ops/sec:       " << ops_per_sec << "\n";

        std::cout << "\n  Latency (ms):  [n=" << n << "]\n";
        std::cout << "    Avg:    " << std::setw(10) << mean_lat << "\n";
        std::cout << "    P99:    " << std::setw(10) << p99;
        if (n < 100) std::cout << "  (caution: n<100)";
        std::cout << "\n";
        std::cout << "    P99.99: " << std::setw(10) << p9999;
        if (n < 10000) std::cout << "  (caution: n<10000)";
        std::cout << "\n";
        std::cout << "    Min:    " << std::setw(10) << min_lat << "\n";
        std::cout << "    Max:    " << std::setw(10) << max_lat << "\n";

        if (total_checksum_failures_ > 0) {
            std::cout << "\n  *** CHECKSUM FAILURES: " << total_checksum_failures_
                      << " ***\n";
        }
    }

   private:
    double GetPercentile(double percentile) const {
        if (merged_latencies_.empty()) return 0.0;
        // Linear interpolation for accuracy.
        double rank = (percentile / 100.0) * (merged_latencies_.size() - 1);
        size_t lower = static_cast<size_t>(rank);
        size_t upper = std::min(lower + 1, merged_latencies_.size() - 1);
        double frac = rank - lower;
        return merged_latencies_[lower] * (1.0 - frac) +
               merged_latencies_[upper] * frac;
    }

    std::vector<ThreadStats> thread_stats_;
    std::vector<double> merged_latencies_;
    size_t total_bytes_ = 0;
    size_t total_operations_ = 0;
    size_t total_errors_ = 0;
    size_t total_checksum_failures_ = 0;
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point end_time_;
};

// ============================================================================
// Aligned Buffer Pool
// ============================================================================

struct AlignedDeleter {
    void operator()(void* ptr) const {
        if (ptr) std::free(ptr);
    }
};
using AlignedBuffer = std::unique_ptr<char, AlignedDeleter>;

class BufferPool {
   public:
    void Init(size_t num_buffers, size_t buffer_size) {
        buffer_size_ = AlignUp(buffer_size, kAlignment);
        buffers_.resize(num_buffers);
        for (auto& buf : buffers_) {
            void* ptr = nullptr;
            int ret = ::posix_memalign(&ptr, kAlignment, buffer_size_);
            if (ret != 0 || ptr == nullptr) {
                LOG(FATAL) << "Failed to allocate aligned buffer of size "
                           << buffer_size_;
            }
            buf = AlignedBuffer(static_cast<char*>(ptr));
        }
    }

    char* Get(size_t index) { return buffers_[index].get(); }
    size_t Size() const { return buffers_.size(); }
    size_t BufferSize() const { return buffer_size_; }

   private:
    std::vector<AlignedBuffer> buffers_;
    size_t buffer_size_ = 0;
};

// ============================================================================
// Deterministic Data Generator / Verifier
// (Same LCG pattern as storage_backend_bench so key->content mapping is stable
//  across prefill and read phases.)
// ============================================================================

class DataGenerator {
   public:
    // Fill buffer with deterministic content keyed by key_index.
    void FillBuffer(char* buffer, size_t size, size_t key_index) {
        uint64_t val = key_index * kLcgMult + kLcgIncr;
        uint64_t* ptr = reinterpret_cast<uint64_t*>(buffer);
        size_t num_words = size / sizeof(uint64_t);
        for (size_t i = 0; i < num_words; ++i) {
            ptr[i] = val;
            val = val * kLcgMult + kLcgIncr;
        }
        size_t remaining = size % sizeof(uint64_t);
        if (remaining > 0) {
            std::memcpy(buffer + num_words * sizeof(uint64_t), &val, remaining);
        }
    }

    // Verify buffer matches the expected deterministic content for key_index.
    bool VerifyBuffer(const char* buffer, size_t size, size_t key_index) {
        uint64_t val = key_index * kLcgMult + kLcgIncr;
        const uint64_t* ptr = reinterpret_cast<const uint64_t*>(buffer);
        size_t num_words = size / sizeof(uint64_t);
        for (size_t i = 0; i < num_words; ++i) {
            if (ptr[i] != val) return false;
            val = val * kLcgMult + kLcgIncr;
        }
        size_t remaining = size % sizeof(uint64_t);
        if (remaining > 0) {
            uint64_t expected = 0;
            std::memcpy(&expected, &val, remaining);
            uint64_t actual = 0;
            std::memcpy(&actual, buffer + num_words * sizeof(uint64_t),
                        remaining);
            if (actual != expected) return false;
        }
        return true;
    }
};

// ============================================================================
// Backend Factory
// ============================================================================

enum class BackendMode { BUCKET, KEY_PER_KEY };

BackendMode ParseBackendMode(const std::string& s) {
    if (s == "bucket") return BackendMode::BUCKET;
    if (s == "keyper_key") return BackendMode::KEY_PER_KEY;
    LOG(FATAL) << "Unknown backend mode: " << s
               << " (expected 'bucket' or 'keyper_key')";
    return BackendMode::BUCKET;
}

std::string BackendModeToString(BackendMode m) {
    switch (m) {
        case BackendMode::BUCKET:
            return "bucket";
        case BackendMode::KEY_PER_KEY:
            return "keyper_key";
    }
    return "unknown";
}

std::shared_ptr<mooncake::StorageBackendInterface> CreateBackend(
    BackendMode mode, const std::string& storage_path, size_t capacity_bytes) {
    mooncake::FileStorageConfig config;
    config.storage_filepath = storage_path;
    config.total_size_limit = capacity_bytes;
    config.total_keys_limit = 10'000'000;

    switch (mode) {
        case BackendMode::BUCKET: {
            config.storage_backend_type =
                mooncake::StorageBackendType::kBucket;
            mooncake::BucketBackendConfig bucket_config;
            bucket_config.bucket_size_limit = 256 * MB;
            bucket_config.bucket_keys_limit = FLAGS_bucket_keys_limit;
            bucket_config.eviction_policy =
                mooncake::BucketEvictionPolicy::NONE;
            return std::make_shared<mooncake::BucketStorageBackend>(
                config, bucket_config);
        }
        case BackendMode::KEY_PER_KEY: {
            config.storage_backend_type =
                mooncake::StorageBackendType::kFilePerKey;
            mooncake::FilePerKeyConfig fpk_config;
            fpk_config.fsdir = "file_per_key_bench";
            fpk_config.enable_eviction = false;
            return std::make_shared<mooncake::StorageBackendAdaptor>(
                config, fpk_config);
        }
    }
    return nullptr;
}

// A no-op completion handler for BatchOffload during prefill.
mooncake::ErrorCode NoopCompleteHandler(
    const std::vector<std::string>& /*keys*/,
    std::vector<mooncake::StorageObjectMetadata>& /*metadatas*/) {
    return mooncake::ErrorCode::OK;
}

// ============================================================================
// Phase 1: Prefill (untimed setup) — concurrent streaming BatchOffload
// ============================================================================

void PrefillPhase(mooncake::StorageBackendInterface* backend,
                  size_t total_keys, size_t value_size, size_t batch_size,
                  size_t num_threads) {
    if (total_keys == 0) {
        std::cout << "  Prefill: 0 keys, skipping.\n";
        return;
    }

    std::cout << "  Prefilling " << total_keys << " keys ("
              << (static_cast<double>(total_keys) * value_size / TB) << " TB) "
              << "with " << num_threads << " threads...\n";

    std::latch start_latch(num_threads + 1);
    std::atomic<size_t> keys_done{0};
    std::atomic<bool> error_flag{false};

    // Each thread owns a disjoint, contiguous key range.
    size_t keys_per_thread = total_keys / num_threads;
    size_t remainder = total_keys % num_threads;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
        size_t my_start = t * keys_per_thread + std::min(t, remainder);

        threads.emplace_back([&, t, my_start, my_keys]() {
            DataGenerator gen;
            BufferPool pool;
            pool.Init(batch_size, value_size);

            // Reusable offload container (cleared, not reallocated, per batch).
            std::unordered_map<std::string, std::vector<mooncake::Slice>>
                offload_batch;
            offload_batch.reserve(batch_size);

            start_latch.arrive_and_wait();

            size_t end = my_start + my_keys;
            for (size_t key_idx = my_start; key_idx < end;
                 key_idx += batch_size) {
                size_t this_batch =
                    std::min(batch_size, end - key_idx);

                // Fill buffers and build the offload batch.
                offload_batch.clear();
                for (size_t i = 0; i < this_batch; ++i) {
                    gen.FillBuffer(pool.Get(i), value_size, key_idx + i);
                    offload_batch.emplace(
                        KeyOf(key_idx + i),
                        std::vector<mooncake::Slice>{
                            {pool.Get(i), value_size}});
                }

                auto result = backend->BatchOffload(offload_batch,
                                                    NoopCompleteHandler);
                if (!result) {
                    LOG(ERROR) << "[prefill t=" << t
                               << "] BatchOffload failed at key " << key_idx
                               << ": " << static_cast<int>(result.error());
                    error_flag.store(true, std::memory_order_relaxed);
                    return;
                }

                size_t done = keys_done.fetch_add(this_batch,
                                                  std::memory_order_relaxed) +
                              this_batch;
                // Progress every ~100GB.
                size_t progress_interval =
                    (100 * GB) / value_size;  // keys per 100GB
                if (progress_interval > 0 &&
                    (done / progress_interval) >
                        ((done - this_batch) / progress_interval)) {
                    double tb_done =
                        static_cast<double>(done) * value_size / TB;
                    std::cout << "    prefill progress: " << std::fixed
                              << std::setprecision(2) << tb_done << " TB ("
                              << done << "/" << total_keys << " keys)\n";
                }
            }
        });
    }

    start_latch.arrive_and_wait();
    for (auto& t : threads) t.join();

    if (error_flag.load()) {
        LOG(FATAL) << "Prefill failed; aborting.";
    }
    std::cout << "  Prefill complete: " << keys_done.load() << " keys written.\n";
}

// ============================================================================
// Phase 2: Read test (measured) — concurrent random BatchLoad
// ============================================================================

void ReadPhase(mooncake::StorageBackendInterface* backend, size_t total_keys,
               size_t value_size, size_t batch_size, size_t num_threads,
               size_t num_operations, size_t warmup_operations,
               BenchmarkStats& stats) {
    if (total_keys == 0) {
        LOG(FATAL) << "Cannot run read test with 0 keys in dataset.";
    }

    std::cout << "  Read test: " << num_threads << " threads x "
              << num_operations << " ops x " << batch_size
              << " keys/op (random sampling from " << total_keys << " keys)\n";
    std::cout << "  Warmup: " << warmup_operations << " ops/thread (untimed)\n";
    std::cout << "  Verification: " << (FLAGS_verify ? "enabled" : "disabled")
              << "\n";

    stats.Clear();
    stats.InitThreads(num_threads, num_operations);

    std::latch start_latch(num_threads + 1);
    std::latch end_latch(num_threads);
    std::atomic<size_t> checksum_failures{0};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            DataGenerator gen;
            BufferPool pool;
            pool.Init(batch_size, value_size);

            std::mt19937_64 rng(42 + t);  // distinct seed per thread

            ThreadStats& my_stats = stats.GetThreadStats(t);

            // Reusable per-op containers.
            std::unordered_map<std::string, mooncake::Slice> load_batch;
            load_batch.reserve(batch_size);
            std::vector<size_t> sampled_keys(batch_size);
            // Used to dedup keys within a single batch (BatchLoad takes a map,
            // so duplicate keys would otherwise collapse and under-read).
            std::unordered_set<size_t> seen;
            seen.reserve(batch_size * 2);

            size_t total_ops = warmup_operations + num_operations;
            bool timer_started = false;

            start_latch.arrive_and_wait();

            for (size_t op = 0; op < total_ops; ++op) {
                bool is_warmup = (op < warmup_operations);

                // Sample batch_size DISTINCT keys (within this batch) from the
                // global key space. Cross-batch/cross-op repetition is allowed
                // and expected (random access).
                seen.clear();
                load_batch.clear();
                sampled_keys.clear();
                while (sampled_keys.size() < batch_size) {
                    size_t k = static_cast<size_t>(rng()) % total_keys;
                    if (seen.insert(k).second) {
                        size_t slot = sampled_keys.size();
                        sampled_keys.push_back(k);
                        load_batch.emplace(KeyOf(k),
                                           mooncake::Slice{pool.Get(slot),
                                                           value_size});
                    }
                }

                if (!is_warmup && !timer_started) {
                    timer_started = true;
                }

                // === Timed region: only the BatchLoad call ===
                auto start = std::chrono::steady_clock::now();
                auto result = backend->BatchLoad(load_batch);
                auto end = std::chrono::steady_clock::now();
                // === End timed region ===

                if (!is_warmup) {
                    double latency_ms =
                        std::chrono::duration<double, std::milli>(end - start)
                            .count();
                    my_stats.RecordLatency(latency_ms);
                    my_stats.RecordOperation();

                    if (result) {
                        my_stats.RecordBytes(batch_size * value_size);

                        if (FLAGS_verify) {
                            for (size_t i = 0; i < batch_size; ++i) {
                                if (!gen.VerifyBuffer(
                                        pool.Get(i), value_size,
                                        sampled_keys[i])) {
                                    my_stats.RecordChecksumFailure();
                                    checksum_failures.fetch_add(
                                        1, std::memory_order_relaxed);
                                    LOG(ERROR)
                                        << "Checksum mismatch for key index "
                                        << sampled_keys[i];
                                    if (FLAGS_fail_fast) {
                                        LOG(FATAL)
                                            << "Data corruption detected; "
                                               "aborting (fail_fast=true)";
                                    }
                                }
                            }
                        }
                    } else {
                        my_stats.RecordError();
                        if (FLAGS_fail_fast) {
                            LOG(FATAL)
                                << "BatchLoad failed (error "
                                << static_cast<int>(result.error())
                                << "); aborting (fail_fast=true)";
                        }
                    }
                }
            }

            end_latch.count_down();
        });
    }

    stats.StartTimer();
    start_latch.arrive_and_wait();
    end_latch.wait();
    stats.StopTimer();

    for (auto& t : threads) t.join();

    stats.Finalize();
}

// ============================================================================
// Utilities
// ============================================================================

void CleanupStoragePath(const std::string& path) {
    if (fs::exists(path)) {
        std::error_code ec;
        fs::remove_all(path, ec);
        if (ec) {
            LOG(WARNING) << "Failed to cleanup " << path << ": "
                         << ec.message();
        }
    }
    fs::create_directories(path);
}

void PrintBanner(BackendMode mode, size_t value_size, size_t batch_size,
                 size_t num_threads, size_t prefill_tb, size_t dataset_tb,
                 size_t total_keys) {
    std::cout << "\n================ BatchLoad Benchmark ================\n";
    std::cout << "Backend:          " << BackendModeToString(mode) << "\n";
    std::cout << "Value size:       " << (value_size / MB) << " MB\n";
    std::cout << "Batch size:       " << batch_size << " keys/op\n";
    std::cout << "Read threads:     " << num_threads << "\n";
    if (mode == BackendMode::BUCKET) {
        std::cout << "Bucket keys limit: " << FLAGS_bucket_keys_limit << "\n";
    }
    std::cout << "Storage path:     " << FLAGS_storage_path << "\n";
    std::cout << "Capacity:         " << FLAGS_capacity_gb << " GB\n";
    std::cout << "Prefill size:     "
              << (prefill_tb > 0 ? std::to_string(prefill_tb) + " TB"
                                 : std::string("0 (reuse existing)"))
              << "\n";
    std::cout << "Dataset size:     " << dataset_tb << " TB (" << total_keys
              << " keys)\n";
    std::cout << "-----------------------------------------------------\n";
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    BackendMode mode = ParseBackendMode(FLAGS_backend);
    size_t value_size = FLAGS_value_size;
    size_t batch_size = FLAGS_batch_size;
    size_t num_threads = FLAGS_num_threads;
    size_t num_operations = FLAGS_num_operations;
    size_t warmup_operations = FLAGS_warmup_operations;
    size_t capacity_bytes = FLAGS_capacity_gb * GB;

    // Determine the active TB amount and key count.
    // Prefill mode (prefill_size_tb > 0): total_keys derived from prefill size.
    // Reuse mode  (prefill_size_tb == 0): total_keys derived from dataset size.
    size_t prefill_tb = FLAGS_prefill_size_tb;
    size_t active_tb = (prefill_tb > 0) ? prefill_tb : FLAGS_dataset_size_tb;
    if (active_tb == 0) {
        LOG(FATAL) << "Either --prefill_size_tb > 0 or --dataset_size_tb > 0 "
                      "must be set; got 0 for both.";
    }
    size_t total_keys = active_tb * TB / value_size;
    if (total_keys == 0) {
        LOG(FATAL) << "Computed 0 keys for " << active_tb
                   << " TB with value_size=" << value_size;
    }

    if (batch_size == 0) {
        LOG(FATAL) << "--batch_size must be > 0";
    }
    if (num_threads == 0) {
        LOG(FATAL) << "--num_threads must be > 0";
    }

    PrintBanner(mode, value_size, batch_size, num_threads, prefill_tb, active_tb,
                total_keys);

    // Wipe storage only when entering prefill mode; in reuse mode the existing
    // on-disk data must be preserved.
    if (prefill_tb > 0) {
        CleanupStoragePath(FLAGS_storage_path);
    } else {
        fs::create_directories(FLAGS_storage_path);
    }

    auto backend = CreateBackend(mode, FLAGS_storage_path, capacity_bytes);
    if (!backend) {
        LOG(FATAL) << "Failed to create backend";
    }

    // ---- Init ----
    auto init_start = std::chrono::steady_clock::now();
    auto init_result = backend->Init();
    if (!init_result) {
        LOG(FATAL) << "Backend Init failed";
    }
    double init_sec = std::chrono::duration<double>(
                          std::chrono::steady_clock::now() - init_start)
                          .count();
    std::cout << "Backend Init: " << std::fixed << std::setprecision(2)
              << init_sec << " s\n";

    // FilePerKey must scan on-disk metadata before reads work. In reuse mode
    // this rebuilds total_keys/total_size from existing files.
    if (mode == BackendMode::KEY_PER_KEY) {
        auto scan_start = std::chrono::steady_clock::now();
        backend->ScanMeta(
            [](const std::vector<std::string>&,
               std::vector<mooncake::StorageObjectMetadata>&) {
                return mooncake::ErrorCode::OK;
            });
        double scan_sec = std::chrono::duration<double>(
                              std::chrono::steady_clock::now() - scan_start)
                              .count();
        std::cout << "ScanMeta:    " << std::fixed << std::setprecision(2)
                  << scan_sec << " s (required for keyper_key)\n";
    }

    // ---- Phase 1: Prefill (setup, untimed) ----
    if (prefill_tb > 0) {
        std::cout << "\n[Phase 1: Prefill (untimed)]\n";
        PrefillPhase(backend.get(), total_keys, value_size, batch_size,
                     FLAGS_prefill_threads);
    } else {
        std::cout << "\n[Phase 1: Prefill skipped (prefill_size_tb=0, "
                     "reusing existing data)]\n";
    }

    // ---- Phase 2: Read test (measured) ----
    std::cout << "\n[Phase 2: Read test (measured)]\n";
    BenchmarkStats stats;
    ReadPhase(backend.get(), total_keys, value_size, batch_size, num_threads,
              num_operations, warmup_operations, stats);
    stats.PrintStatistics();

    // ---- Cleanup ----
    if (!FLAGS_skip_cleanup) {
        std::cout << "\nCleaning up storage path...\n";
        CleanupStoragePath(FLAGS_storage_path);
    } else {
        std::cout << "\nSkipping cleanup (skip_cleanup=true); dataset kept at "
                  << FLAGS_storage_path << " for reuse.\n";
    }

    google::ShutdownGoogleLogging();
    return 0;
}
