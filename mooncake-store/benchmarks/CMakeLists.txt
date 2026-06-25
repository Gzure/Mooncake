# Add allocator benchmark executable
add_executable(allocator_bench allocator_bench.cpp)
target_link_libraries(allocator_bench PRIVATE cachelib_memory_allocator
                                              mooncake_store)

# Add master benchmark executable
add_executable(master_bench master_bench.cpp)
target_link_libraries(master_bench PRIVATE cachelib_memory_allocator
                                           mooncake_store)

# Add file interface benchmark executable
add_executable(file_interface_bench file_interface_bench.cpp)
target_link_libraries(file_interface_bench PRIVATE mooncake_store glog::glog)

# Add storage backend benchmark executable
# This benchmark tests all storage backends (OffsetAllocator, Bucket, FilePerKey)
# with realistic KV cache workloads for LLM inference scenarios
add_executable(storage_backend_bench storage_backend_bench.cpp)
target_link_libraries(
  storage_backend_bench PRIVATE mooncake_store cachelib_memory_allocator
                                gflags::gflags glog::glog pthread)

# Add allocation strategy benchmark executable
# This benchmark tests AllocationStrategy performance with configurable
# segment counts, allocation sizes, replica counts, and workload patterns
add_executable(allocation_strategy_bench allocation_strategy_bench.cpp)
target_link_libraries(
  allocation_strategy_bench PRIVATE mooncake_store cachelib_memory_allocator
                                    gflags::gflags glog::glog pthread)

add_executable(stress_cluster_bench stress_cluster_bench.cpp)
target_link_libraries(
  stress_cluster_bench PRIVATE mooncake_store transfer_engine asio_shared
                               gflags::gflags glog::glog pthread)

# Add NoF worker pool benchmark executable
if(USE_NOF)
  add_executable(nof_worker_pool_bench nof_worker_pool_bench.cpp)
  target_link_libraries(nof_worker_pool_bench PRIVATE mooncake_store glog::glog
                                                       gflags::gflags)
endif()

# Add batchload benchmark executable
# Tests single-node BatchLoad (read) performance for the SSD-offload layer
# against bucket and file-per-key backends, with TB-scale streaming prefill
# (BatchOffload) as untimed setup and configurable read thread counts.
add_executable(batchload_bench batchload_bench.cpp)
target_link_libraries(
  batchload_bench PRIVATE mooncake_store cachelib_memory_allocator
                         gflags::gflags glog::glog pthread)
