#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client/db_client.hpp"
#include "common/result.hpp"

namespace {

using namespace std::chrono_literals;

enum class BenchMode { kUpsert, kDelete, kSearch, kMixed, kFull };
enum class OperationKind { kUpsert, kDelete, kSearch };

constexpr uint64_t kDefaultUpsertIdBase = 1U;
constexpr uint64_t kDeleteModeIdBase = 1000000U;
constexpr uint64_t kFullModeIdBase = 2000000U;

struct BenchOptions {
    std::vector<std::string> peers;
    uint32_t threads = 4;
    uint64_t requests = 10000;
    uint32_t dim = 1024;
    BenchMode mode = BenchMode::kMixed;
    uint64_t warmup = 1000;
    std::string output_path;
};

struct RequestRecord {
    int64_t timestamp_us = 0;
    uint64_t latency_us = 0;
    bool success = false;
    std::string mode;
};

struct WorkerContext {
    std::unique_ptr<DBClient> client;
    std::vector<RequestRecord> records;
    uint64_t warmup_errors = 0;
};

struct SharedCounters {
    std::atomic<uint64_t> next_vector_id{kDefaultUpsertIdBase};
    std::atomic<uint64_t> next_delete_id{kDefaultUpsertIdBase};
    std::atomic<uint64_t> visible_upper_bound{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> failures{0};
};

std::string Trim(const std::string& value) {
    const auto begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return {};
    }
    const auto end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::vector<std::string> SplitPeers(const std::string& peers_text) {
    std::vector<std::string> peers;
    std::stringstream stream(peers_text);
    std::string item;
    while (std::getline(stream, item, ',')) {
        item = Trim(item);
        if (!item.empty()) {
            peers.push_back(std::move(item));
        }
    }
    return peers;
}

template <typename UInt>
Result<UInt> ParseUnsigned(std::string_view text, const char* option_name) {
    try {
        const unsigned long long raw = std::stoull(std::string(text));
        if (raw > static_cast<unsigned long long>(std::numeric_limits<UInt>::max())) {
            return Result<UInt>::Err(std::string(option_name) + " 超出允许范围");
        }
        return Result<UInt>::Ok(static_cast<UInt>(raw));
    } catch (const std::exception&) {
        return Result<UInt>::Err(std::string(option_name) + " 不是合法的无符号整数");
    }
}

std::string BenchModeToString(BenchMode mode) {
    switch (mode) {
    case BenchMode::kUpsert:
        return "upsert";
    case BenchMode::kDelete:
        return "delete";
    case BenchMode::kSearch:
        return "search";
    case BenchMode::kMixed:
        return "mixed";
    case BenchMode::kFull:
        return "full";
    }
    return "mixed";
}

std::string OperationKindToString(OperationKind kind) {
    switch (kind) {
    case OperationKind::kUpsert:
        return "upsert";
    case OperationKind::kDelete:
        return "delete";
    case OperationKind::kSearch:
        return "search";
    }
    return "search";
}

Result<BenchMode> ParseMode(std::string_view text) {
    if (text == "upsert") {
        return Result<BenchMode>::Ok(BenchMode::kUpsert);
    }
    if (text == "delete") {
        return Result<BenchMode>::Ok(BenchMode::kDelete);
    }
    if (text == "search") {
        return Result<BenchMode>::Ok(BenchMode::kSearch);
    }
    if (text == "mixed") {
        return Result<BenchMode>::Ok(BenchMode::kMixed);
    }
    if (text == "full") {
        return Result<BenchMode>::Ok(BenchMode::kFull);
    }
    return Result<BenchMode>::Err("mode 只支持 upsert / delete / search / mixed / full");
}

void PrintUsage(std::ostream& output, const char* program) {
    output << "用法:\n"
           << "  " << program
           << " --peers host1:port,host2:port [--threads 4] [--requests 10000]\n"
           << "     [--dim 1024] [--mode upsert|delete|search|mixed|full] [--warmup 1000]\n"
           << "     [--output result.csv]\n\n"
           << "说明:\n"
           << "  --peers    逗号分隔的节点地址列表\n"
           << "  --threads  并发线程数，默认 4\n"
           << "  --requests 计入统计的总请求数，默认 10000\n"
           << "  --dim      向量维度，默认 1024\n"
           << "  --mode     压测模式，默认 mixed（70% upsert + 30% search）\n"
           << "             delete 会先单线程预填充 requests+warmup 条向量，再统计删除\n"
           << "             full 为 50% upsert + 20% delete + 30% search\n"
           << "  --warmup   预热请求数，默认 1000；包含 search/delete 的模式会先准备样本\n"
           << "  --output   可选，输出 CSV 文件路径\n";
}

Result<BenchOptions> ParseCommandLine(int argc, char** argv) {
    BenchOptions options;
    for (int index = 1; index < argc; ++index) {
        const std::string arg = argv[index];
        auto read_value = [&](const char* option_name) -> Result<std::string> {
            if (index + 1 >= argc) {
                return Result<std::string>::Err(std::string("缺少 ") + option_name + " 的参数值");
            }
            ++index;
            return Result<std::string>::Ok(std::string(argv[index]));
        };

        if (arg == "--help" || arg == "-h") {
            PrintUsage(std::cout, argv[0]);
            std::exit(0);
        } else if (arg == "--peers") {
            auto value = read_value("--peers");
            if (!value) {
                return Result<BenchOptions>::Err(value.error);
            }
            options.peers = SplitPeers(*value);
        } else if (arg == "--threads") {
            auto value = read_value("--threads");
            if (!value) {
                return Result<BenchOptions>::Err(value.error);
            }
            auto parsed = ParseUnsigned<uint32_t>(*value, "--threads");
            if (!parsed) {
                return Result<BenchOptions>::Err(parsed.error);
            }
            options.threads = *parsed;
        } else if (arg == "--requests") {
            auto value = read_value("--requests");
            if (!value) {
                return Result<BenchOptions>::Err(value.error);
            }
            auto parsed = ParseUnsigned<uint64_t>(*value, "--requests");
            if (!parsed) {
                return Result<BenchOptions>::Err(parsed.error);
            }
            options.requests = *parsed;
        } else if (arg == "--dim") {
            auto value = read_value("--dim");
            if (!value) {
                return Result<BenchOptions>::Err(value.error);
            }
            auto parsed = ParseUnsigned<uint32_t>(*value, "--dim");
            if (!parsed) {
                return Result<BenchOptions>::Err(parsed.error);
            }
            options.dim = *parsed;
        } else if (arg == "--mode") {
            auto value = read_value("--mode");
            if (!value) {
                return Result<BenchOptions>::Err(value.error);
            }
            auto parsed = ParseMode(*value);
            if (!parsed) {
                return Result<BenchOptions>::Err(parsed.error);
            }
            options.mode = *parsed;
        } else if (arg == "--warmup") {
            auto value = read_value("--warmup");
            if (!value) {
                return Result<BenchOptions>::Err(value.error);
            }
            auto parsed = ParseUnsigned<uint64_t>(*value, "--warmup");
            if (!parsed) {
                return Result<BenchOptions>::Err(parsed.error);
            }
            options.warmup = *parsed;
        } else if (arg == "--output") {
            auto value = read_value("--output");
            if (!value) {
                return Result<BenchOptions>::Err(value.error);
            }
            options.output_path = *value;
        } else {
            return Result<BenchOptions>::Err("未知参数: " + arg);
        }
    }

    if (options.peers.empty()) {
        return Result<BenchOptions>::Err("必须通过 --peers 提供至少一个节点地址");
    }
    if (options.threads == 0U) {
        return Result<BenchOptions>::Err("--threads 必须大于 0");
    }
    if (options.requests == 0U) {
        return Result<BenchOptions>::Err("--requests 必须大于 0");
    }
    if (options.dim == 0U) {
        return Result<BenchOptions>::Err("--dim 必须大于 0");
    }
    return Result<BenchOptions>::Ok(std::move(options));
}

std::vector<uint64_t> SplitWork(uint64_t total, uint32_t workers) {
    std::vector<uint64_t> counts(workers, total / workers);
    const uint64_t remainder = total % workers;
    for (uint64_t index = 0; index < remainder; ++index) {
        ++counts[static_cast<std::size_t>(index)];
    }
    return counts;
}

// 这里使用稳定的伪随机向量生成方式。
// 这样 Upsert 和 Search 只要给出同一个 id，就能构造出完全一致的向量，
// 便于压测时不额外保存大规模样本副本。
std::vector<float> BuildVectorForId(uint64_t id, uint32_t dim) {
    std::vector<float> values(dim, 0.0F);
    uint64_t state = id * 11400714819323198485ULL + 0x9E3779B97F4A7C15ULL;
    for (uint32_t index = 0; index < dim; ++index) {
        state ^= state >> 12U;
        state ^= state << 25U;
        state ^= state >> 27U;
        const uint32_t bucket = static_cast<uint32_t>((state * 2685821657736338717ULL) & 0xFFFFULL);
        values[index] = static_cast<float>(bucket) / 65535.0F;
    }
    return values;
}

std::vector<float> BuildRandomVector(std::mt19937_64& rng, uint32_t dim) {
    std::vector<float> values(dim, 0.0F);
    std::uniform_real_distribution<float> distribution(0.0F, 1.0F);
    for (auto& value : values) {
        value = distribution(rng);
    }
    return values;
}

void UpdateVisibleUpperBound(std::atomic<uint64_t>& current, uint64_t candidate) {
    uint64_t observed = current.load(std::memory_order_acquire);
    while (observed < candidate &&
           !current.compare_exchange_weak(observed, candidate, std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
    }
}

OperationKind PickOperation(BenchMode mode, std::mt19937_64& rng) {
    switch (mode) {
    case BenchMode::kUpsert:
        return OperationKind::kUpsert;
    case BenchMode::kDelete:
        return OperationKind::kDelete;
    case BenchMode::kSearch:
        return OperationKind::kSearch;
    case BenchMode::kMixed: {
        std::uniform_int_distribution<int> distribution(0, 99);
        return distribution(rng) < 70 ? OperationKind::kUpsert : OperationKind::kSearch;
    }
    case BenchMode::kFull:
        break;
    }
    return OperationKind::kSearch;
}

uint64_t IdBaseForMode(BenchMode mode) {
    switch (mode) {
    case BenchMode::kDelete:
        return kDeleteModeIdBase;
    case BenchMode::kFull:
        return kFullModeIdBase;
    case BenchMode::kUpsert:
    case BenchMode::kSearch:
    case BenchMode::kMixed:
        return kDefaultUpsertIdBase;
    }
    return kDefaultUpsertIdBase;
}

std::string BuildClientId(BenchMode mode, uint32_t thread_index) {
    switch (mode) {
    case BenchMode::kDelete:
        return "bench-thread-" + std::to_string(thread_index) + "-del";
    case BenchMode::kFull:
        return "bench-thread-" + std::to_string(thread_index) + "-full";
    case BenchMode::kUpsert:
        return "bench-thread-" + std::to_string(thread_index) + "-upsert";
    case BenchMode::kSearch:
        return "bench-thread-" + std::to_string(thread_index) + "-search";
    case BenchMode::kMixed:
        return "bench-thread-" + std::to_string(thread_index) + "-mixed";
    }
    return "bench-thread-" + std::to_string(thread_index);
}

std::string BuildPrefillClientId(BenchMode mode) {
    switch (mode) {
    case BenchMode::kDelete:
        return "bench-prefill-delete";
    case BenchMode::kFull:
        return "bench-prefill-full";
    case BenchMode::kUpsert:
    case BenchMode::kSearch:
    case BenchMode::kMixed:
        return "bench-prefill-generic";
    }
    return "bench-prefill";
}

OperationKind PickOperation(BenchMode mode, uint64_t request_index, std::mt19937_64& rng) {
    if (mode == BenchMode::kFull) {
        // full 模式按固定比例循环：每 10 个请求里前 5 个 upsert、接着 2 个 delete、
        // 剩余 3 个 search。这样既能稳定复现混合负载，也便于跨轮次比较。
        const uint64_t slot = request_index % 10U;
        if (slot < 5U) {
            return OperationKind::kUpsert;
        }
        if (slot < 7U) {
            return OperationKind::kDelete;
        }
        return OperationKind::kSearch;
    }
    return PickOperation(mode, rng);
}

void InitializeCountersForMode(const BenchOptions& options, SharedCounters& counters) {
    const uint64_t base = IdBaseForMode(options.mode);
    counters.next_vector_id.store(base, std::memory_order_release);
    counters.next_delete_id.store(base, std::memory_order_release);
    counters.visible_upper_bound.store(base == 0U ? 0U : base - 1U, std::memory_order_release);
    counters.completed.store(0U, std::memory_order_release);
    counters.failures.store(0U, std::memory_order_release);
}

Result<void> RunPrefillIfNeeded(const BenchOptions& options, SharedCounters& counters) {
    if (options.mode != BenchMode::kDelete) {
        return Result<void>::Ok();
    }

    auto client = DBClient::Connect(options.peers, ClientConfig{}, BuildPrefillClientId(options.mode));
    if (!client) {
        return Result<void>::Err("delete 模式预填充阶段建立客户端失败: " + client.error);
    }

    const uint64_t total_prefill = options.requests + options.warmup;
    for (uint64_t index = 0; index < total_prefill; ++index) {
        const uint64_t vector_id = counters.next_vector_id.fetch_add(1U, std::memory_order_acq_rel);
        auto result =
            (*client)->Upsert(UpsertRequest{.id = vector_id, .vector = BuildVectorForId(vector_id, options.dim)});
        if (!result) {
            return Result<void>::Err("delete 模式预填充失败: " + result.error);
        }
        UpdateVisibleUpperBound(counters.visible_upper_bound, vector_id);
    }
    return Result<void>::Ok();
}

Result<void> EnsureCsvParent(const std::string& output_path) {
    if (output_path.empty()) {
        return Result<void>::Ok();
    }
    const std::filesystem::path path(output_path);
    if (path.has_parent_path()) {
        std::error_code error;
        std::filesystem::create_directories(path.parent_path(), error);
        if (error) {
            return Result<void>::Err("创建 CSV 输出目录失败: " + error.message());
        }
    }
    return Result<void>::Ok();
}

Result<void> WriteCsv(const std::string& output_path, std::vector<RequestRecord> records) {
    if (output_path.empty()) {
        return Result<void>::Ok();
    }

    auto ensure_parent = EnsureCsvParent(output_path);
    if (!ensure_parent) {
        return ensure_parent;
    }

    std::sort(records.begin(), records.end(), [](const RequestRecord& lhs, const RequestRecord& rhs) {
        return lhs.timestamp_us < rhs.timestamp_us;
    });

    std::ofstream output(output_path, std::ios::trunc);
    if (!output.is_open()) {
        return Result<void>::Err("无法创建 CSV 输出文件: " + output_path);
    }

    output << "timestamp_us,latency_us,success,mode\n";
    for (const auto& record : records) {
        output << record.timestamp_us << ','
               << record.latency_us << ','
               << (record.success ? 1 : 0) << ','
               << record.mode << '\n';
    }

    if (!output.good()) {
        return Result<void>::Err("写入 CSV 输出文件失败: " + output_path);
    }
    return Result<void>::Ok();
}

double Percentile(const std::vector<uint64_t>& sorted_latencies, double ratio) {
    if (sorted_latencies.empty()) {
        return 0.0;
    }
    const double raw_index = std::ceil(ratio * static_cast<double>(sorted_latencies.size())) - 1.0;
    const std::size_t index = static_cast<std::size_t>(std::clamp(raw_index, 0.0,
        static_cast<double>(sorted_latencies.size() - 1)));
    return static_cast<double>(sorted_latencies[index]);
}

void PrintSummary(const BenchOptions& options,
                  const std::vector<RequestRecord>& records,
                  uint64_t failures,
                  std::chrono::steady_clock::duration total_duration) {
    std::vector<uint64_t> latencies;
    latencies.reserve(records.size());
    for (const auto& record : records) {
        latencies.push_back(record.latency_us);
    }
    std::sort(latencies.begin(), latencies.end());

    const double seconds =
        std::max(1e-9, std::chrono::duration_cast<std::chrono::duration<double>>(total_duration).count());
    const double throughput = static_cast<double>(records.size()) / seconds;
    const double error_rate = records.empty()
                                  ? 0.0
                                  : (static_cast<double>(failures) / static_cast<double>(records.size())) * 100.0;

    std::cout << "\n====== RaftVDB 压测汇总 ======\n";
    std::cout << "peers      : ";
    for (std::size_t index = 0; index < options.peers.size(); ++index) {
        if (index != 0U) {
            std::cout << ',';
        }
        std::cout << options.peers[index];
    }
    std::cout << "\nthreads    : " << options.threads
              << "\nrequests   : " << options.requests
              << "\nmode       : " << BenchModeToString(options.mode)
              << "\nwarmup     : " << options.warmup
              << "\nduration_s : " << std::fixed << std::setprecision(3) << seconds
              << "\nthroughput : " << throughput << " ops/sec"
              << "\nerror_rate : " << error_rate << "%\n";

    std::cout << "latency(us):"
              << " P50=" << Percentile(latencies, 0.50)
              << " P99=" << Percentile(latencies, 0.99)
              << " P999=" << Percentile(latencies, 0.999)
              << "\n";
}

Result<void> RunWarmup(const BenchOptions& options,
                       std::vector<WorkerContext>& workers,
                       SharedCounters& counters,
                       uint64_t& warmup_errors) {
    std::mutex error_mutex;
    std::string fatal_error;
    const auto work = SplitWork(options.warmup, options.threads);
    std::vector<std::thread> threads;
    threads.reserve(options.threads);

    for (uint32_t index = 0; index < options.threads; ++index) {
        threads.emplace_back([&, index]() {
            const std::string client_id = BuildClientId(options.mode, index);
            auto client = DBClient::Connect(options.peers, ClientConfig{}, client_id);
            if (!client) {
                std::lock_guard lock(error_mutex);
                if (fatal_error.empty()) {
                    fatal_error = client.error;
                }
                return;
            }

            workers[static_cast<std::size_t>(index)].client = std::move(*client);
            for (uint64_t request = 0; request < work[static_cast<std::size_t>(index)]; ++request) {
                Result<void> result = Result<void>::Ok();
                if (options.mode == BenchMode::kDelete) {
                    const uint64_t vector_id =
                        counters.next_delete_id.fetch_add(1U, std::memory_order_acq_rel);
                    result = workers[static_cast<std::size_t>(index)].client->Delete(
                        DeleteRequest{.id = vector_id});
                } else {
                    const uint64_t vector_id =
                        counters.next_vector_id.fetch_add(1U, std::memory_order_acq_rel);
                    result = workers[static_cast<std::size_t>(index)].client->Upsert(
                        UpsertRequest{.id = vector_id, .vector = BuildVectorForId(vector_id, options.dim)});
                    if (result) {
                        UpdateVisibleUpperBound(counters.visible_upper_bound, vector_id);
                    }
                }
                if (!result) {
                    ++workers[static_cast<std::size_t>(index)].warmup_errors;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    if (!fatal_error.empty()) {
        return Result<void>::Err("预热阶段建立客户端失败: " + fatal_error);
    }

    for (const auto& worker : workers) {
        warmup_errors += worker.warmup_errors;
    }
    return Result<void>::Ok();
}

Result<std::vector<RequestRecord>> RunMeasuredPhase(const BenchOptions& options,
                                                    std::vector<WorkerContext>& workers,
                                                    SharedCounters& counters) {
    const auto work = SplitWork(options.requests, options.threads);
    std::atomic<bool> measurement_done{false};
    const auto measurement_begin = std::chrono::steady_clock::now();

    std::thread reporter([&]() {
        auto last_print = measurement_begin;
        uint64_t last_completed = 0;
        while (!measurement_done.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(200ms);
            const auto now = std::chrono::steady_clock::now();
            if (now - last_print < 5s) {
                continue;
            }

            const uint64_t completed = counters.completed.load(std::memory_order_acquire);
            const double interval_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(now - last_print).count();
            const double realtime_tput =
                interval_seconds <= 0.0 ? 0.0 : static_cast<double>(completed - last_completed) / interval_seconds;
            const double total_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(now - measurement_begin).count();
            const double average_tput =
                total_seconds <= 0.0 ? 0.0 : static_cast<double>(completed) / total_seconds;

            std::cout << "[bench] elapsed_s=" << std::fixed << std::setprecision(2) << total_seconds
                      << " completed=" << completed << "/" << options.requests
                      << " throughput=" << realtime_tput << " ops/sec"
                      << " avg=" << average_tput << " ops/sec"
                      << " failures=" << counters.failures.load(std::memory_order_acquire)
                      << '\n';

            last_print = now;
            last_completed = completed;
        }
    });

    std::mutex error_mutex;
    std::string fatal_error;
    std::vector<std::thread> threads;
    threads.reserve(options.threads);

    for (uint32_t index = 0; index < options.threads; ++index) {
        threads.emplace_back([&, index]() {
            auto& worker = workers[static_cast<std::size_t>(index)];
            if (!worker.client) {
                std::lock_guard lock(error_mutex);
                if (fatal_error.empty()) {
                    fatal_error = "测量阶段缺少线程专属 DBClient";
                }
                return;
            }

            worker.records.reserve(static_cast<std::size_t>(work[static_cast<std::size_t>(index)]));
            std::mt19937_64 rng(static_cast<uint64_t>(index + 1U) * 0x9E3779B97F4A7C15ULL);

            for (uint64_t request = 0; request < work[static_cast<std::size_t>(index)]; ++request) {
                const OperationKind operation = PickOperation(options.mode, request, rng);
                const auto wall_start = std::chrono::system_clock::now();
                const auto steady_start = std::chrono::steady_clock::now();

                bool success = false;
                if (operation == OperationKind::kUpsert) {
                    const uint64_t vector_id =
                        counters.next_vector_id.fetch_add(1, std::memory_order_acq_rel);
                    auto result = worker.client->Upsert(
                        UpsertRequest{.id = vector_id, .vector = BuildVectorForId(vector_id, options.dim)});
                    success = static_cast<bool>(result);
                    if (success) {
                        UpdateVisibleUpperBound(counters.visible_upper_bound, vector_id);
                    }
                } else if (operation == OperationKind::kDelete) {
                    const uint64_t vector_id =
                        counters.next_delete_id.fetch_add(1U, std::memory_order_acq_rel);
                    auto result = worker.client->Delete(DeleteRequest{.id = vector_id});
                    success = static_cast<bool>(result);
                } else {
                    const uint64_t visible = counters.visible_upper_bound.load(std::memory_order_acquire);
                    const uint64_t base = IdBaseForMode(options.mode);
                    const uint64_t range_begin = base;
                    const uint64_t range_end = visible < range_begin ? range_begin : visible;
                    const uint64_t target_id =
                        std::uniform_int_distribution<uint64_t>(range_begin, range_end)(rng);
                    const std::vector<float> query_vector =
                        options.mode == BenchMode::kFull ? BuildRandomVector(rng, options.dim)
                                                         : BuildVectorForId(target_id, options.dim);
                    auto result = worker.client->Search(
                        SearchRequest{.vector = std::move(query_vector), .top_k = 10U});
                    success = static_cast<bool>(result);
                }

                const auto steady_end = std::chrono::steady_clock::now();
                const auto timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                              wall_start.time_since_epoch())
                                              .count();
                const auto latency_us = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(steady_end - steady_start).count());

                worker.records.push_back(RequestRecord{
                    .timestamp_us = timestamp_us,
                    .latency_us = latency_us,
                    .success = success,
                    .mode = OperationKindToString(operation),
                });

                counters.completed.fetch_add(1, std::memory_order_acq_rel);
                if (!success) {
                    counters.failures.fetch_add(1, std::memory_order_acq_rel);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    measurement_done.store(true, std::memory_order_release);
    reporter.join();

    if (!fatal_error.empty()) {
        return Result<std::vector<RequestRecord>>::Err(fatal_error);
    }

    std::vector<RequestRecord> merged;
    merged.reserve(static_cast<std::size_t>(options.requests));
    for (auto& worker : workers) {
        merged.insert(merged.end(),
                      std::make_move_iterator(worker.records.begin()),
                      std::make_move_iterator(worker.records.end()));
    }
    return Result<std::vector<RequestRecord>>::Ok(std::move(merged));
}

} // namespace

int main(int argc, char** argv) {
    auto options = ParseCommandLine(argc, argv);
    if (!options) {
        PrintUsage(std::cerr, argv[0]);
        std::cerr << "\n错误: " << options.error << '\n';
        return 1;
    }

    if ((options->mode == BenchMode::kSearch || options->mode == BenchMode::kMixed ||
         options->mode == BenchMode::kDelete || options->mode == BenchMode::kFull) &&
        options->warmup == 0U) {
        std::cerr << "警告: 当前模式包含 Search/Delete，但 warmup=0；前几次操作可能命中空样本集。\n";
    }

    std::cout << "bench 配置:"
              << " peers=";
    for (std::size_t index = 0; index < options->peers.size(); ++index) {
        if (index != 0U) {
            std::cout << ',';
        }
        std::cout << options->peers[index];
    }
    std::cout << " threads=" << options->threads
              << " requests=" << options->requests
              << " dim=" << options->dim
              << " mode=" << BenchModeToString(options->mode)
              << " warmup=" << options->warmup << '\n';

    SharedCounters counters;
    InitializeCountersForMode(*options, counters);
    std::vector<WorkerContext> workers(options->threads);

    auto prefill = RunPrefillIfNeeded(*options, counters);
    if (!prefill) {
        std::cerr << "预填充失败: " << prefill.error << '\n';
        return 1;
    }

    uint64_t warmup_errors = 0;
    auto warmup = RunWarmup(*options, workers, counters, warmup_errors);
    if (!warmup) {
        std::cerr << "预热失败: " << warmup.error << '\n';
        return 1;
    }
    if (warmup_errors > 0U) {
        std::cerr << "警告: 预热阶段发生 " << warmup_errors
                  << " 次请求错误，这些错误不会计入最终统计。\n";
    }

    const auto measurement_begin = std::chrono::steady_clock::now();
    auto measured = RunMeasuredPhase(*options, workers, counters);
    const auto measurement_end = std::chrono::steady_clock::now();
    if (!measured) {
        std::cerr << "压测执行失败: " << measured.error << '\n';
        return 1;
    }

    auto write_csv = WriteCsv(options->output_path, *measured);
    if (!write_csv) {
        std::cerr << "写入 CSV 失败: " << write_csv.error << '\n';
        return 1;
    }

    PrintSummary(*options,
                 *measured,
                 counters.failures.load(std::memory_order_acquire),
                 measurement_end - measurement_begin);
    if (!options->output_path.empty()) {
        std::cout << "CSV 已写入: " << options->output_path << '\n';
    }
    return 0;
}
