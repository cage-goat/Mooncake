#include <gflags/gflags.h>
#include <glog/logging.h>
#include <spdk/env.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "transfer_task.h"

DEFINE_string(endpoint, "", "Controller endpoint without namespace ID");
DEFINE_string(control_dir, "", "Runner-owned lifecycle marker directory");
DEFINE_string(mode, "resume",
              "resume/restart/disabled/short/shutdown/pool_exit; *_recovery "
              "exits after timeout");
DEFINE_uint32(cycles, 3, "Number of faults using the same pool and handles");
DEFINE_uint32(workers, 2, "Workers per pool");
DEFINE_uint32(pools, 2, "Pools sharing the controller on different namespaces");
DEFINE_uint32(timeout_sec, 20, "Deadline for each phase");

namespace {
using namespace mooncake;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Buffer = std::unique_ptr<void, decltype(&spdk_free)>;
using Requests = std::vector<std::shared_ptr<SpdkNofOperationState>>;
constexpr size_t kNamespaces = 4;
constexpr size_t kRequestsPerNamespace = 3;
constexpr size_t kIoSize = 256 * 1024;

void Require(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

auto Deadline() {
    return Clock::now() + std::chrono::seconds(FLAGS_timeout_sec);
}

void WaitForMarker(const std::string& name) {
    const auto deadline = Deadline();
    auto path = std::filesystem::path(FLAGS_control_dir) / name;
    while (!std::filesystem::exists(path)) {
        Require(Clock::now() < deadline, "Missing marker: " + path.string());
        std::this_thread::sleep_for(1ms);
    }
}

void WorkerBarrier(SpdkNofWorkerPool& pool, unsigned index) {
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    pool.PostControlTask(index, [done](auto&) { done->set_value(); });
    Require(future.wait_until(Deadline()) == std::future_status::ready,
            "Worker barrier timed out");
}

struct Client {
    // Destruction joins the pools before releasing DMA buffers, even on
    // failure.
    std::vector<Buffer> buffers;
    std::vector<std::unique_ptr<SpdkNofWorkerPool>> pools;
    std::vector<nof_seg_handle*> segments;

    Client() {
        Require(FLAGS_pools >= 1 && FLAGS_pools <= 2 && FLAGS_workers >= 1 &&
                    FLAGS_workers <= 2,
                "Use one or two pools and workers");
        auto worker_count = std::to_string(FLAGS_workers);
        Require(setenv("MC_NOF_WORKERS", worker_count.c_str(), 1) == 0,
                "setenv failed");
        auto& wrapper = SpdkWrapper::GetInstance();
        for (size_t i = 0; i < kNamespaces * kRequestsPerNamespace; ++i) {
            buffers.emplace_back(wrapper.Alloc(kIoSize, 4096, -1), spdk_free);
            Require(buffers.back() != nullptr, "DMA allocation failed");
        }
        for (unsigned i = 0; i < FLAGS_pools; ++i)
            pools.push_back(std::make_unique<SpdkNofWorkerPool>());
        for (size_t i = 0; i < kNamespaces; ++i) {
            auto* seg = pools[i % pools.size()]->OpenNofSegment(
                FLAGS_endpoint + " ns:" + std::to_string(i + 1));
            Require(seg != nullptr, "Open failed");
            const auto block_size = wrapper.GetBlockSize(seg);
            Require(block_size > 0 && block_size <= kIoSize &&
                        kIoSize % block_size == 0,
                    "Invalid block size");
            segments.push_back(seg);
            std::cout << "TEST namespace nsid=" << i + 1
                      << " seg_handle=" << seg
                      << " pool_idx=" << i % pools.size() << std::endl;
            Require(wrapper.GetController(seg) ==
                        wrapper.GetController(segments[0]),
                    "Namespaces must share a controller");
        }
        if (pools.size() == 2) {
            Require(
                pools[1]->OpenNofSegment(FLAGS_endpoint + " ns:1") == nullptr,
                "Another pool must not steal an already owned qpair");
        }
    }

    bool Active(size_t index) const {
        return pools[(index / kRequestsPerNamespace) % pools.size()] != nullptr;
    }

    void Barrier() {
        // The second batch runs after the preceding I/O snapshot was processed.
        for (int round = 0; round < 2; ++round)
            for (auto& pool : pools)
                if (pool)
                    for (unsigned i = 0; i < FLAGS_workers; ++i)
                        WorkerBarrier(*pool, i);
    }

    Requests Submit(int op, bool mixed = false) {
        Requests requests;
        auto& wrapper = SpdkWrapper::GetInstance();
        for (size_t i = 0; i < buffers.size(); ++i) {
            if (!Active(i)) continue;
            const size_t ns = i / kRequestsPerNamespace;
            const auto block_size = wrapper.GetBlockSize(segments[ns]);
            auto request = std::make_shared<SpdkNofOperationState>();
            // Separate LBAs and buffers for queued, partially submitted and
            // outstanding tasks. Reads and writes exercise both QoS lists.
            const auto lba = (i % kRequestsPerNamespace) * kIoSize / block_size;
            pools[ns % pools.size()]->submitTask(SpdkNofTask(
                segments[ns], buffers[i].get(), lba, kIoSize / block_size,
                mixed ? static_cast<int>(i % 2) : op, request));
            requests.push_back(std::move(request));
        }
        return requests;
    }

    void Fill(unsigned char pattern) {
        for (auto& buffer : buffers)
            std::memset(buffer.get(), pattern, kIoSize);
    }

    void Check(unsigned char pattern, bool active_only = true) {
        for (size_t i = 0; i < buffers.size(); ++i) {
            if (active_only && !Active(i)) continue;
            const auto* bytes =
                static_cast<const unsigned char*>(buffers[i].get());
            for (size_t j = 0; j < kIoSize; ++j)
                Require(bytes[j] == pattern, "Buffer mismatch at request " +
                                                 std::to_string(i) + " byte " +
                                                 std::to_string(j));
        }
    }
};

bool Wait(const Requests& requests, Clock::time_point deadline) {
    bool ok = true;
    for (const auto& request : requests) {
        while (!request->is_completed()) {
            Require(Clock::now() < deadline,
                    "I/O completion deadline exceeded");
            std::this_thread::sleep_for(1ms);
        }
        ok &= request->get_result() == ErrorCode::OK;
    }
    return ok;
}

void VerifyIo(Client& client, unsigned char pattern, bool retry) {
    const auto deadline = Deadline();
    do {
        client.Fill(pattern);
        if (Wait(client.Submit(kSpdkNofOpWrite), deadline)) {
            client.Fill(0);
            if (Wait(client.Submit(kSpdkNofOpRead), deadline)) {
                client.Check(pattern);
                return;
            }
        }
        Require(retry && Clock::now() < deadline, "Healthy I/O failed");
        std::this_thread::sleep_for(10ms);
    } while (true);
}

void Run() {
    Require(!FLAGS_endpoint.empty() && !FLAGS_control_dir.empty() &&
                FLAGS_timeout_sec > 0 && FLAGS_cycles > 0,
            "Invalid test configuration");
    Client client;
    VerifyIo(client, 0x31, true);
    std::cout << "PASS initial_io" << std::endl;
    const bool should_timeout =
        FLAGS_mode != "disabled" && FLAGS_mode != "short";
    for (unsigned cycle = 1; cycle <= FLAGS_cycles; ++cycle) {
        const auto suffix = std::to_string(cycle);
        std::cout << "READY cycle=" << cycle << std::endl;
        WaitForMarker("paused-" + suffix);
        const auto started = Clock::now();
        const auto pending = client.Submit(kSpdkNofOpRead, true);
        client.Barrier();
        for (const auto& request : pending)
            Require(!request->is_completed(), "Fault I/O was not pending");
        std::cout << "READY pending cycle=" << cycle << std::endl;

        if (FLAGS_mode == "shutdown") {
            for (auto& pool : client.pools) pool.reset();
        } else if (FLAGS_mode == "pool_exit") {
            Require(client.pools.size() == 2, "pool_exit needs two pools");
            client.pools[0].reset();
        }
        if (!should_timeout) {
            WaitForMarker("check-pending-" + suffix);
            for (const auto& request : pending)
                Require(!request->is_completed(), "Unexpected timeout/reset");
            std::cout << "PASS still_pending cycle=" << cycle << std::endl;
            WaitForMarker("resumed-" + suffix);
        }
        const bool ok = Wait(pending, Deadline());
        Require(ok != should_timeout, "Unexpected fault I/O result");
        if (!should_timeout) client.Check(cycle == 1 ? 0x31 : 0x6f);
        if (should_timeout) {
            for (const auto& request : pending)
                Require(request->get_result() == ErrorCode::TRANSFER_FAIL,
                        "Every outstanding/queued request must fail");
        }
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                                  started)
                .count();
        std::cout << "PASS drained cycle=" << cycle
                  << " requests=" << pending.size() << " elapsed_ms=" << elapsed
                  << std::endl;
        if (FLAGS_mode == "shutdown") return;

        if (FLAGS_mode == "shutdown_recovery" ||
            FLAGS_mode == "pool_exit_recovery") {
            // The runner observes an actual reconnect retry while the target
            // remains paused before allowing pool destruction.
            WaitForMarker("exit-recovery-" + suffix);
            if (FLAGS_mode == "shutdown_recovery") {
                for (auto& pool : client.pools) pool.reset();
                return;
            }
            Require(client.pools.size() == 2,
                    "pool_exit_recovery needs two pools");
            client.pools[0].reset();
            std::cout << "PASS pool_exited cycle=" << cycle << std::endl;
        }

        // A caller can reuse a failed read buffer immediately. A late
        // completion on an old connection must not overwrite this canary after
        // resume.
        client.Fill(0xa5);
        if (should_timeout) WaitForMarker("resumed-" + suffix);
        std::this_thread::sleep_for(500ms);
        client.Check(0xa5, false);
        std::cout << "PASS no_late_buffer_access cycle=" << cycle << std::endl;
        // Reuse the original handles. New writes precede reads because a
        // restarted malloc target does not preserve data.
        VerifyIo(client, 0x40 + cycle, should_timeout);
        for (int i = 0; i < 16; ++i) VerifyIo(client, 0x60 + i, false);
        client.Barrier();
        std::this_thread::sleep_for(300ms);
        std::cout << "PASS recovered cycle=" << cycle << std::endl;
    }
}
}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    try {
        Run();
        std::cout << "PASS stale_io_recovery mode=" << FLAGS_mode << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL stale_io_recovery: " << error.what() << std::endl;
        return 1;
    }
}
