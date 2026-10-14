// Exercise the production task/accounting helpers used by normal callbacks
// and controller cleanup. Real worker/transport ordering is covered by E2E.
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <stack>
#include <vector>

namespace mooncake {
namespace {

constexpr uint32_t kBlockSize = 4096;
constexpr int kSlotCount = 16;

// nof_seg_handle is opaque; the helpers only compare the pointer.
nof_seg_handle* FakeSegment() {
    static int anchor = 0;
    return reinterpret_cast<nof_seg_handle*>(&anchor);
}

class NofTaskLifecycleTest : public ::testing::Test {
   protected:
    void SetUp() override {
        qos_ = std::make_unique<SpdkNofQos>(kBlockSize);
        slots_.resize(kSlotCount);
        for (auto& slot : slots_) {
            slot.task = nullptr;
            slot.submit_lba_count = 0;
            slot.sub_task_pool = &pool_;
            pool_.push(&slot);
        }
    }

    // What the worker does when it takes a task off its queue.
    SpdkNofTask* Enqueue(int lba_count, int op,
                         std::shared_ptr<SpdkNofOperationState>* state_out) {
        auto state = std::make_shared<SpdkNofOperationState>();
        auto* task = new SpdkNofTask(FakeSegment(), buffer_, /*off=*/0,
                                     lba_count, op, state);
        task->io_count = &total_outstanding_io_;
        task->nof_qos = qos_.get();
        task->on_chain = true;
        qos_->PushTask(task);
        *state_out = std::move(state);
        return task;
    }

    // The bookkeeping the submit loop performs for one accepted sub-I/O.
    SpdkNofSubTask* Submit(SpdkNofTask* task, int lba_count) {
        EXPECT_FALSE(pool_.empty());
        SpdkNofSubTask* sub_task = pool_.top();
        pool_.pop();
        sub_task->task = task;
        sub_task->submit_lba_count = lba_count;
        task->idx++;
        task->remaining_lba -= lba_count;
        qos_->inflight_blocks[task->op] += lba_count;
        task->outstanding_sub_io++;
        total_outstanding_io_++;
        return sub_task;
    }

    // The submit loop pops a task once all of its LBAs have been submitted
    // (or it was failed) and lets SpdkNofTaskCompletion() decide whether it
    // is finished. After this the task pointer must not be used.
    void PopFromChain(SpdkNofTask* task) {
        ASSERT_EQ(qos_->head[task->op], task);
        qos_->PopTask(task->op);
        task->on_chain = false;
        SpdkNofTaskCompletion(task);
    }

    void ExpectBaseline() {
        EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount));
        EXPECT_EQ(qos_->InflightBlocks(), 0);
        EXPECT_EQ(total_outstanding_io_, 0);
        EXPECT_TRUE(qos_->Empty());
    }

    alignas(kBlockSize) char buffer_[kBlockSize];
    std::unique_ptr<SpdkNofQos> qos_;
    std::vector<SpdkNofSubTask> slots_;
    std::stack<SpdkNofSubTask*> pool_;
    int64_t total_outstanding_io_ = 0;
};

TEST_F(NofTaskLifecycleTest, NormalCompletion) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/2, kSpdkNofOpWrite, &state);
    SpdkNofSubTask* first = Submit(task, 1);
    SpdkNofSubTask* second = Submit(task, 1);
    PopFromChain(task);  // fully submitted, still 2 sub-I/Os outstanding
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount - 2));

    SpdkNofCompleteSubTask(first, /*failed=*/false);
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(first->task, nullptr);

    SpdkNofCompleteSubTask(second, /*failed=*/false);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, ErrorOnPartiallySubmittedTaskCompletesCallerOnce) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/3, kSpdkNofOpRead, &state);
    SpdkNofSubTask* sub_task = Submit(task, 1);

    // An error callback may run while this partially submitted task is
    // still linked. Do not publish completion until the worker unlinks it.
    SpdkNofCompleteSubTask(sub_task, /*failed=*/true);
    EXPECT_FALSE(state->is_completed());
    EXPECT_TRUE(task->on_chain);
    EXPECT_EQ(task->remaining_lba, 0);

    PopFromChain(task);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, SlotIsReturnedOnlyByCompletion) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/1, kSpdkNofOpWrite, &state);
    SpdkNofSubTask* sub_task = Submit(task, 1);
    PopFromChain(task);

    // Neither the caller nor the pool may see anything until SPDK is done
    // with the callback context.
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount - 1));
    EXPECT_EQ(sub_task->task, task);

    SpdkNofCompleteSubTask(sub_task, /*failed=*/false);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount));
    EXPECT_EQ(pool_.top(), sub_task);
    EXPECT_EQ(sub_task->task, nullptr);
}

// Controller cleanup first unlinks queued work, then polls asynchronous
// disconnect until every accepted request has called back.
TEST_F(NofTaskLifecycleTest, CleanupWaitsForEveryAcceptedSubIo) {
    std::shared_ptr<SpdkNofOperationState> state;
    auto* task = Enqueue(4, kSpdkNofOpWrite, &state);
    auto* first = Submit(task, 1);
    auto* second = Submit(task, 1);
    task->failed = true;
    task->remaining_lba = 0;
    PopFromChain(task);
    EXPECT_FALSE(state->is_completed());
    SpdkNofCompleteSubTask(first, true);
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(total_outstanding_io_, 1);
    SpdkNofCompleteSubTask(second, true);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, CompletedChunksDoNotMaskAnAbortedChunk) {
    std::shared_ptr<SpdkNofOperationState> state;
    auto* task = Enqueue(3, kSpdkNofOpRead, &state);
    auto* first = Submit(task, 1);
    auto* second = Submit(task, 1);
    auto* third = Submit(task, 1);
    PopFromChain(task);
    SpdkNofCompleteSubTask(first, false);
    SpdkNofCompleteSubTask(second, true);
    EXPECT_FALSE(state->is_completed());
    SpdkNofCompleteSubTask(third, false);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, CleanupFailsQueuedWorkWithoutCallbacks) {
    std::shared_ptr<SpdkNofOperationState> state;
    auto* task = Enqueue(4, kSpdkNofOpWrite, &state);
    task->remaining_lba = 0;
    task->failed = true;
    PopFromChain(task);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, RecoveredIoReusesReclaimedSlots) {
    for (int cycle = 0; cycle < 20; ++cycle) {
        for (bool failed : {true, false}) {
            std::shared_ptr<SpdkNofOperationState> state;
            auto* task = Enqueue(1, kSpdkNofOpRead, &state);
            auto* sub = Submit(task, 1);
            PopFromChain(task);
            SpdkNofCompleteSubTask(sub, failed);
            ASSERT_TRUE(state->is_completed());
            EXPECT_EQ(state->get_result(),
                      failed ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK);
            ExpectBaseline();
        }
    }
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging("NofTaskLifecycleTest");
    FLAGS_logtostderr = true;
    return RUN_ALL_TESTS();
}
