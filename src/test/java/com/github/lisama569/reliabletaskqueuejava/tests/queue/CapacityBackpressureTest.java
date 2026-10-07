package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueResult;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 覆盖容量背压：达到上限后新提交返回确定结果（不静默丢弃）；
 * 幂等重复提交、已完成/已死信任务不占容量，也不因容量限制改变幂等结果。
 */
class CapacityBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityBackpressureTest.class);

    @TempDir
    Path tempDir;

    private InMemoryReliableTaskQueue newQueue(TestClock clock, int maxCapacity) {
        return QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 3, 30_000L, maxCapacity, 1_000L, 60_000L);
    }

    @Test
    void newSubmissionsRejectedDeterministicallyWhenCapacityReached() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock, 2);

        assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("k1", "p").status());
        assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("k2", "p").status());
        assertEquals(2, queue.inFlightCount());

        EnqueueResult rejected = queue.enqueue("k3", "p");
        assertEquals(EnqueueStatus.REJECTED_CAPACITY, rejected.status());
        assertNull(rejected.task(), "容量拒绝时返回结果不含任务，调用方可明确解释");
        assertEquals(2, queue.listAll().size(), "被拒绝的任务不能悄悄入队，也不能覆盖已有任务");
        assertEquals(2, queue.inFlightCount());
        log.info("[test] capacity reached: inFlight={}/{}, k3 rejected deterministically",
                queue.inFlightCount(), queue.maxCapacity());

        // 确认一个任务释放容量后，新提交恢复 ACCEPTED
        TaskRecord claimed = queue.poll("w");
        queue.ack(claimed.getOffset(), "w");
        assertEquals(1, queue.inFlightCount());
        assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("k3", "p").status());
    }

    @Test
    void duplicateAndTerminalResubmissionsDoNotConsumeCapacity() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock, 2);

        queue.enqueue("pending-task", "p");
        TaskRecord running = queue.poll("w"); // pending-task -> RUNNING
        queue.enqueue("done-task", "p");
        TaskRecord done = queue.poll("w");
        queue.ack(done.getOffset(), "w"); // done-task -> SUCCEEDED，不再占容量
        assertEquals(1, queue.inFlightCount(), "SUCCEEDED 不占在途容量");

        // 容量未满时先验证：重复提交不占容量、幂等结果不变
        assertEquals(EnqueueStatus.DEDUPLICATED, queue.enqueue("pending-task", "p").status());
        assertEquals(EnqueueStatus.ALREADY_SUCCEEDED, queue.enqueue("done-task", "p").status());
        assertEquals(1, queue.inFlightCount(), "重复提交不得额外占用容量");

        // 把容量填满：死信任务同样不占容量
        queue.enqueue("dead-task", "p");
        TaskRecord d1 = queue.poll("w");
        queue.nack(d1.getOffset(), "w", "boom-1");
        clock.advance(1_000L);
        TaskRecord d2 = queue.poll("w");
        queue.nack(d2.getOffset(), "w", "boom-2");
        clock.advance(2_000L);
        TaskRecord d3 = queue.poll("w");
        queue.nack(d3.getOffset(), "w", "boom-3");
        assertEquals(TaskState.DEAD, queue.get(d3.getOffset()).getState());
        assertEquals(1, queue.inFlightCount(), "DEAD 不占在途容量");

        // 占容量的只有 pending-task(RUNNING)，再补一个就满
        assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("filler", "p").status());
        assertEquals(2, queue.inFlightCount());

        // 容量已满：新键被拒绝，但各种幂等重提的结果完全不受容量影响
        assertEquals(EnqueueStatus.REJECTED_CAPACITY, queue.enqueue("brand-new", "p").status());
        assertEquals(EnqueueStatus.DEDUPLICATED, queue.enqueue("pending-task", "p").status());
        assertEquals(EnqueueStatus.ALREADY_SUCCEEDED, queue.enqueue("done-task", "p").status());
        assertEquals(EnqueueStatus.ALREADY_DEAD, queue.enqueue("dead-task", "p").status());
        assertEquals(EnqueueStatus.DEDUPLICATED, queue.enqueue("filler", "p").status());
        assertEquals(2, queue.inFlightCount(), "容量满时的幂等重提不得改变在途数");
        assertEquals(4, queue.listAll().size(), "被拒绝与去重的提交都不产生新任务");
    }

    @Test
    void unlimitedCapacityByDefaultKeepsLegacyBehavior() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock, 0);
        for (int i = 0; i < 100; i++) {
            assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("t-" + i, "p").status());
        }
        assertEquals(100, queue.inFlightCount());
        assertEquals(0, queue.maxCapacity(), "默认不限容量，保持旧行为");
    }
}
