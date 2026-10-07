package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
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
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 覆盖队列容量上限与背压：
 * 满员时新提交得到确定的 QUEUE_FULL 且不丢任务；
 * 幂等重复提交（含已完成/已死信）不额外占用容量，也不改变幂等结果。
 */
class CapacityBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityBackpressureTest.class);

    @TempDir
    Path tempDir;

    @Test
    void newSubmissionsAtCapacity_areRejectedWithDeterministicQueueFull_andNotSilentlyDropped() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 2, 1_000L, 60_000L);
        assertEquals(2, queue.capacity());

        assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("k1", "p1").status());
        assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("k2", "p2").status());
        assertEquals(2, queue.currentSize());

        EnqueueResult rejected = queue.enqueue("k3", "p3");
        log.info("[test] enqueue at capacity -> {} (task={})", rejected.status(), rejected.task());
        assertEquals(EnqueueStatus.QUEUE_FULL, rejected.status(), "达到上限必须返回确定、可解释的背压结果");
        assertNull(rejected.task(), "被背压的提交没有任何任务实体，绝不悄悄落库");
        assertEquals(2, queue.currentSize(), "被拒绝的任务不得占用容量");
        assertEquals(2, queue.listAll().size(), "被拒绝的任务绝不能被静默丢入队列");

        // 已入队的任务照常可消费、确认——容量限制不改变既有语义
        TaskRecord t1 = queue.poll("c1");
        assertEquals("k1", t1.getIdempotencyKey());
        assertEquals(AckStatus.APPLIED, queue.ack(t1.getOffset(), "c1").status());

        // 完成一个条目后容量条目数不减少（完成的任务仍保留为记录），仍为满员：
        // 容量按“任务条目（不同幂等键）”计数，去重/完成不重复占容量，但完成本身不清理条目
        EnqueueResult stillFull = queue.enqueue("k3", "p3");
        log.info("[test] after completing one retained record, enqueue new key -> {}", stillFull.status());
        assertEquals(EnqueueStatus.QUEUE_FULL, stillFull.status());
    }

    @Test
    void duplicateSubmissionsDoNotConsumeCapacity_evenWhenFull() {
        TestClock clock = new TestClock(2_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1, 1_000L, 60_000L);

        queue.enqueue("dup-key", "v1");
        // 队列已满（容量 1）
        assertEquals(EnqueueStatus.QUEUE_FULL, queue.enqueue("other-key", "x").status());

        // 完成前重复提交：即使满员也返回 DEDUPLICATED，不被容量限制挡住、不额外占容量
        EnqueueResult dedup = queue.enqueue("dup-key", "v2");
        log.info("[test] duplicate of in-flight key while full -> {}", dedup.status());
        assertEquals(EnqueueStatus.DEDUPLICATED, dedup.status());
        assertEquals(1, queue.currentSize());

        // 完成后重复提交：仍返回 ALREADY_SUCCEEDED，容量限制不能改变幂等结果
        TaskRecord t = queue.poll("w");
        assertEquals(AckStatus.APPLIED, queue.ack(t.getOffset(), "w").status());
        EnqueueResult already = queue.enqueue("dup-key", "v3");
        log.info("[test] duplicate of succeeded key while full -> {}", already.status());
        assertEquals(EnqueueStatus.ALREADY_SUCCEEDED, already.status());
        assertSame(t, already.task());
        assertEquals(1, queue.currentSize(), "重复提交始终不增加条目");
        // 已成功任务不会因为“满”而被重新投递
        assertNull(queue.poll("w2"));
    }

    @Test
    void duplicateOfDeadLetterDoesNotConsumeCapacity_orChangeIdempotentResult() {
        TestClock clock = new TestClock(3_000_000L);
        // maxAttempts=1：首次投递后 nack 即入死信；退避设很短不影响本用例
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 1, 30_000L, 1, 1_000L, 60_000L);

        queue.enqueue("dead-key", "p");
        TaskRecord t = queue.poll("w");
        assertEquals(AckStatus.APPLIED, queue.nack(t.getOffset(), "w", "fatal").status());
        assertEquals(TaskState.DEAD, t.getState());
        assertEquals(1, queue.currentSize());

        // 满员状态下重提已死信 key：必须返回 ALREADY_DEAD（而不是 QUEUE_FULL），不新增条目
        EnqueueResult dead = queue.enqueue("dead-key", "p2");
        log.info("[test] duplicate of dead key while full -> {}", dead.status());
        assertEquals(EnqueueStatus.ALREADY_DEAD, dead.status());
        assertSame(t, dead.task());
        assertEquals(1, queue.currentSize(), "死信重复提交不额外占用容量");

        EnqueueResult otherRejected = queue.enqueue("brand-new", "p");
        assertEquals(EnqueueStatus.QUEUE_FULL, otherRejected.status(),
                "真正的新 key 在满员时仍被背压，与死信幂等结果可区分");
    }

    @Test
    void unlimitedCapacityByDefault_keepsLegacyBehavior() {
        TestClock clock = new TestClock(4_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);
        assertEquals(0, queue.capacity(), "capacity<=0 表示不限制（旧行为）");
        for (int i = 0; i < 100; i++) {
            assertEquals(EnqueueStatus.ACCEPTED, queue.enqueue("bulk-" + i, "p").status());
        }
        assertEquals(100, queue.currentSize());
    }
}
