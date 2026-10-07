package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.PendingReason;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖消费者主动放弃 / 优雅退出：
 * 放弃后任务立即可被他人重新领取、同一时刻只有一个有效占用者、
 * 放弃后原占用者的确认被拒绝，以及 PENDING 原因可解释。
 */
class AbandonAndReclaimTest {

    private static final Logger log = LoggerFactory.getLogger(AbandonAndReclaimTest.class);

    @TempDir
    Path tempDir;

    @Test
    void abandonMakesTaskImmediatelyReclaimable_andStaleAckRejected() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "job-give-up-1";
        queue.enqueue(key, "work");
        TaskRecord t = queue.poll("consumer-quitting");
        assertEquals(1, t.getAttempts());

        // 租约远未到期，其他消费者本来 poll 不到
        assertNull(queue.poll("consumer-2"), "放弃前租约仍有效，他人不能抢占");

        AckOutcome abandoned = queue.abandon(t.getOffset(), "consumer-quitting", "graceful shutdown");
        log.info("[test] key='{}' abandon -> {}, state={}, pendingReason={}",
                key, abandoned.status(), t.getState(), t.getPendingReason());
        assertEquals(AckStatus.APPLIED, abandoned.status());
        assertEquals(TaskState.PENDING, t.getState());
        assertEquals(PendingReason.ABANDONED, t.getPendingReason(), "放弃重投必须有唯一可解释的 PENDING 原因");
        assertNull(t.getOwnerToken(), "放弃后不得再保留占用者");

        // 不需要等租约过期：任务立刻可被重新领取
        TaskRecord reclaimed = queue.poll("consumer-2");
        assertNotNull(reclaimed, "放弃后任务必须立即可被其他消费者领取");
        assertEquals(2, reclaimed.getAttempts(), "重新投递累加投递次数（至少一次）");
        assertEquals("consumer-2", reclaimed.getOwnerToken());
        assertEquals(TaskState.RUNNING, reclaimed.getState());

        // 原占用者晚到的确认/上报必须被拒绝，不能覆盖新占用者的结果
        AckOutcome staleAck = queue.ack(t.getOffset(), "consumer-quitting");
        AckOutcome staleNack = queue.nack(t.getOffset(), "consumer-quitting", "late");
        log.info("[test] key='{}' stale ack after abandon -> {}, stale nack -> {}",
                key, staleAck.status(), staleNack.status());
        assertEquals(AckStatus.LEASE_EXPIRED, staleAck.status());
        assertEquals(AckStatus.LEASE_EXPIRED, staleNack.status());
        assertEquals("consumer-2", t.getOwnerToken());

        // 新占用者正常完成
        assertEquals(AckStatus.APPLIED, queue.ack(t.getOffset(), "consumer-2").status());
        assertEquals(TaskState.SUCCEEDED, t.getState());
    }

    @Test
    void abandonByNonOwnerOrAfterLeaseExpiry_isRejected() {
        TestClock clock = new TestClock(2_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "job-give-up-2";
        queue.enqueue(key, "work");
        TaskRecord t = queue.poll("owner-a");

        AckOutcome wrongOwner = queue.abandon(t.getOffset(), "owner-b", "not mine");
        assertEquals(AckStatus.LEASE_EXPIRED, wrongOwner.status(), "非占用者不能放弃别人的任务");
        assertEquals("owner-a", t.getOwnerToken(), "被拒后占用关系不变");

        // 租约过期并被接手后，原占用者的放弃也要被拒绝
        clock.advance(30_001L);
        assertNotNull(queue.poll("owner-c"));
        AckOutcome stale = queue.abandon(t.getOffset(), "owner-a", "late quit");
        log.info("[test] key='{}' abandon after takeover -> {}", key, stale.status());
        assertEquals(AckStatus.LEASE_EXPIRED, stale.status());
        assertEquals("owner-c", t.getOwnerToken());

        // 对未占用任务放弃返回 NOT_RUNNING
        queue.enqueue("job-pending", "w");
        AckOutcome notRunning = queue.abandon(t.getOffset() + 1, "owner-a", null);
        assertEquals(AckStatus.NOT_RUNNING, notRunning.status());
    }

    /**
     * 并发验证：一个任务被原消费者放弃的同一时刻，多个其他消费者真实同时争用，
     * 任一时刻只能有一个有效占用者，且最终恰好被成功确认一次。
     */
    @Test
    void concurrentReclaimAfterAbandon_singleOwnerAtAnyTime_ackedExactlyOnce() throws Exception {
        TestClock clock = new TestClock(3_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        final int taskCount = 40;
        final int reclaimConsumers = 8;
        for (int i = 0; i < taskCount; i++) {
            queue.enqueue("abandon-job-" + i, "payload");
        }
        // 单线程先占用全部任务，随后优雅退出（逐个放弃）
        List<TaskRecord> claimed = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            claimed.add(queue.poll("leaving-consumer"));
        }
        assertEquals(taskCount, claimed.size());

        ExecutorService pool = Executors.newFixedThreadPool(reclaimConsumers + 1);
        CountDownLatch abandonGate = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger acks = new AtomicInteger();
        List<String> ownerObservations = Collections.synchronizedList(new ArrayList<>());

        // 放弃线程：放开闸门后连续放弃全部任务
        pool.submit(() -> {
            try {
                abandonGate.await();
                for (TaskRecord t : claimed) {
                    AckOutcome o = queue.abandon(t.getOffset(), "leaving-consumer", "shutdown");
                    if (o.status() != AckStatus.APPLIED) {
                        failure.compareAndSet(null,
                                new AssertionError("abandon must apply, got " + o.status()
                                        + " for " + t.getIdempotencyKey()));
                    }
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });

        // 多个真实消费者在同一时刻开始争抢被放弃的任务
        CountDownLatch done = new CountDownLatch(reclaimConsumers);
        for (int c = 0; c < reclaimConsumers; c++) {
            final String owner = "reclaimer-" + c;
            pool.submit(() -> {
                try {
                    abandonGate.await();
                    while (acks.get() < taskCount && failure.get() == null) {
                        TaskRecord t = queue.poll(owner);
                        if (t == null) {
                            if (queue.listAll().stream().allMatch(x -> x.getState() == TaskState.SUCCEEDED)) {
                                break;
                            }
                            continue;
                        }
                        // 领取后立刻校验：此刻有效占用者必须且只能是自己
                        TaskRecord fresh = queue.get(t.getOffset());
                        ownerObservations.add(t.getIdempotencyKey() + "->" + fresh.getOwnerToken());
                        if (!owner.equals(fresh.getOwnerToken()) || fresh.getState() != TaskState.RUNNING) {
                            failure.compareAndSet(null,
                                    new AssertionError("dual owner detected for " + t.getIdempotencyKey()));
                        }
                        AckOutcome ack = queue.ack(t.getOffset(), owner);
                        if (ack.status() != AckStatus.APPLIED) {
                            failure.compareAndSet(null,
                                    new AssertionError("ack must apply, got " + ack.status()
                                            + " for " + t.getIdempotencyKey()));
                        } else {
                            acks.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            });
        }

        abandonGate.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "争用消费者应在限定时间内结束");
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        log.info("[test] concurrent reclaim finished: tasks={}, successfulAcks={}, ownerObservations={}",
                taskCount, acks.get(), ownerObservations.size());
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertEquals(taskCount, acks.get(), "每个被放弃的任务必须恰好被成功确认一次");
        assertEquals(taskCount, queue.committedOffset());
        queue.listAll().forEach(t -> assertEquals(TaskState.SUCCEEDED, t.getState()));

        // 原占用者退出后对所有任务的确认都必须被拒绝
        queue.listAll().forEach(t ->
                assertEquals(AckStatus.ALREADY_ACKED, queue.ack(t.getOffset(), "leaving-consumer").status(),
                        "已成功任务的重复确认是无害空操作"));
    }
}
