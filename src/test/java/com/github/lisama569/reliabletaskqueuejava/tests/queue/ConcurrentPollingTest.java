package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖并发消费：多个消费者同时拉取时，
 * 每个任务在任一时刻只能被一个消费者占用，全部任务恰好被成功确认一次。
 */
class ConcurrentPollingTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentPollingTest.class);

    @TempDir
    Path tempDir;

    @Test
    void concurrentConsumers_eachTaskClaimedAndAckedExactlyOnce() throws Exception {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        int taskCount = 50;
        int consumerCount = 8;
        for (int i = 0; i < taskCount; i++) {
            queue.enqueue("task-" + i, "payload-" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(consumerCount);
        CountDownLatch start = new CountDownLatch(1);
        Set<String> claimedOwners = ConcurrentHashMap.newKeySet();
        AtomicInteger successfulAcks = new AtomicInteger();
        AtomicInteger emptyPolls = new AtomicInteger();
        List<String> errors = Collections.synchronizedList(new ArrayList<>());

        for (int c = 0; c < consumerCount; c++) {
            final String owner = "consumer-" + c;
            pool.submit(() -> {
                try {
                    start.await();
                    while (true) {
                        TaskRecord t;
                        synchronized (queue) {
                            t = queue.poll(owner);
                        }
                        if (t == null) {
                            emptyPolls.incrementAndGet();
                            if (successfulAcks.get() + queue.listDeadLetters().size() >= taskCount) {
                                break;
                            }
                            if (emptyPolls.get() > 5_000) {
                                break;
                            }
                            continue;
                        }
                        if (!claimedOwners.add(t.getIdempotencyKey() + "@" + t.getOffset())) {
                            errors.add("task claimed twice: " + t.getIdempotencyKey());
                        }
                        // 校验：此刻记录中的占用者必须是自己
                        TaskRecord fresh = queue.get(t.getOffset());
                        if (!owner.equals(fresh.getOwnerToken()) || fresh.getState() != TaskState.RUNNING) {
                            errors.add("unexpected owner/state for " + t.getIdempotencyKey());
                        }
                        AckOutcome ack = queue.ack(t.getOffset(), owner);
                        if (ack.status() == AckStatus.APPLIED) {
                            successfulAcks.incrementAndGet();
                        } else {
                            errors.add("ack failed with " + ack.status() + " for " + t.getIdempotencyKey());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "消费者线程应在限定时间内结束");

        log.info("[test] concurrent run finished: tasks={}, successfulAcks={}, emptyPolls={}, uniqueClaims={}",
                taskCount, successfulAcks.get(), emptyPolls.get(), claimedOwners.size());
        assertTrue(errors.isEmpty(), "并发抢占不允许出现重复占用或错误确认: " + errors);
        assertEquals(taskCount, successfulAcks.get(), "每个任务必须恰好成功确认一次");
        assertEquals(taskCount, claimedOwners.size());
        assertEquals(taskCount, queue.committedOffset(), "全部连续成功后位点推进到末尾");
        assertEquals(0, queue.listDeadLetters().size());

        assertNull(queue.poll("late-consumer"), "全部确认后不应再有可消费任务");
        Set<Long> offsets = new HashSet<>();
        queue.listAll().forEach(t -> {
            offsets.add(t.getOffset());
            assertEquals(TaskState.SUCCEEDED, t.getState());
        });
        assertEquals(taskCount, offsets.size());
    }
}
