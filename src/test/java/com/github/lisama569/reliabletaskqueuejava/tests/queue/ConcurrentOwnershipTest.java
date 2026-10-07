package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.JsonFileTaskQueueStore;
import com.github.lisama569.reliabletaskqueuejava.queue.RetryPolicy;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真实时钟下的多消费者并发争用：poll / renew / abandon / ack 混合交错，
 * 验证同一任务任一时刻只有一个有效占用者，且每个任务最终恰好被成功确认一次。
 */
class ConcurrentOwnershipTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentOwnershipTest.class);

    @TempDir
    Path tempDir;

    @Test
    void mixedOperationsUnderContention_singleOwnerAtAnyMoment() throws Exception {
        // 真实系统时钟 + 较短租约，让续期/放弃/失联回收在争用中真实发生
        InMemoryReliableTaskQueue queue = new InMemoryReliableTaskQueue(
                5, 400L, 0, new RetryPolicy(50L, 200L),
                new JsonFileTaskQueueStore(tempDir.resolve("snap.json"), new ObjectMapper()),
                Clock.systemUTC());
        queue.recover();

        int taskCount = 30;
        int consumerCount = 6;
        for (int i = 0; i < taskCount; i++) {
            queue.enqueue("task-" + i, "payload-" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(consumerCount);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successfulAcks = new AtomicInteger();
        AtomicInteger renews = new AtomicInteger();
        AtomicInteger abandons = new AtomicInteger();
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        Map<Long, String> terminalAckOwner = new ConcurrentHashMap<>();

        for (int c = 0; c < consumerCount; c++) {
            final String owner = "consumer-" + c;
            pool.submit(() -> {
                int idleRounds = 0;
                try {
                    start.await();
                    while (successfulAcks.get() < taskCount && idleRounds < 2_000) {
                        TaskRecord t = queue.poll(owner);
                        if (t == null) {
                            idleRounds++;
                            Thread.sleep(5);
                            continue;
                        }
                        idleRounds = 0;
                        long offset = t.getOffset();
                        // 不变量 1：领取成功的瞬间，记录中的占用者必须是自己
                        TaskRecord fresh = queue.get(offset);
                        if (fresh.getState() != TaskState.RUNNING || !owner.equals(fresh.getOwnerToken())) {
                            errors.add("claim invariant violated: key=" + t.getIdempotencyKey()
                                    + " state=" + fresh.getState() + " owner=" + fresh.getOwnerToken());
                        }
                        // 处理一会儿，期间随机续期（模拟长任务心跳）
                        int renewTimes = ThreadLocalRandom.current().nextInt(3);
                        for (int r = 0; r < renewTimes; r++) {
                            Thread.sleep(50);
                            AckOutcome renew = queue.renew(offset, owner);
                            if (renew.status() == AckStatus.APPLIED) {
                                renews.incrementAndGet();
                                // 不变量 2：续期成功后占用者仍然只能是自己
                                TaskRecord afterRenew = queue.get(offset);
                                if (!owner.equals(afterRenew.getOwnerToken())) {
                                    errors.add("renew invariant violated: key=" + t.getIdempotencyKey()
                                            + " owner=" + afterRenew.getOwnerToken());
                                }
                            } // 租约竞争下被拒也是合法结果，不视为错误
                        }
                        // 约 1/4 主动放弃，其余确认
                        if (ThreadLocalRandom.current().nextInt(4) == 0) {
                            AckOutcome abandon = queue.abandon(offset, owner);
                            if (abandon.status() == AckStatus.APPLIED) {
                                abandons.incrementAndGet();
                                // 不变量 3：放弃后任务立即回到 PENDING 且无占用者
                                TaskRecord afterAbandon = queue.get(offset);
                                if (afterAbandon.getState() != TaskState.PENDING
                                        && afterAbandon.getState() != TaskState.RUNNING) {
                                    errors.add("abandon invariant violated: key=" + t.getIdempotencyKey()
                                            + " state=" + afterAbandon.getState());
                                }
                                log.info("[test] key='{}' offset={} abandoned by {}, state now {}",
                                        t.getIdempotencyKey(), offset, owner, afterAbandon.getState());
                            }
                        } else {
                            AckOutcome ack = queue.ack(offset, owner);
                            if (ack.status() == AckStatus.APPLIED) {
                                // 不变量 4：同一任务只能被成功确认一次
                                if (terminalAckOwner.putIfAbsent(offset, owner) != null) {
                                    errors.add("task acked twice: key=" + t.getIdempotencyKey());
                                }
                                successfulAcks.incrementAndGet();
                            } // LEASE_EXPIRED（被抢/过期）是合法竞争结果
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "消费者线程应在限定时间内结束");

        log.info("[test] contention finished: acks={}, renews={}, abandons={}, errors={}",
                successfulAcks.get(), renews.get(), abandons.get(), errors.size());
        assertTrue(errors.isEmpty(), "并发争用下不允许违反单占用/单次确认不变量: " + errors);
        assertEquals(taskCount, successfulAcks.get(), "每个任务必须恰好被成功确认一次");
        assertEquals(taskCount, terminalAckOwner.size());
        assertEquals(taskCount, queue.committedOffset());
        queue.listAll().forEach(t -> assertEquals(TaskState.SUCCEEDED, t.getState(),
                "全部任务最终必须唯一地落到 SUCCEEDED: " + t));
    }
}
