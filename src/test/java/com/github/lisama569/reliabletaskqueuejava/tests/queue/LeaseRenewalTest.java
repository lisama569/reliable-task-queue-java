package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.RenewOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.RenewStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 覆盖长任务租约续期（心跳）：
 * 续期延长占用、过期后不能续期且原占用者不能再确认、结束态的确定结果。
 */
class LeaseRenewalTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseRenewalTest.class);

    @TempDir
    Path tempDir;

    @Test
    void heartbeatKeepsLongRunningTaskLeased_andOtherConsumersCannotClaim() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "long-job-1";
        queue.enqueue(key, "heavy");
        TaskRecord t = queue.poll("worker-long");
        long firstExpire = t.getLeaseExpireAtMillis();
        assertEquals(1_000_000L + 30_000L, firstExpire);

        // 处理中持续心跳：每次只前进不到一个租约周期，任务始终不应该被判失联
        for (int i = 1; i <= 5; i++) {
            clock.advance(20_000L);
            RenewOutcome renew = queue.renew(t.getOffset(), "worker-long");
            log.info("[test] key='{}' heartbeat #{} -> {} newLeaseExpireAt={}",
                    key, i, renew.status(), renew.newLeaseExpireAtMillis());
            assertEquals(RenewStatus.RENEWED, renew.status());
            assertEquals(clock.millis() + 30_000L, renew.newLeaseExpireAtMillis());

            TaskRecord other = queue.poll("worker-other");
            assertNull(other, "心跳有效期间其他消费者不能接手 key=" + key);
        }

        // 最终正常确认成功，位点推进
        AckOutcome ack = queue.ack(t.getOffset(), "worker-long");
        log.info("[test] key='{}' ack after heartbeats -> {}", key, ack.status());
        assertEquals(AckStatus.APPLIED, ack.status());
        assertEquals(TaskState.SUCCEEDED, t.getState());
        assertEquals(1L, queue.committedOffset());
    }

    @Test
    void renewAfterLeaseExpiry_isRejected_andStaleOwnerCanNoLongerAckOrNack() {
        TestClock clock = new TestClock(2_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "long-job-2";
        queue.enqueue(key, "heavy-stalled");
        TaskRecord first = queue.poll("worker-stalled");
        assertEquals(1, first.getAttempts());

        // 租约过期且已被别的消费者接手
        clock.advance(30_001L);
        TaskRecord takenOver = queue.poll("worker-recovery");
        assertNotNull(takenOver);
        assertEquals("worker-recovery", takenOver.getOwnerToken());
        assertEquals(2, takenOver.getAttempts());
        log.info("[test] key='{}' taken over by '{}' after lease expiry", key, takenOver.getOwnerToken());

        // 原占用者的心跳必须失败
        RenewOutcome staleRenew = queue.renew(first.getOffset(), "worker-stalled");
        log.info("[test] key='{}' stale renew -> {}", key, staleRenew.status());
        assertEquals(RenewStatus.LEASE_EXPIRED, staleRenew.status());

        // 续期失败后，原占用者既不能 ack 也不能 nack——它已不是有效占用者
        AckOutcome staleAck = queue.ack(first.getOffset(), "worker-stalled");
        AckOutcome staleNack = queue.nack(first.getOffset(), "worker-stalled", "late result");
        log.info("[test] key='{}' stale ack -> {}, stale nack -> {}",
                key, staleAck.status(), staleNack.status());
        assertEquals(AckStatus.LEASE_EXPIRED, staleAck.status());
        assertEquals(AckStatus.LEASE_EXPIRED, staleNack.status());
        assertEquals(TaskState.RUNNING, first.getState(), "新占用者的工作不能被旧占用者覆盖");
        assertEquals("worker-recovery", first.getOwnerToken());

        // 新占用者可正常续期并确认
        assertEquals(RenewStatus.RENEWED, queue.renew(first.getOffset(), "worker-recovery").status());
        assertEquals(AckStatus.APPLIED, queue.ack(first.getOffset(), "worker-recovery").status());
    }

    @Test
    void renewByWrongOwnerOrAtTerminalStates_returnsDeterministicStatus() {
        TestClock clock = new TestClock(3_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "long-job-3";
        queue.enqueue(key, "heavy");
        TaskRecord t = queue.poll("owner-a");

        // 令牌不匹配（租约并未过期、也没被接手）：同样视为不是有效占用者
        RenewOutcome wrong = queue.renew(t.getOffset(), "owner-b");
        log.info("[test] key='{}' renew by wrong owner while lease valid -> {}", key, wrong.status());
        assertEquals(RenewStatus.LEASE_EXPIRED, wrong.status());

        // 结束态：成功后再续期是无害的确定结果
        assertEquals(AckStatus.APPLIED, queue.ack(t.getOffset(), "owner-a").status());
        RenewOutcome afterSuccess = queue.renew(t.getOffset(), "owner-a");
        log.info("[test] key='{}' renew after success -> {}", key, afterSuccess.status());
        assertEquals(RenewStatus.ALREADY_ACKED, afterSuccess.status());
    }

    @Test
    void renewOnPendingTask_returnsNotRunning() {
        TestClock clock = new TestClock(4_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "long-job-4";
        queue.enqueue(key, "heavy");
        // 尚未被任何消费者占用
        RenewOutcome notRunning = queue.renew(0, "nobody");
        log.info("[test] key='{}' renew while PENDING -> {}", key, notRunning.status());
        assertEquals(RenewStatus.NOT_RUNNING, notRunning.status());
    }
}
