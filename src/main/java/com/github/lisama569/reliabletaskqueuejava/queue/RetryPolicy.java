package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 可解释的重试退避策略：delayMillis = baseDelayMillis * 2^(attempt-1)，上限 maxDelayMillis。
 * attempt 为本次失败之前已经发生过的投递次数（1 表示首次投递失败）。
 */
public class RetryPolicy {

    private final long baseDelayMillis;
    private final long maxDelayMillis;

    public RetryPolicy(long baseDelayMillis, long maxDelayMillis) {
        this.baseDelayMillis = baseDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
    }

    /** 计算第 {@code attemptsUsed} 次投递失败后的退避时长。 */
    public long backoffMillis(int attemptsUsed) {
        long delay = baseDelayMillis;
        for (int i = 1; i < attemptsUsed; i++) {
            delay = Math.min(maxDelayMillis, delay * 2);
            if (delay >= maxDelayMillis) {
                break;
            }
        }
        return Math.min(maxDelayMillis, delay);
    }

    public long getBaseDelayMillis() {
        return baseDelayMillis;
    }

    public long getMaxDelayMillis() {
        return maxDelayMillis;
    }
}
