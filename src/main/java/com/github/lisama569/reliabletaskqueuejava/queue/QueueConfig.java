package com.github.lisama569.reliabletaskqueuejava.queue;

import java.time.Duration;

/**
 * 队列配置：租约时长、退避与重试上限。
 *
 * @param leaseDuration      消费租约时长；租约到期未确认则任务可被重新投递（至少一次）
 * @param maxAttempts        最大尝试次数；{@code nack} 后 attempts 达到该值则进入死信
 * @param backoffBase        指数退避基准时长（第 n 次失败后等待 base * 2^(n-1)，封顶 maxBackoff）
 * @param maxBackoff         退避上限
 * @param snapshotFile       快照持久化文件路径；null 表示纯内存（不跨重启）
 */
public record QueueConfig(
        Duration leaseDuration,
        int maxAttempts,
        Duration backoffBase,
        Duration maxBackoff,
        String snapshotFile
) {

    public static QueueConfig defaults() {
        return new QueueConfig(Duration.ofSeconds(30), 3,
                Duration.ofMillis(200), Duration.ofSeconds(10), null);
    }

    /** 计算第 attempt 次失败（attempt 从 1 开始）后任务再次可见前的等待毫秒数。 */
    public long backoffMillis(int attempt) {
        long base = backoffBase.toMillis();
        long delay = base * (1L << Math.min(attempt - 1, 20));
        return Math.min(delay, maxBackoff.toMillis());
    }
}
