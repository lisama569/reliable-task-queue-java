package com.github.lisama569.reliabletaskqueuejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 队列配置项，前缀 taskqueue。
 */
@ConfigurationProperties(prefix = "taskqueue")
public class QueueProperties {

    /** 快照文件路径，重启后从该文件恢复未完成与死信状态。 */
    private String storePath = "data/taskqueue-snapshot.json";

    /** 最大投递次数（含首次）；超过后进入死信。 */
    private int maxAttempts = 5;

    /** 租约时长：超过后未确认的任务可被重新投递（至少一次）。 */
    private long leaseMillis = 30_000L;

    /** 退避基数。 */
    private long retryBaseMillis = 1_000L;

    /** 退避上限。 */
    private long retryMaxMillis = 60_000L;

    /** 队列容量上限（在途任务数，PENDING+RUNNING）；<=0 表示不限容量。 */
    private int maxCapacity = 0;

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getLeaseMillis() {
        return leaseMillis;
    }

    public void setLeaseMillis(long leaseMillis) {
        this.leaseMillis = leaseMillis;
    }

    public long getRetryBaseMillis() {
        return retryBaseMillis;
    }

    public void setRetryBaseMillis(long retryBaseMillis) {
        this.retryBaseMillis = retryBaseMillis;
    }

    public long getRetryMaxMillis() {
        return retryMaxMillis;
    }

    public void setRetryMaxMillis(long retryMaxMillis) {
        this.retryMaxMillis = retryMaxMillis;
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    public void setMaxCapacity(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }
}
