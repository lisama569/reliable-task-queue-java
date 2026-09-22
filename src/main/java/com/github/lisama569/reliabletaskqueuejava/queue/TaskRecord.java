package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 一个任务的完整状态记录（可变状态字段仅允许在 {@link TaskQueue} 的锁内修改）。
 */
public class TaskRecord {

    private long id;
    private String idempotencyKey;
    private String payload;
    private TaskStatus status;
    private int attempts;
    /** 每次投递（领取/超时重投）都会递增的投递序号，0 表示从未投递。 */
    private long deliveryTag;
    private String leasedBy;
    private long leasedAt;
    private long leaseExpiresAt;
    private long notVisibleUntil;
    private long createdAt;
    private long updatedAt;
    private String lastError;
    private long completedAt;
    /** 死信被显式处置时留下的决策说明（审计用）；非死信处置场景为 null。 */
    private String resolution;

    public TaskRecord() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(long deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    public String getLeasedBy() {
        return leasedBy;
    }

    public void setLeasedBy(String leasedBy) {
        this.leasedBy = leasedBy;
    }

    public long getLeasedAt() {
        return leasedAt;
    }

    public void setLeasedAt(long leasedAt) {
        this.leasedAt = leasedAt;
    }

    public long getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public void setLeaseExpiresAt(long leaseExpiresAt) {
        this.leaseExpiresAt = leaseExpiresAt;
    }

    public long getNotVisibleUntil() {
        return notVisibleUntil;
    }

    public void setNotVisibleUntil(long notVisibleUntil) {
        this.notVisibleUntil = notVisibleUntil;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(long completedAt) {
        this.completedAt = completedAt;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }
}
