package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 任务处于 {@link TaskState#PENDING} 的可解释原因，
 * 保证“正常处理 / 主动放弃重投 / 失联回收”等流转对外查到的状态唯一、可解释。
 */
public enum PendingReason {

    /** 新任务入队，等待首次投递。 */
    ENQUEUED,

    /** nack 失败上报后按指数退避等待重试。 */
    RETRY_BACKOFF,

    /** 原消费者主动放弃（abandon），任务立即重新可领取。 */
    ABANDONED,

    /** 进程重启恢复：崩溃前 RUNNING 的租约一律失效，等待重新投递。 */
    LEASE_RECOVERED
}
