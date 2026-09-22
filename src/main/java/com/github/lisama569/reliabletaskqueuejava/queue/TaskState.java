package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 任务在其生命周期中的状态。
 */
public enum TaskState {

    /** 已入队，等待投递。 */
    PENDING,

    /** 已投递给某个消费者并被占用（租约期内）。 */
    RUNNING,

    /** 消费者已显式确认，任务最终成功；仅成功确认可推进位点。 */
    SUCCEEDED,

    /** 重试次数耗尽，进入死信队列，保留失败原因，不会被静默丢弃。 */
    DEAD
}
