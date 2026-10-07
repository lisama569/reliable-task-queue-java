package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 幂等提交的确定结果。提交永远不会既执行又报冲突。
 */
public enum EnqueueStatus {

    /** 新建任务已入队。 */
    ACCEPTED,

    /** 幂等键对应的任务尚未完成，本次提交被去重，没有产生重复任务/执行。 */
    DEDUPLICATED,

    /** 幂等键对应的任务此前已确认成功；返回原任务，不会再次执行。 */
    ALREADY_SUCCEEDED,

    /** 幂等键对应的任务此前已重试耗尽进入死信；返回原任务，不会再次执行。 */
    ALREADY_DEAD,

    /**
     * 队列已达到容量上限（在途任务数），新任务被明确拒绝：返回结果中不含任务，
     * 任务不会被静默丢弃，调用方可据此稍后重试或告警。
     * 幂等键已存在的重复提交不受容量限制，仍返回原有的幂等结果。
     */
    REJECTED_CAPACITY
}
