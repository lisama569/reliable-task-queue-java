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
    ALREADY_DEAD
}
