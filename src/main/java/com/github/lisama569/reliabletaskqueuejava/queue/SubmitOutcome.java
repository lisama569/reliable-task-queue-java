package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 幂等提交的确定结果。重复提交永远不会“既执行又报冲突”：
 * 要么产生一个新任务（{@link #ACCEPTED}），要么返回已有任务的确定状态。
 */
public enum SubmitOutcome {
    /** 新任务被接受入队。 */
    ACCEPTED,
    /** 同幂等键的任务尚未确认完成（PENDING/RUNNING），本次提交被去重忽略。 */
    DUPLICATE_IN_FLIGHT,
    /** 同幂等键的任务此前已确认完成，不会重复执行。 */
    ALREADY_COMPLETED,
    /** 同幂等键的任务此前已进入死信，不会重复执行。 */
    ALREADY_DEAD
}
