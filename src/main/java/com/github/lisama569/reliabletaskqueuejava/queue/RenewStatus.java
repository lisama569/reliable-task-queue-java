package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 租约续期（heartbeat）操作的确定结果。
 */
public enum RenewStatus {

    /** 续期生效，租约到期时间已延长。 */
    RENEWED,

    /** 调用者不再是有效占用者（租约已过期并被他人接手，或令牌不匹配），续期被拒绝。 */
    LEASE_EXPIRED,

    /** 任务当前未被占用（PENDING），无法续期。 */
    NOT_RUNNING,

    /** 任务此前已确认成功，无需续期。 */
    ALREADY_ACKED,

    /** 任务已进入死信，无法续期。 */
    DEAD
}
