package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 租约续期结果。
 *
 * @param status                 确定结果
 * @param newLeaseExpireAtMillis 续期生效后的新租约到期时间；未生效时为 0
 */
public record RenewOutcome(RenewStatus status, long newLeaseExpireAtMillis) {
}
