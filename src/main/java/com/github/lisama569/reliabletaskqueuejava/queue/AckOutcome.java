package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 确认操作结果。
 *
 * @param status   确定结果
 * @param advanced 该确认是否实际推进了位点（仅首次确认连续成功任务时为 true）
 */
public record AckOutcome(AckStatus status, boolean advanced) {
}
