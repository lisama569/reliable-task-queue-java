package com.github.lisama569.reliabletaskqueuejava.queue.web;

/**
 * 入队请求。
 *
 * @param idempotencyKey 幂等键（必填），同一键在确认完成前重复提交不会重复执行
 * @param payload        任务载荷
 */
public record SubmitRequest(String idempotencyKey, String payload) {
}
