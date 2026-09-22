package com.github.lisama569.reliabletaskqueuejava.queue.web;

/**
 * 确认/失败请求。deliveryTag 必须与领取时返回的一致。
 */
public record AckRequest(String consumerId, Long deliveryTag, String error) {
}
