package com.github.lisama569.reliabletaskqueuejava.queue.web;

/**
 * 死信显式处置请求。
 *
 * @param operator   处置人/处置方
 * @param resolution 处置决策说明（必填，审计用）
 */
public record ResolveRequest(String operator, String resolution) {
}
