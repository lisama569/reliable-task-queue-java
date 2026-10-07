package com.github.lisama569.reliabletaskqueuejava.queue;

import java.util.List;

/**
 * 本地可靠任务队列：以进程内存储模拟消息代理，配合文件快照实现重启恢复。
 *
 * <p>语义：
 * <ul>
 *   <li>至少一次投递：消费失败或租约过期会重新投递，可能重复投递但不会丢失；</li>
 *   <li>单消费者占用：同一任务任一时刻只能被一个持有有效租约的消费者占用；</li>
 *   <li>幂等提交：相同幂等键在完成前去重，完成后返回确定结果，绝不既执行又报冲突；</li>
 *   <li>位点只进不退：只有被成功确认的任务才能推进位点，回退请求被明确拒绝。</li>
 * </ul>
 */
public interface TaskQueue {

    /**
     * 提交任务（幂等）。
     *
     * @param idempotencyKey 幂等键，非空
     * @param payload        任务载荷
     * @return 确定的提交结果（新建、去重、此前已成功、此前已死信）
     */
    EnqueueResult enqueue(String idempotencyKey, String payload);

    /**
     * 非阻塞拉取一个可消费任务，并以租约形式占用它。
     *
     * @param ownerToken 消费者令牌；后续 ack/nack 必须携带同一令牌
     * @return 被占用的任务；没有可消费任务时返回 null
     */
    TaskRecord poll(String ownerToken);

    /**
     * 成功确认。仅持有有效租约可确认；仅成功确认可推进位点。
     */
    AckOutcome ack(long offset, String ownerToken);

    /**
     * 消费失败上报：按可解释的指数退避安排重试；超过重试上限则进入死信并保留原因。
     */
    AckOutcome nack(long offset, String ownerToken, String reason);

    /**
     * 续期（心跳）：长任务处理过程中刷新租约到期时间，避免被当成失联重新投递。
     * 仅当前持有有效租约的占用者可续期；租约已过期、已被他人接手或任务已结束时
     * 续期被拒绝，此后原占用者的 ack/nack 同样会被拒绝。
     */
    AckOutcome renew(long offset, String ownerToken);

    /**
     * 主动放弃：消费者放弃当前占用的任务，任务立即回到 PENDING 并对所有消费者可见，
     * 不消耗额外重试额度、不记录失败原因；放弃后原占用者的 ack/nack 一律被拒绝。
     */
    AckOutcome abandon(long offset, String ownerToken);

    /**
     * 优雅退出：释放该消费者当前占用的全部任务，使其立即可被其他消费者领取。
     *
     * @return 实际释放的任务数
     */
    int releaseAll(String ownerToken);

    /** 位点：首个未被成功确认的任务序号；所有序号小于该值的任务均已确认成功。 */
    long committedOffset();

    /**
     * 将位点推进到指定值。只允许向前移动到“目标位置之前任务全部已成功”的位置；
     * 回退或跨越未完成任务都会被明确拒绝。
     */
    void seek(long targetOffsetInclusive);

    /** 按序号查询任务。 */
    TaskRecord get(long offset);

    /** 列出全部任务（按序号）。 */
    List<TaskRecord> listAll();

    /** 列出死信任务。 */
    List<TaskRecord> listDeadLetters();

    /** 当前最大重试次数（含首次投递）。 */
    int maxAttempts();

    /** 队列容量上限（在途任务数）；{@code <= 0} 表示不限容量。 */
    int maxCapacity();

    /** 当前在途任务数（PENDING + RUNNING），用于背压判断与观测。 */
    int inFlightCount();
}
