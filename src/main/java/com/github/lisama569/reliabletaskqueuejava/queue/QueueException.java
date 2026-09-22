package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 队列操作的基类异常，附带稳定的机器可读错误码，便于区分失败原因。
 */
public class QueueException extends RuntimeException {

    private final String code;

    public QueueException(String code, String message) {
        super(message);
        this.code = code;
    }

    public QueueException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    /** 任务不存在。 */
    public static class NotFound extends QueueException {
        public NotFound(long taskId) {
            super("TASK_NOT_FOUND", "任务不存在: id=" + taskId);
        }
    }

    /** 确认（ack/nack）方不是当前租约持有者，或租约已过期被他人重新领取。 */
    public static class LeaseMismatch extends QueueException {
        public LeaseMismatch(long taskId, String consumerId) {
            super("LEASE_MISMATCH",
                    "租约不匹配，确认被拒绝: id=" + taskId + ", consumer=" + consumerId
                            + "（任务未被该消费者占用，或其租约已过期被重新投递）");
        }
    }

    /** 任务当前状态不允许该操作（例如对已完成/死信任务 ack）。 */
    public static class IllegalState extends QueueException {
        public IllegalState(long taskId, TaskStatus status, String action) {
            super("ILLEGAL_TASK_STATE",
                    "任务状态不允许操作: id=" + taskId + ", status=" + status + ", action=" + action);
        }
    }

    /** 位点回退被明确拒绝；位点只能单调向前，防止重复消费已确认任务。 */
    public static class OffsetRewind extends QueueException {
        public OffsetRewind(long requested, long current) {
            super("OFFSET_REWIND_REJECTED",
                    "位点回退被拒绝: requested=" + requested + ", current=" + current
                            + "（位点只允许单调前进，已确认任务不会被重复消费）");
        }
    }

    /** 推进位点时，目标位点之前仍存在未确认完成的任务。 */
    public static class OffsetGapUnconfirmed extends QueueException {
        public OffsetGapUnconfirmed(long requested, long firstUnconfirmed) {
            super("OFFSET_GAP_UNCONFIRMED",
                    "无法推进位点到 " + requested + "：id=" + firstUnconfirmed
                            + " 尚未确认完成（位点只能在连续任务全部确认后推进）");
        }
    }
}
