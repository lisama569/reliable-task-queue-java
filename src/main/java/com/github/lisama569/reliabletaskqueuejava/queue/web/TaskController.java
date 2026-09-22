package com.github.lisama569.reliabletaskqueuejava.queue.web;

import com.github.lisama569.reliabletaskqueuejava.queue.QueueException;
import com.github.lisama569.reliabletaskqueuejava.queue.SubmitOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.SubmitResult;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 可靠任务队列的本地 HTTP 接口，便于手动验证投递语义。
 *
 * <ul>
 *   <li>POST /api/tasks                幂等入队</li>
 *   <li>POST /api/tasks/claim          领取（抢占）任务</li>
 *   <li>POST /api/tasks/{id}/ack       确认完成</li>
 *   <li>POST /api/tasks/{id}/nack      报告失败（触发退避重试或死信）</li>
 *   <li>POST /api/offset/commit        显式推进位点（回退被拒绝）</li>
 *   <li>POST /api/offset/advance       自动推进连续已确认位点</li>
 *   <li>GET  /api/offset               查询当前位点</li>
 *   <li>GET  /api/tasks / /api/tasks/{id} 查询任务状态</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class TaskController {

    private final TaskQueue taskQueue;

    public TaskController(TaskQueue taskQueue) {
        this.taskQueue = taskQueue;
    }

    @PostMapping("/tasks")
    public ResponseEntity<SubmitResponse> submit(@RequestBody SubmitRequest req) {
        if (req == null || req.idempotencyKey() == null || req.idempotencyKey().isBlank()) {
            throw new IllegalArgumentException("idempotencyKey 必填");
        }
        SubmitResult result = taskQueue.submit(req.idempotencyKey(), req.payload());
        HttpStatus http = result.outcome() == SubmitOutcome.ACCEPTED
                ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(http)
                .body(new SubmitResponse(result.outcome().name(),
                        explain(result.outcome()), TaskView.of(result.task())));
    }

    @PostMapping("/tasks/claim")
    public ClaimResponse claim(@RequestParam String consumerId) {
        TaskRecord t = taskQueue.claim(consumerId);
        return new ClaimResponse(consumerId, t == null ? null : TaskView.of(t));
    }

    @PostMapping("/tasks/{id}/ack")
    public TaskView ack(@PathVariable long id, @RequestBody AckRequest req) {
        requireAckFields(req);
        return TaskView.of(taskQueue.ack(id, req.deliveryTag(), req.consumerId()));
    }

    @PostMapping("/tasks/{id}/nack")
    public TaskView nack(@PathVariable long id, @RequestBody AckRequest req) {
        requireAckFields(req);
        String error = req.error() == null || req.error().isBlank() ? "未提供失败原因" : req.error();
        return TaskView.of(taskQueue.nack(id, req.deliveryTag(), req.consumerId(), error));
    }

    /** 显式处置死信：记录审计决策后任务视为完成，解除对位点的阻塞。 */
    @PostMapping("/tasks/{id}/resolve")
    public TaskView resolve(@PathVariable long id, @RequestBody ResolveRequest req) {
        if (req == null || req.operator() == null || req.resolution() == null
                || req.resolution().isBlank()) {
            throw new IllegalArgumentException("operator 与 resolution 必填");
        }
        return TaskView.of(taskQueue.resolveDead(id, req.operator(), req.resolution()));
    }

    @PostMapping("/offset/commit")
    public OffsetResponse commit(@RequestBody CommitOffsetRequest req) {
        if (req == null || req.offset() == null || req.consumerId() == null) {
            throw new IllegalArgumentException("consumerId 与 offset 必填");
        }
        return new OffsetResponse(taskQueue.commitOffset(req.offset(), req.consumerId()));
    }

    @PostMapping("/offset/advance")
    public OffsetResponse advance() {
        return new OffsetResponse(taskQueue.autoAdvanceOffset());
    }

    @GetMapping("/offset")
    public OffsetResponse offset() {
        return new OffsetResponse(taskQueue.committedOffset());
    }

    @GetMapping("/tasks")
    public List<TaskView> list() {
        return taskQueue.listTasks().stream().map(TaskView::of).toList();
    }

    @GetMapping("/tasks/{id}")
    public TaskView get(@PathVariable long id) {
        TaskRecord t = taskQueue.getTask(id);
        if (t == null) {
            throw new QueueException.NotFound(id);
        }
        return TaskView.of(t);
    }

    private static void requireAckFields(AckRequest req) {
        if (req == null || req.consumerId() == null || req.deliveryTag() == null) {
            throw new IllegalArgumentException("consumerId 与 deliveryTag 必填");
        }
    }

    private static String explain(SubmitOutcome outcome) {
        return switch (outcome) {
            case ACCEPTED -> "新任务已接受入队";
            case DUPLICATE_IN_FLIGHT -> "同幂等键的任务尚未确认完成，重复提交已去重，不会重复执行";
            case ALREADY_COMPLETED -> "同幂等键的任务此前已确认完成，返回既有结果，不会重复执行";
            case ALREADY_DEAD -> "同幂等键的任务此前已重试耗尽进入死信，不会重复执行";
        };
    }

    // ---- 可区分的错误响应：稳定错误码 + 可读消息 + 恰当 HTTP 状态 ----

    @ExceptionHandler(QueueException.NotFound.class)
    public ResponseEntity<Map<String, String>> notFound(QueueException.NotFound e) {
        return error(HttpStatus.NOT_FOUND, e);
    }

    @ExceptionHandler(QueueException.LeaseMismatch.class)
    public ResponseEntity<Map<String, String>> lease(QueueException.LeaseMismatch e) {
        return error(HttpStatus.CONFLICT, e);
    }

    @ExceptionHandler(QueueException.IllegalState.class)
    public ResponseEntity<Map<String, String>> illegalState(QueueException.IllegalState e) {
        return error(HttpStatus.CONFLICT, e);
    }

    @ExceptionHandler({QueueException.OffsetRewind.class, QueueException.OffsetGapUnconfirmed.class})
    public ResponseEntity<Map<String, String>> offsetRejected(QueueException e) {
        return error(HttpStatus.CONFLICT, e);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest()
                .body(Map.of("code", "BAD_REQUEST", "message", e.getMessage()));
    }

    private static ResponseEntity<Map<String, String>> error(HttpStatus status, QueueException e) {
        return ResponseEntity.status(status).body(Map.of("code", e.getCode(), "message", e.getMessage()));
    }
}
