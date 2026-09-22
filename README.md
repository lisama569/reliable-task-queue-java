# 本地可靠任务队列（Reliable Task Queue）

用 **进程内存储 + 本地 JSON 快照** 模拟消息代理的可靠任务队列（Spring Boot）。
不依赖外部服务、真实凭据或不可访问资源；构建与验证全部在本地完成：

```bash
mvn test          # 离线环境可加 -o
mvn spring-boot:run
```

## 1. 投递语义：至少一次（At-least-once），但确认后不重复

消费者通过 `claim` 领取任务时获得一个**带过期时间的租约（lease）**和一个单调递增的
`deliveryTag`：

- 租约有效期内，其他消费者**无法**领取同一任务——同一任务任一时刻最多被一个消费者占用；
- 消费者必须在租约内用领取时拿到的 `deliveryTag` 调用 `ack`/`nack`。租约到期仍未确认
  （例如消费者崩溃），任务会被**重新投递**给其他消费者，`deliveryTag` 加一；
- 旧持有者迟到的 `ack`/`nack` 会因持有者或 `deliveryTag` 不匹配被**明确拒绝**
  （错误码 `LEASE_MISMATCH`），不会影响新占用者；
- 任务一旦 `ack` 进入 `COMPLETED`，**永远不会再被投递**。

因此重复投递在“崩溃/超时”时**可能发生**（至少一次），消费者侧应尽量让处理幂等；
但已确认的结果不会重复，最终状态确定。

任务状态机：

```
            submit
PENDING ───────────────► (入队)
   │ ▲                   
   │ │ claim             claim（租约过期重投）
   │ ▼                   
RUNNING ──ack──────────► COMPLETED        （终态，不再投递）
   │
   │ nack（attempts < max）
   └──► PENDING（指数退避后重新可见，attempts 保留累加）
   │
   └──nack（attempts == maxAttempts）──► DEAD（死信，终态，保留失败原因）

DEAD ──显式处置 resolveDead（必须填写处置决策）──► COMPLETED
```

## 2. 幂等提交与去重规则

提交必须带 `idempotencyKey`。同一键的重复提交**永远不会产生第二个任务，也不会
“既执行又报冲突”**，而是返回确定且可解释的结果（HTTP 200，不抛冲突错误）：

| 提交时既有任务状态 | 返回 outcome | 含义 |
|---|---|---|
| 不存在 | `ACCEPTED`（HTTP 201） | 新任务入队 |
| `PENDING` / `RUNNING` | `DUPLICATE_IN_FLIGHT` | 尚未确认完成，重复提交被去重，不重复执行 |
| `COMPLETED` | `ALREADY_COMPLETED` | 此前已确认完成，返回既有任务，不重复执行 |
| `DEAD` | `ALREADY_DEAD` | 此前已重试耗尽进入死信，不重新执行 |

## 3. 重试与退避规则

- 每次 `claim` 都会使 `attempts + 1`（包括租约过期后的重复投递）；
- `nack` 后若 `attempts < maxAttempts`：任务回到 `PENDING`，按
  **指数退避** `backoffBase * 2^(attempt-1)`（封顶 `maxBackoff`）延迟后重新可见；
- `attempts == maxAttempts`：任务进入 `DEAD` 死信，不再投递，且：
  - 保留**最后一次失败原因**（`lastError`）与尝试次数，失败原因可区分、可追溯，**绝不静默丢弃**；
  - 死信会阻塞其之后连续任务的位点推进（见第 4 节），从而强制问题被显式处理；
  - 必须由人工/补偿流程调用 `resolveDead(taskId, operator, resolution)` 显式处置，
    处置决策必填且与原始失败原因一并保留，处置后任务视为 `COMPLETED`。

默认参数（`application.properties`，可按测试需要覆盖）：

```properties
taskqueue.lease-seconds=30        # 租约时长
taskqueue.max-attempts=3          # 最大尝试次数
taskqueue.backoff-base-millis=200 # 退避基准
taskqueue.max-backoff-millis=10000# 退避上限
taskqueue.snapshot-file=target/taskqueue-snapshot.json
```

## 4. 确认（ack）、位点（offset）与重启恢复

- **只有被确认（`COMPLETED`）的任务才能推进位点。** 位点表示“连续已确认”的高水位：
  目标位点之前只要还有未 `COMPLETED` 的任务（PENDING/RUNNING/DEAD），
  推进就被拒绝（`OFFSET_GAP_UNCONFIRMED`，并指出第一个未确认任务的 id）；
- **位点只允许单调前进。** 任何回退请求都被明确拒绝
  （`OFFSET_GAP_UNCONFIRMED` 同层的 `OFFSET_REWIND_REJECTED`），
  因此按位点重放不会重复消费已确认任务。提交到当前位点是幂等空操作；
- `ack` 只接受当前租约持有者 + 匹配的 `deliveryTag`；
- **跨进程重启恢复**：每次状态变更后通过“临时文件 + 原子 rename”同步落盘 JSON 快照。
  重启时恢复 `nextId`、已提交位点和全部任务：
  - `COMPLETED` / `DEAD`（含失败原因）/ 已提交位点原样恢复；
  - 上个进程中处于 `RUNNING` 的任务，其租约随进程死亡必然失效，
    为保证至少一次会被重置为 `PENDING` 重新投递，`attempts` 保留并继续计入重试上限。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/tasks` | 幂等入队 `{"idempotencyKey","payload"}` |
| POST | `/api/tasks/claim?consumerId=c1` | 抢占式领取（租约 + deliveryTag），无任务时 `task=null` |
| POST | `/api/tasks/{id}/ack` | 确认完成 `{"consumerId","deliveryTag"}` |
| POST | `/api/tasks/{id}/nack` | 报告失败 `{"consumerId","deliveryTag","error"}` |
| POST | `/api/tasks/{id}/resolve` | 显式处置死信 `{"operator","resolution"}` |
| POST | `/api/offset/commit` | 显式推进位点 `{"consumerId","offset"}` |
| POST | `/api/offset/advance` | 在连续已确认范围内自动推进位点 |
| GET  | `/api/offset` | 查询当前位点 |
| GET  | `/api/tasks`、`/api/tasks/{id}` | 查询任务状态（含 status/attempts/lastError/resolution） |

错误响应统一为 `{"code": "...", "message": "..."}`，稳定错误码包括：
`TASK_NOT_FOUND`(404)、`LEASE_MISMATCH`(409)、`ILLEGAL_TASK_STATE`(409)、
`OFFSET_REWIND_REJECTED`(409)、`OFFSET_GAP_UNCONFIRMED`(409)、`BAD_REQUEST`(400)。

### curl 速览

```bash
curl -s -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-1","payload":"ship-order-1"}'
curl -s -X POST 'localhost:8080/api/tasks/claim?consumerId=c1'
# 用上一步返回的 id 与 deliveryTag：
curl -s -X POST localhost:8080/api/tasks/1/ack -H 'Content-Type: application/json' \
  -d '{"consumerId":"c1","deliveryTag":1}'
curl -s -X POST localhost:8080/api/offset/advance
```

## 6. 验证方法（测试覆盖的场景）

```bash
mvn test
```

测试使用可注入的虚拟时钟与 `@TempDir` 快照文件，结论确定、无外部依赖；
所有用例都通过 SLF4J 打印**任务键与状态流转**（`[task] key=... 状态流转: A -> B`）。

| 测试类 | 覆盖场景 |
|---|---|
| `IdempotentSubmitTest` | 完成前重复提交去重（PENDING 与 RUNNING 两种窗口）；完成后再提交得到 `ALREADY_COMPLETED` 且不重复执行 |
| `ConcurrentClaimTest` | 8 个消费者并发抢占 50 个任务，每个任务恰好被一个消费者占用、全部完成且不重不丢；租约过期重投、旧持有者迟到 ack 被 `LEASE_MISMATCH` 拒绝 |
| `RetryDeadLetterTest` | 指数退避（100ms→200ms）、退避窗口不可见、重试耗尽进死信并保留可区分原因、死信不重投、同键再提交 `ALREADY_DEAD`、重试后成功不进死信 |
| `OffsetAndRecoveryTest` | 未确认不能推进位点、回退被 `OFFSET_REWIND_REJECTED` 拒绝、死信显式处置前阻塞位点且处置后保留原因；模拟进程重启恢复 COMPLETED/DEAD/位点，并把崩溃在途的 RUNNING 任务重投 |
| `QueueApiIntegrationTest` | 通过 HTTP 跑通完整生命周期，并验证各 409 错误码确定可区分 |
