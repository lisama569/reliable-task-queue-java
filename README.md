# 本地可靠任务队列（Spring Boot，进程内存储模拟消息代理）

一个不依赖任何外部服务（无真实 MQ / 数据库 / 凭据）的本地可靠任务队列：

- 任务存放在进程内内存结构中，并以 **JSON 文件快照**（临时文件 + 原子 rename）模拟消息代理的持久化；
- 单把公平锁串行化所有状态变更，变更后立即落盘，使任务在**并发消费、重复投递、消费失败、进程重启**下的最终状态确定、可预期、可恢复。

## 1. 投递语义（至少一次 + 单消费者占用）

### 状态机

```
PENDING ──poll(租约占用)──▶ RUNNING ──ack──▶ SUCCEEDED（终态）
   ▲                          │
   │ nack（仍有重试额度）       │ nack（attempts == maxAttempts）
   └──────── 退避后重新可见 ◀───┴──────────▶ DEAD（终态，死信）

RUNNING ──abandon(当前占用者主动放弃)──▶ PENDING(pendingReason=ABANDONED，立即重新可领取)
RUNNING ──renew(当前占用者心跳)──▶ RUNNING（租约到期时间延长，状态不变）
RUNNING 且租约过期 ──任意消费者 poll──▶ 重新投递（attempts+1，pendingReason=LEASE_RECOVERED，仍为至少一次）
进程重启时所有 RUNNING ──恢复阶段──▶ PENDING（pendingReason=LEASE_RECOVERED，旧租约一律失效）
```

- **单消费者占用**：任务只在 `PENDING 且到了可见时间`，或 `RUNNING 且租约已过期` 时可被领取。
  领取时写入 `ownerToken` 与 `leaseExpireAt`；租约期内其他消费者 `poll` 拿不到、用旧令牌 `ack/nack/abandon/renew` 一律被拒绝。
- **PENDING 原因唯一可解释**：同一个 `PENDING` 状态通过 `pendingReason` 区分来源——
  `ENQUEUED`（新入队）、`RETRY_BACKOFF`（nack 后退避重试）、`ABANDONED`（消费者主动放弃）、
  `LEASE_RECOVERED`（租约过期被接手 / 进程重启回收）。任务在“正常处理 / 放弃重投 / 失联回收”之间如何流转，对外查到的状态都是确定、唯一、可解释的。
- **租约续期（心跳，`renew`）**：长任务可在处理过程中持续延长租约，避免处理到一半被判失联重投。
  仅“令牌匹配且租约未过期的当前占用者”能续期成功（`RENEWED`）；
  任务已过期、已被他人接手或已结束（`LEASE_EXPIRED` / `NOT_RUNNING` / `ALREADY_ACKED` / `DEAD`）时续期失败，
  **续期失败后原占用者再 `ack/nack` 也必然失败**——它已经不是有效占用者，无法覆盖接手者的结果。
- **主动放弃（`abandon`）**：消费者优雅退出时可主动交还任务，任务**立即**变为 `PENDING(ABANDONED)` 并可被他人领取，不需要等租约过期。
  放弃只对当前有效占用者生效；放弃后原占用者与该任务的占用关系即告终结，其后续 `ack/nack` 会被拒绝（`NOT_RUNNING`/`LEASE_EXPIRED`）。
  无论正常处理、放弃重投还是失联回收，**任一时刻同一个任务至多只有一个有效占用者**。
- **至少一次投递**：消费失败（`nack`）、主动放弃（`abandon`）或消费者崩溃（租约过期 / 进程重启）都会重新投递，因此重复投递/重复执行是允许的，任务不会丢失；
  消费侧若需“恰好一次效果”，应结合下文的**幂等键**在业务侧去重。
- **租约（lease）**：默认 30s，可用 `taskqueue.lease-millis` 调整。租约过期不是立即抢占，而是在下次扫描到该任务且已过可见/到期时间时重新投递。

## 2. 幂等提交（去重规则）

`enqueue(idempotencyKey, payload)` 的返回结果是**确定且可解释**的，任何情况下都不会“既执行又报冲突”：

| 提交时已有任务的状态 | 返回 `EnqueueStatus` | 行为 |
| --- | --- | --- |
| 不存在 | `ACCEPTED` | 创建新任务（offset、key 不可变） |
| `PENDING` / `RUNNING`（未确认完成） | `DEDUPLICATED` | 返回**原任务实体**，不创建新任务、不产生第二次执行 |
| `SUCCEEDED` | `ALREADY_SUCCEEDED` | 返回原任务，**不会再次投递/执行** |
| `DEAD` | `ALREADY_DEAD` | 返回原任务与既有失败信息，**不会再次投递/执行** |

注意两点设计决定：

1. **完成前的重复提交不会重复执行**：相同 key 始终映射到同一个任务实体。
2. **完成后的重复提交不会“复活”任务**：成功就是成功、死信就是死信，返回既有结果而不是报错冲突，调用方可以直接依据返回状态解释结果。

确认接口同样幂等：对已成功任务再次 `ack` 返回 `ALREADY_ACKED`（无害空操作），不会重复消费。

### 容量上限与背压

可通过 `taskqueue.max-tasks` 设置队列任务数量上限（`<=0` 表示不限制，**默认不限制，即旧行为**）。

- **满员结果确定且可解释**：达到上限后，**新的**幂等键提交返回 `EnqueueStatus.QUEUE_FULL`
  （HTTP **429 Too Many Requests**），且 `task` 为 `null`——任务既没有入队、没有占用容量，**绝不会被静默丢弃**，调用方应退避后重试。
- **容量按“任务条目（不同幂等键）”计数**：相同幂等键的重复提交不新增条目、不额外占用容量。
- **幂等优先于容量**：重复提交永远先命中幂等判定，即使队列已满：
  - 未完成的重复提交仍是 `DEDUPLICATED`；
  - 已成功的重复提交仍是 `ALREADY_SUCCEEDED`；
  - 已死信的重复提交仍是 `ALREADY_DEAD`。
  **容量限制不会改变任何幂等结果**，这三种情况都返回 `200` 而不是 `429`。
- 已完成 / 已死信的任务作为记录仍保留在队列中（位点与审计需要），因此仍计入条目数；腾出容量的语义是“没有重复条目”，而不是“完成即删除”。
- 可通过 `capacity()` / `currentSize()`（HTTP `GET /api/tasks/capacity`）查看上限与当前占用。

## 3. 重试与死信规则

- 最大投递次数（含首次）由 `taskqueue.max-attempts` 控制，默认 **5**。每次 `poll` 成功占用都会使 `attempts += 1`。
- `nack(offset, owner, reason)` 必须持有有效租约，结果：
  - `attempts < maxAttempts`：任务回到 `PENDING`，按**指数退避**设置重新可见时间，记录本次失败原因；
  - `attempts == maxAttempts`：任务进入 `DEAD`（死信），**保留最后一次失败原因**（`failureReason`）、投递次数与完成时间，可通过 `listDeadLetters()` / HTTP 接口查询，**绝不静默丢弃**。
- 退避策略可解释：`delay = min(retryMaxMillis, retryBaseMillis * 2^(attempts-1))`，
  默认基数 1s、上限 60s，例如 1s → 2s → 4s → 8s → 16s … 封顶 60s。
- 不同失败原因会依次记录在任务上；死信时保留最终原因（测试中验证了 `NullPointerException…` / `Timeout…` / `HTTP 500…` 等可区分原因的流转）。

## 4. 位点（offset）与重启恢复- 每个任务有不可变的单调递增 `offset`（0,1,2,…）。
- **只有成功 `ack` 才能推进位点**：位点 = 连续 `SUCCEEDED` 前缀之后的第一个位置。
  - 飞行中（`RUNNING`）的任务**不阻塞后续任务投递**，但会阻塞位点；
  - `DEAD` 死信同样挡住位点——即使其后的任务全部成功，位点也不会越过未确认任务。
- `seek(target)` 只允许前进，且目标之前的每个任务都必须已成功：
  - 目标小于当前位点 → 抛 `OffsetRollbackException`（HTTP 409，消息含 `rollback rejected`）；
  - 跨越未确认任务 → 同样拒绝（消息含 `only confirmed tasks can advance`）。
  - **位点回退被明确拒绝，而不是重复消费已确认任务。**
- **持久化与恢复**：每次状态变更（入队/领取/ack/nack/seek/recover）后都把全量快照原子写入 `taskqueue.store-path`（默认 `data/taskqueue-snapshot.json`）。
  启动时若快照存在：
  - `SUCCEEDED` / `DEAD` / `PENDING` 状态、死信失败原因、已提交位点原样恢复；
  - 所有 `RUNNING` 一律重置为 `PENDING` 并清空 owner/租约——崩溃前的消费者不再被信任，任务将被重新投递（至少一次）。

## 5. 配置项（`src/main/resources/application.properties`）

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `taskqueue.store-path` | `data/taskqueue-snapshot.json` | JSON 快照文件路径 |
| `taskqueue.max-attempts` | `5` | 最大投递次数（含首次），耗尽入死信 |
| `taskqueue.lease-millis` | `30000` | 租约时长，过期后可重新投递 |
| `taskqueue.retry-base-millis` | `1000` | 指数退避基数 |
| `taskqueue.retry-max-millis` | `60000` | 指数退避上限 |
| `taskqueue.max-tasks` | `0`（不限制） | 队列任务数量上限；满员新提交返回 `QUEUE_FULL`（HTTP 429），重复幂等提交不受影响 |

## 6. HTTP 演示接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/tasks` | 幂等提交，body：`{"key":"...","payload":"..."}`；满员返回 **429** + `QUEUE_FULL` |
| `POST /api/tasks/poll?owner=consumer-1` | 拉取并租约占用一个任务；无任务 204 |
| `POST /api/tasks/{offset}/ack?owner=consumer-1` | 成功确认（可能推进位点） |
| `POST /api/tasks/{offset}/nack?owner=consumer-1&reason=...` | 失败上报（重试或入死信） |
| `POST /api/tasks/{offset}/renew?owner=consumer-1` | 租约续期（心跳）；非有效占用者返回 `LEASE_EXPIRED` 等确定结果 |
| `POST /api/tasks/{offset}/abandon?owner=consumer-1&reason=...` | 主动放弃占用；任务立即回到 `PENDING(ABANDONED)` 可被他人领取 |
| `GET  /api/tasks` / `GET /api/tasks/{offset}` | 列出/查询任务（含 `pendingReason`） |
| `GET  /api/tasks/dead-letters` | 列出死信（含失败原因） |
| `GET  /api/tasks/capacity` | 查看容量上限 `capacity` 与当前条目数 `currentSize` |
| `GET  /api/tasks/committed-offset` | 查看已提交位点 |
| `POST /api/tasks/committed-offset/{target}` | 显式推进位点；回退/跨越未确认 → 409 |

## 7. 验证方法

需要 JDK 21+ 与 Maven，无需任何外部服务：

```bash
mvn test
```

测试（共 24 个）覆盖：

| 测试类 | 覆盖点 |
| --- | --- |
| `IdempotencyAndDeliveryTest` | 完成前重复提交去重且只执行一次；成功后重提 `ALREADY_SUCCEEDED`；重复 ack 为 `ALREADY_ACKED`；租约过期重新投递、旧 owner 确认被拒 `LEASE_EXPIRED` |
| `ConcurrentPollingTest` | 8 个消费者并发争抢 50 个任务：每个任务同一时刻只有一个占用者、恰好被成功确认一次，位点推进到末尾 |
| `RetryAndDeadLetterTest` | 退避序列 1s/2s/4s…封顶；退避窗口内不可见；重试耗尽入死信并保留可区分原因；死信不再投递且挡住位点；死信后重提 `ALREADY_DEAD` |
| `OffsetAndRecoveryTest` | 位点回退/跨越未确认被明确拒绝；重启恢复 SUCCEEDED/DEAD/PENDING/位点/死信原因；重启使 RUNNING 租约失效并按序重新投递 |
| `LeaseRenewalTest` | 长任务持续心跳不被判失联、心跳期间他人不能接手；过期/被接手后续期失败且原占用者 ack/nack 一并被拒；令牌不匹配与终态的确定结果 |
| `AbandonAndReclaimTest` | 主动放弃后立即可被他人领取（`pendingReason=ABANDONED`）、原占用者确认被拒；非占用者/过期放弃被拒；**8 个消费者真实同时争用 40 个被放弃任务，任一时刻只有一个占用者、恰好确认一次** |
| `CapacityBackpressureTest` | 满员新提交 `QUEUE_FULL` 且不落库不占容量；重复提交（未完成/已成功/已死信）在满员下仍保持原幂等结果且不占容量；默认不限制保持旧行为 |
| `TaskQueueWebIntegrationTest` | HTTP 端到端：幂等提交、租约拉取、确认、续期、放弃重投、容量查询、非法 seek 返回 409 |
| `TaskQueueWebBackpressureIntegrationTest` | HTTP 端到端：满员提交返回 429 `QUEUE_FULL`；满员下相同 key 仍返回 200 `DEDUPLICATED` |

测试中使用**可控时钟**（`TestClock`）推进时间，因此退避与租约过期是确定性的，不需要真实等待。

单测日志会打印**任务键与完整状态流转**（`PENDING → RUNNING → SUCCEEDED/PENDING/DEAD`、attempts、owner、退避时长、死信原因、recover 过程），
Surefire 将输出重定向到：

```
target/surefire-reports/<测试类名>-output.txt
```

也可以只跑某一组：

```bash
mvn test -Dtest=RetryAndDeadLetterTest -Dsurefire.useFile=false   # 日志直接打印到控制台
```

手动端到端体验（启动后另开终端）：

```bash
mvn spring-boot:run
curl -s -XPOST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"key":"order-1","payload":"hello"}'
curl -s -XPOST 'localhost:8080/api/tasks/poll?owner=c1'
curl -s -XPOST 'localhost:8080/api/tasks/0/ack?owner=c1'
curl -s localhost:8080/api/tasks
# 重启进程后再次 curl localhost:8080/api/tasks，状态与位点仍然一致
```

续期 / 主动放弃 / 容量背压的手动体验（租约可先用 `-Dtaskqueue.lease-millis=...` 调小，容量用 `--taskqueue-max-tasks=2`）：

```bash
# 长任务处理期间持续续期（默认每次延长一个租约周期）
curl -s -XPOST 'localhost:8080/api/tasks/0/renew?owner=c1'
# 优雅退出：主动放弃，任务立即对其他消费者可见（pendingReason=ABANDONED）
curl -s -XPOST 'localhost:8080/api/tasks/0/abandon?owner=c1&reason=shutdown'
# 查看容量与占用
curl -s localhost:8080/api/tasks/capacity
# 容量打满后新 key 返回 429 与 {"status":"QUEUE_FULL",...}，重复 key 仍是 200 的幂等结果
curl -i -XPOST localhost:8080/api/tasks -H 'Content-Type: application/json' -d '{"key":"new","payload":"x"}'
```

## 8. 新增运维能力对旧行为的兼容范围

本次仅在既有状态机上**新增**能力，不改变原有提交、确认、重试与恢复语义：

- **默认无容量限制**（`taskqueue.max-tasks=0`）：不配置时入队、去重、`ALREADY_*` 行为与旧版完全一致；只有显式配置正数上限后，才会出现新的 `QUEUE_FULL` 结果（HTTP 429）。
- `enqueue/poll/ack/nack/seek/recover` 的方法签名与返回含义保持不变；新增的 `renew/abandon/capacity/currentSize` 是独立方法，旧调用方无需任何改动。
- 新增的 `pendingReason` 是 `TaskRecord` 上的可空字段：旧快照中没有该字段时反序列化为 `null`，恢复逻辑会为重启回收的 RUNNING 任务补上 `LEASE_RECOVERED`，不影响历史快照读取。
- 续期与放弃都复用与 `ack/nack` 完全相同的“当前有效占用者”判定，因此不会放宽任何占用约束，单占用者语义只强不弱。
- 放弃 / 失联回收后的重新投递与既有“至少一次”一致：`attempts` 在重新 `poll` 时照常 +1，退避与死信规则不变。

## 9. 代码结构

```
src/main/java/com/github/lisama569/reliabletaskqueuejava/
├── queue/
│   ├── TaskQueue.java                 # 队列接口（语义契约）
│   ├── InMemoryReliableTaskQueue.java # 核心实现：状态机/租约/退避/位点/恢复
│   ├── TaskRecord / QueueSnapshot     # 实体与持久化快照（TaskRecord 含 pendingReason）
│   ├── TaskState / EnqueueStatus / AckStatus  # 确定的状态与结果枚举
│   ├── PendingReason                  # PENDING 的可解释原因（ENQUEUED/RETRY_BACKOFF/ABANDONED/LEASE_RECOVERED）
│   ├── RenewStatus / RenewOutcome     # 租约续期（心跳）的确定结果
│   ├── RetryPolicy.java               # 指数退避（可解释）
│   ├── TaskQueueStore.java            # 存储接口
│   ├── JsonFileTaskQueueStore.java    # JSON 原子快照实现
│   └── OffsetRollbackException.java   # 位点回退拒绝
├── config/TaskQueueAutoConfiguration.java + QueueProperties.java
└── web/TaskQueueController.java       # HTTP 演示接口
```
