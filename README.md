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
   ▲                          │
   │ abandon / releaseAll     │ renew（持有有效租约：刷新 leaseExpireAt，状态不变）
   └────── 立即可见 ◀──────────┘

RUNNING 且租约过期 ──任意消费者 poll──▶ 重新投递（attempts+1，仍为至少一次）
进程重启时所有 RUNNING ──恢复阶段──▶ PENDING（旧租约一律失效）
```

- **单消费者占用**：任务只在 `PENDING 且到了可见时间`，或 `RUNNING 且租约已过期` 时可被领取。
  领取时写入 `ownerToken` 与 `leaseExpireAt`；租约期内其他消费者 `poll` 拿不到、用旧令牌 `ack/nack` 一律返回 `LEASE_EXPIRED`。
- **至少一次投递**：消费失败（`nack`）或消费者崩溃（租约过期 / 进程重启）都会重新投递，因此重复投递/重复执行是允许的，任务不会丢失；
  消费侧若需“恰好一次效果”，应结合下文的**幂等键**在业务侧去重。
- **租约（lease）**：默认 30s，可用 `taskqueue.lease-millis` 调整。租约过期不是立即抢占，而是在下次扫描到该任务且已过可见/到期时间时重新投递。

### 续期（心跳）

长任务处理过程中可调用 `renew(offset, ownerToken)` 刷新租约到期时间（`leaseExpireAt = now + leaseMillis`），避免处理到一半被当成失联重新投递：

- 只有**当前持有有效租约的占用者**续期成功（`APPLIED`）；租约已过期、已被其他消费者接手、任务未在运行或已进入终态时，续期被明确拒绝（分别返回 `LEASE_EXPIRED` / `NOT_RUNNING` / `ALREADY_ACKED` / `DEAD`）；
- 续期不改变任务状态（保持 `RUNNING`）、不增加 `attempts`；
- 续期失败后，原消费者的 `ack` / `nack` 同样会被拒绝——占用权已经不在它手上。

### 主动放弃与优雅退出

- `abandon(offset, ownerToken)`：消费者主动放弃当前任务。任务立即回到 `PENDING` 并**立即可见**（不等租约过期、无退避），其他消费者下一次 `poll` 即可领取；放弃不消耗额外重试额度、不记录失败原因。放弃后原消费者的 `ack` / `nack` / `renew` 一律被拒绝（`LEASE_EXPIRED` 或任务尚未被领取时的 `NOT_RUNNING`）。
- `releaseAll(ownerToken)`：优雅退出。释放该消费者占用的全部任务并返回释放数量，使它们立即可被其他消费者领取。
- 任务在**正常处理（RUNNING）、放弃重投（PENDING）、失联回收（租约过期重投）**之间流转时，对外查询到的状态始终是状态机中的唯一一个值，配合 `ownerToken` / `attempts` / 日志可完整解释每一次流转。

## 2. 幂等提交（去重规则）与容量背压

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

`taskqueue.max-capacity` 限制**在途任务数**（`PENDING + RUNNING`），默认 `0` 表示不限容量（与旧行为完全一致）：

- 达到上限后，**新幂等键**的提交返回 `REJECTED_CAPACITY`（HTTP 429），结果中不含任务——任务是**被明确拒绝**而不是被静默丢弃，调用方可据此稍后重试或告警；
- **幂等重复提交不占容量**：相同幂等键的重复提交（`DEDUPLICATED` / `ALREADY_SUCCEEDED` / `ALREADY_DEAD`）即使队列已满也照常返回原有幂等结果，容量限制不会改变幂等语义；
- **终态任务不占容量**：`SUCCEEDED` / `DEAD` 不计入在途数，确认或死信后容量自动释放，新提交恢复 `ACCEPTED`；
- 可通过 `inFlightCount()` 观测当前在途数。

## 3. 重试与死信规则

- 最大投递次数（含首次）由 `taskqueue.max-attempts` 控制，默认 **5**。每次 `poll` 成功占用都会使 `attempts += 1`。
- `nack(offset, owner, reason)` 必须持有有效租约，结果：
  - `attempts < maxAttempts`：任务回到 `PENDING`，按**指数退避**设置重新可见时间，记录本次失败原因；
  - `attempts == maxAttempts`：任务进入 `DEAD`（死信），**保留最后一次失败原因**（`failureReason`）、投递次数与完成时间，可通过 `listDeadLetters()` / HTTP 接口查询，**绝不静默丢弃**。
- 退避策略可解释：`delay = min(retryMaxMillis, retryBaseMillis * 2^(attempts-1))`，
  默认基数 1s、上限 60s，例如 1s → 2s → 4s → 8s → 16s … 封顶 60s。
- 不同失败原因会依次记录在任务上；死信时保留最终原因（测试中验证了 `NullPointerException…` / `Timeout…` / `HTTP 500…` 等可区分原因的流转）。

## 4. 位点（offset）与重启恢复

- 每个任务有不可变的单调递增 `offset`（0,1,2,…）。
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
| `taskqueue.max-capacity` | `0` | 在途任务数上限（PENDING+RUNNING），`<=0` 不限容量 |

## 6. HTTP 演示接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/tasks` | 幂等提交，body：`{"key":"...","payload":"..."}`；容量满返回 429 + `REJECTED_CAPACITY` |
| `POST /api/tasks/poll?owner=consumer-1` | 拉取并租约占用一个任务；无任务 204 |
| `POST /api/tasks/{offset}/ack?owner=consumer-1` | 成功确认（可能推进位点） |
| `POST /api/tasks/{offset}/nack?owner=consumer-1&reason=...` | 失败上报（重试或入死信） |
| `POST /api/tasks/{offset}/renew?owner=consumer-1` | 续期（心跳），仅当前有效占用者可用 |
| `POST /api/tasks/{offset}/abandon?owner=consumer-1` | 主动放弃，任务立即可被他人领取 |
| `POST /api/tasks/release?owner=consumer-1` | 优雅退出，释放该消费者全部占用，返回释放数 |
| `GET  /api/tasks` / `GET /api/tasks/{offset}` | 列出/查询任务 |
| `GET  /api/tasks/dead-letters` | 列出死信（含失败原因） |
| `GET  /api/tasks/committed-offset` | 查看已提交位点 |
| `POST /api/tasks/committed-offset/{target}` | 显式推进位点；回退/跨越未确认 → 409 |

## 7. 验证方法

需要 JDK 21+ 与 Maven，无需任何外部服务：

```bash
mvn test
```

测试（共 23 个）覆盖：

| 测试类 | 覆盖点 |
| --- | --- |
| `IdempotencyAndDeliveryTest` | 完成前重复提交去重且只执行一次；成功后重提 `ALREADY_SUCCEEDED`；重复 ack 为 `ALREADY_ACKED`；租约过期重新投递、旧 owner 确认被拒 `LEASE_EXPIRED` |
| `ConcurrentPollingTest` | 8 个消费者并发争抢 50 个任务：每个任务同一时刻只有一个占用者、恰好被成功确认一次，位点推进到末尾 |
| `RetryAndDeadLetterTest` | 退避序列 1s/2s/4s…封顶；退避窗口内不可见；重试耗尽入死信并保留可区分原因；死信不再投递且挡住位点；死信后重提 `ALREADY_DEAD` |
| `OffsetAndRecoveryTest` | 位点回退/跨越未确认被明确拒绝；重启恢复 SUCCEEDED/DEAD/PENDING/位点/死信原因；重启使 RUNNING 租约失效并按序重新投递 |
| `LeaseRenewalTest` | 续期延长租约并阻止重投；非占用者/过期后续期被拒且原消费者 ack/nack 被拒；终态与未占用任务续期被拒 |
| `AbandonAndReleaseTest` | 放弃后任务立即可见、原 owner 确认被拒、不耗重试额度；优雅退出 `releaseAll` 释放全部占用；处理→放弃→失联回收流转中状态唯一 |
| `CapacityBackpressureTest` | 容量满时新键 `REJECTED_CAPACITY` 且不入队；重复/已完成/已死信重提不占容量且幂等结果不变；确认后容量释放恢复 `ACCEPTED` |
| `ConcurrentOwnershipTest` | 真实时钟下 6 个消费者混合 poll/renew/abandon/ack 争用 30 个任务：任一时刻单占用、每任务恰好成功确认一次 |
| `TaskQueueWebIntegrationTest` | HTTP 端到端：幂等提交、租约拉取、确认、非法 seek 返回 409 |

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

## 8. 兼容性说明

本轮新增能力（续期、主动放弃/优雅退出、容量背压）均为**纯增量**，不改变既有语义：

- 提交、确认、重试、死信、位点、重启恢复的行为与返回枚举**完全不变**；原有 11 个测试用例未修改即全部通过；
- `renew` / `abandon` / `releaseAll` 是新增操作，不调用则行为与之前一致；
- `taskqueue.max-capacity` 默认 `0`（不限容量），不配置时 `enqueue` 行为与之前完全一致；
- 快照文件格式未变，旧快照可直接恢复。

## 9. 代码结构

```
src/main/java/com/github/lisama569/reliabletaskqueuejava/
├── queue/
│   ├── TaskQueue.java                 # 队列接口（语义契约：提交/租约/续期/放弃/背压）
│   ├── InMemoryReliableTaskQueue.java # 核心实现：状态机/租约/续期/放弃/退避/位点/背压/恢复
│   ├── TaskRecord / QueueSnapshot     # 实体与持久化快照
│   ├── TaskState / EnqueueStatus / AckStatus  # 确定的状态与结果枚举
│   ├── RetryPolicy.java               # 指数退避（可解释）
│   ├── TaskQueueStore.java            # 存储接口
│   ├── JsonFileTaskQueueStore.java    # JSON 原子快照实现
│   └── OffsetRollbackException.java   # 位点回退拒绝
├── config/TaskQueueAutoConfiguration.java + QueueProperties.java
└── web/TaskQueueController.java       # HTTP 演示接口
```
