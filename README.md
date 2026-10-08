# concurrent-batch-pipeline-java

具备**背压**、**幂等**、**状态查询与重提**语义的本地并发批处理服务（Spring Boot 3 / Java 21）。

## 功能概览

- **批量提交、逐条反馈**：`POST /api/batch` 一次提交多条记录，逐条返回最终状态与原因。
- **幂等去重**：以 `businessId` 为幂等键，重复提交不产生重复副作用，并返回可区分原因。
- **状态查询（只读）**：按 `businessId` 查询四态——已成功 / 已失败 / 处理中 / 未知，
  查询不改变状态、不占用处理容量。
- **背压后可重提、占用可回收**：容量满被快速挡回不占用标识，高峰过后同一标识可作为
  新任务正常受理；只有已有最终结论的标识，再提交才算重复。
- **重启语义明确**：已确认结论（成功/失败）落盘、重启不丢；处理中被中断、没有结论的
  标识重启后按未知处理，可重新提交并得到唯一结论。
- **有界并发 + 背压**：并发度与等待队列均有界，超时无法承接即快速拒绝，不无限堆积。
- **失败分类**：可重试失败按上限重试，不可恢复失败直接落终态。
- **停机收敛**：关闭时在途任务安全完成并落盘。
- **统计守恒**：每条提交恰好落一个终态，`sum(stats) == submitted` 可随时校验。

## 并发与背压模型

```
提交方 ──> 幂等占位(tryClaim) ──> 许可获取(Semaphore, 超时即拒) ──> 工作线程池 ──> 终态落盘
              │                        │
              ├─ 已有最终结论/在途      ├─ 超时/关闭 → REJECTED (backpressure)
      │       │  → DUPLICATE          │   └─ releaseClaim 回收占位（标识回到未知，可重提）
```

- 系统内容纳上限 = `pipeline.concurrency`（处理中）+ `pipeline.queueCapacity`（排队中），
  由信号量统一控制。
- 提交方在 `pipeline.offer-timeout-ms` 内拿不到许可即返回 `REJECTED`，
  原因含 `backpressure`，调用方不会被无限阻塞。
- **拒绝不占标识**：背压拒绝 / 关闭竞态发生在形成结论之前，引擎会 `releaseClaim`
  回收内部占位。因此同一标识反复被挡后仍可重提，内部占用随拒绝次数**零增长**
  （可通过 stats 中的 `claimed` 观测：`claimed = 已确认结论数 + 在途占位数`）。
- 每条记录的状态计数单调累加且互斥，高峰期也满足守恒：`sum(stats) == submitted`。

## 状态查询（GET /api/batch/status/{businessId}）

只读返回四态，`state` 取值：

| state | 含义 | recordStatus | 重提行为 |
|---|---|---|---|
| `SUCCEEDED` | 已有最终成功结论 | `PROCESSED` | 重提返回 `DUPLICATE`（already confirmed） |
| `FAILED` | 已有最终失败结论 | `FAILED_RETRYABLE` / `FAILED_FATAL` | 重提返回 `DUPLICATE`（already confirmed） |
| `IN_PROGRESS` | 已受理、正在处理，尚无结论 | null | 并发重提返回 `DUPLICATE`（in-flight） |
| `UNKNOWN` | 从未提交，或曾被背压挡回 | null | **作为新任务正常受理** |

查询语义：

- **只读**：不写任何记录、不获取许可，连续查询结果一致，也不影响吞吐。
- **重启一致**：`SUCCEEDED` / `FAILED` 直接读落盘结论，重启后查询结果与重启前一致；
  `IN_PROGRESS` 仅存内存，重启后归入 `UNKNOWN`（因为中断时确实没有结论）。
- 返回体示例：
  `{"businessId":"a-1","state":"SUCCEEDED","recordStatus":"PROCESSED","reason":"processed","attempts":1}`

## 幂等、重提与重启规则

| 提交时标识处境 | 本次结果 | 后续同一标识 |
|---|---|---|
| 从未见过 / 仅被背压挡回过 | 正常受理，给真实结论 | — |
| 正在处理（在途） | `DUPLICATE`（`already in-flight`） | 待其落定后再提 → `DUPLICATE`（已确认） |
| 已有最终结论（成功或失败） | `DUPLICATE`（`already confirmed as ...`） | 永远重复，结论不覆盖 |
| 处理中被服务中断、重启 | 重启后查为 `UNKNOWN` | **可重新提交受理**，只落一份结论 |

- 处理前先 `tryClaim` 占位，同一 `businessId` 全系统只有一个执行体，副作用最多发生一次。
- 终态结果**先追加落盘（`pipeline.store-file`）再对调用方可见**；重启回放日志恢复，
  **重启前已对外给过的确定结论一条不丢，且每个标识在日志中至多一条结论**。
- 在途占位仅内存、不持久化：任务处理到一半被 `kill -9` 时没有结论，重启不会谎称
  “处理过”，而是允许重提，由重提产生唯一真实结论。

| 状态 | 含义 | 触发条件 |
|---|---|---|
| `PROCESSED` | 处理成功 | 业务逻辑正常返回 |
| `DUPLICATE` | 幂等去重 | 同一 `businessId` 已确认（`already confirmed as ...`）或正在处理（`already in-flight`） |
| `REJECTED` | 快速拒绝 | 记录非法（空 `businessId`）、背压超时、引擎关闭中；拒绝后标识可重提 |
| `FAILED_RETRYABLE` | 可重试失败且次数耗尽 | 抛出 `RetryableProcessingException` 达 `pipeline.max-attempts` 次 |
| `FAILED_FATAL` | 不可恢复失败 | 抛出 `FatalProcessingException` 或未知异常，不重试 |

## 配置项（`pipeline.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `pipeline.concurrency` | 4 | 处理线程并发度 |
| `pipeline.queue-capacity` | 64 | 等待队列容量（有界） |
| `pipeline.offer-timeout-ms` | 200 | 提交等待空位的最长毫秒数，超时背压拒绝（拒绝后可重提） |
| `pipeline.max-attempts` | 3 | 可重试失败的最大尝试次数（含首次） |
| `pipeline.shutdown-timeout-ms` | 10000 | 关闭时等待在途任务收敛的最长毫秒数 |
| `pipeline.store-file` | `data/idempotency-store.log` | 幂等结果追加日志文件路径（仅终态结论落盘） |

## API

```bash
# 批量提交（payload 以 RETRY 开头模拟可重试失败，FATAL 开头模拟不可恢复失败）
curl -X POST localhost:8080/api/batch -H 'Content-Type: application/json' -d '[
  {"businessId":"a-1","payload":"ok"},
  {"businessId":"a-2","payload":"FATAL bad"},
  {"businessId":"a-1","payload":"ok"}
]'
# => [{"businessId":"a-1","status":"PROCESSED",...},
#     {"businessId":"a-2","status":"FAILED_FATAL",...},
#     {"businessId":"a-1","status":"DUPLICATE","reason":"duplicate: already confirmed as PROCESSED",...}]

# 按业务标识只读查询状态
curl localhost:8080/api/batch/status/a-1
# => {"businessId":"a-1","state":"SUCCEEDED","recordStatus":"PROCESSED","reason":"processed","attempts":1}
curl localhost:8080/api/batch/status/a-x
# => {"businessId":"a-x","state":"UNKNOWN","recordStatus":null,"reason":null,"attempts":0}

# 容量高峰被挡回（REJECTED/backpressure）后，用同一标识重新提交即正常受理
curl -X POST localhost:8080/api/batch -H 'Content-Type: application/json' -d '[{"businessId":"a-1","payload":"ok"}]'
# a-1 已有最终结论 => DUPLICATE；而只被挡回过的标识 => 重新 PROCESSED

# 状态统计与守恒校验（claimed = 已确认结论数 + 在途占位数）
curl localhost:8080/api/batch/stats
# => {"stats":{"PROCESSED":1,...},"submitted":3,"claimed":2,"conserved":true}
```

### 重启验证（手工）

```bash
mvn spring-boot:run
# 提交若干记录（其中留一条处理中的场景可加长业务耗时），观察 SUCCEEDED/FAILED 已落盘后停掉进程
grep . data/idempotency-store.log        # 只有终态结论，每行一个标识，拒绝/在途不落盘
mvn spring-boot:run
curl localhost:8080/api/batch/status/<已成功标识>   # 仍为 SUCCEEDED，明细不变
curl localhost:8080/api/batch/status/<在途中断标识> # 为 UNKNOWN，可重新提交
```

## 本地验证

```bash
# 运行全部测试（13 个）
mvn test

# 本地启动（随机端口见日志，或自行指定 server.port）
mvn spring-boot:run

# 打包
mvn package
```

测试覆盖（旧能力保留）：并发提交守恒、已确认重复去重、在途去重、容量饱和快速拒绝、
失败分类、停机收敛。新增覆盖：

- `backpressureRejected_sameIdCanResubmitAndSettles`：容量拒绝后同一标识高峰过去可正常
  重提并给出真实结论，已有结论后再提才算重复；
- `repeatedRejection_internalClaimsDoNotGrow`：8 标识 × 5 轮共 40 次拒绝，期间系统占用
  恒为 1（只剩在途 anchor），放行后全部受理，落盘日志每标识仅一条结论；
- `restart_confirmedResultsSurviveAndInterruptedInFlightIsResubmittable`：强制中断在途
  任务（模拟 kill -9），重启后已确认结论查询/重提不变、被中断标识查为 UNKNOWN 且可重提；
- `queryStatus_fourStatesAndReadOnly`：四态可区分、连续查询只读不改状态不增占用；
- 集成测试覆盖 HTTP 查询端点四态与只读语义。

测试日志中会打印**输入规模、并发度/队列/容量/超时设置与判定依据**
（如 `输入: 8 个标识 x 5 轮拒绝（共 40 次被挡回）; 容量=1, 超时=20ms`、
`守恒判定依据: submitted=49 == sum(stats)=49`），便于核对状态一致性。
