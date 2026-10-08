# concurrent-batch-pipeline-java

具备**背压**、**幂等**、**只读状态查询**与**重启一致性**语义的本地并发批处理服务（Spring Boot 3 / Java 21）。

## 功能概览

- **批量提交、逐条反馈**：`POST /api/batch` 一次提交多条记录，逐条返回最终状态与原因。
- **幂等去重**：以 `businessId` 为幂等键，**只有已确认最终结论**的标识才算重复；正在处理的提交去重但不产生重复副作用。
- **有界并发 + 背压**：并发度与等待队列均有界，超时无法承接即快速拒绝，不无限堆积；**被挡回的标识立即回收，可随时用同一标识重提，内部占用不膨胀**。
- **只读状态查询**：按业务标识区分 `CONFIRMED / IN_FLIGHT / INTERRUPTED / NOT_FOUND` 四种情况，查询不改状态、不占处理能力。
- **失败分类**：可重试失败按上限重试，不可恢复失败直接落终态。
- **停机/崩溃一致性**：终态先落盘再可见，重启后已确认结论一条不丢；处理途中被中断的标识可查、可重提，不伪造结论、不产生第二份结论。
- **统计守恒**：每条提交恰好落一个终态，`sum(stats) == submitted` 可随时校验。

## 并发与背压模型

```
提交方 ──> 原子受理(acquireClaim) ──> 许可获取(Semaphore, 超时即拒) ──> 写"处理已开始"日志(C)
              │                            │                                   │
              ├─ 已确认 → DUPLICATE         └─ 超时/关闭 → 回滚占位(REJECTED)    └─> 工作线程处理
              └─ 在途   → DUPLICATE              占位立即回收，可重提                   │
                                                                                 终态落盘(R)
```

- 系统内容纳上限 = `pipeline.concurrency`（处理中）+ `pipeline.queueCapacity`（排队中），由信号量统一控制。
- 提交方在 `pipeline.offer-timeout-ms` 内拿不到许可即返回 `REJECTED`（原因含 `backpressure`），调用方不会被无限阻塞。
- **容量拒绝先回滚内存占位再返回**：拒绝不写任何日志、不留记录。同一 `businessId` 随后可正常重提；反复被挡多少次，系统内部的在途/中断记录都不会增长。
- 每条记录的状态计数单调累加且互斥，高峰期也满足守恒：`sum(stats) == submitted`，可通过 `GET /api/batch/stats` 观察（含 `conserved`、`inFlight`、`interrupted`）。

## 按业务标识查询（只读）

`GET /api/batch/records/{businessId}` 返回只读快照：

| `state` | 含义 | `confirmedStatus` | 重提结果 |
|---|---|---|---|
| `CONFIRMED` | 已有最终结论（成功/失败都算） | `PROCESSED` / `FAILED_RETRYABLE` / `FAILED_FATAL` | `DUPLICATE`（`already confirmed as ...`） |
| `IN_FLIGHT` | 本进程已受理、正在处理，尚无结论 | `null` | `DUPLICATE`（`already in-flight`） |
| `INTERRUPTED` | 曾开始处理但没有终态（通常是处理途中进程被中断） | `null` | **正常受理**，处理后落唯一结论 |
| `NOT_FOUND` | 从未提交，或仅有过已被回收的容量拒绝 | `null` | **正常受理** |

查询是纯读操作：既不改变上述状态，也不申请处理许可（容量打满时查询照常可用）。

## 重提规则（关键变化）

- **容量拒绝不是终态**：因背压/关闭中被挡回的提交，占位当场回收。高峰期过后用**同一个 `businessId`** 重新提交，会被当成新任务正常受理并给出真实结论；只有后来拿到了最终结论，再提交才算 `DUPLICATE`。
- **在途去重保持不变**：处理尚未结束期间并发重复提交，仍返回 `DUPLICATE`（`already in-flight`）。
- **占用可回收**：容量拒绝只在内存里短暂存在；落盘的只有"处理已开始(C)"和"终态结果(R)"两类记录，终态落盘后 C 对应的未决标记同步收退，长跑下内部记录不随拒绝次数增长。

## 重启一致性保证

存储为本地追加日志（`pipeline.store-file`），每行一条记录：

- `C\t<businessId>`：一次处理尝试**已开始**（业务副作用发生前写入并 flush）；
- `R\t<businessId>\t<status>\t<attempts>\t<reason>`：终态结果（先 append+flush，再对调用方可见）。

启动时严格按行序回放：

1. **已确认结论不丢、不变**：凡有 `R` 行的标识，重启后查询仍是同一个 `CONFIRMED` + 同一个 `confirmedStatus`，再提交一律 `DUPLICATE`，不可能落第二份结论。
2. **在途中断不撒谎**：只有 `C` 没有 `R` 的标识（处理到一半进程被杀死/强杀），重启后呈现为 `INTERRUPTED`——系统既不会说它"已经处理过"，也不会伪造成功/失败结论。
3. **中断后可重提、结论唯一**：对 `INTERRUPTED` 标识重新提交会被正常受理，处理完落唯一 `R`；此后再提交即去重。
4. **容量拒绝无痕**：被容量挡回的提交不写日志，重启后等同于 `NOT_FOUND`。
5. 优雅停机（`close()`）仍会等待在途任务在 `pipeline.shutdown-timeout-ms` 内收敛并落盘。

> 说明：`INTERRUPTED` 只保证本服务"不伪造、不重复落结论、可安全重提"。如果业务副作用本身不是幂等的（例如外部扣款），下游仍应以 `businessId` 做幂等，这是 at-least-once 重提的常规要求。

旧版本无 `C/R` 前缀的四字段日志行会按终态结果（`R`）兼容回放。

## 幂等与失败分类规则

| 状态 | 含义 | 触发条件 |
|---|---|---|
| `PROCESSED` | 处理成功 | 业务逻辑正常返回 |
| `DUPLICATE` | 幂等去重 | 同一 `businessId` 已确认（`already confirmed as ...`）或正在处理（`already in-flight`） |
| `REJECTED` | 快速拒绝 | 记录非法（空 `businessId`）、背压超时、引擎关闭中、处理开始日志写盘失败 |
| `FAILED_RETRYABLE` | 可重试失败且次数耗尽 | 抛出 `RetryableProcessingException` 达 `pipeline.max-attempts` 次 |
| `FAILED_FATAL` | 不可恢复失败 | 抛出 `FatalProcessingException` 或未知异常，不重试 |

## 配置项（`pipeline.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `pipeline.concurrency` | 4 | 处理线程并发度 |
| `pipeline.queue-capacity` | 64 | 等待队列容量（有界） |
| `pipeline.offer-timeout-ms` | 200 | 提交等待空位的最长毫秒数，超时背压拒绝（占位即回收） |
| `pipeline.max-attempts` | 3 | 可重试失败的最大尝试次数（含首次） |
| `pipeline.shutdown-timeout-ms` | 10000 | 关闭时等待在途任务收敛的最长毫秒数 |
| `pipeline.store-file` | `data/idempotency-store.log` | 幂等 WAL 日志路径（`C` 开始行 + `R` 终态行） |

> 容量拒绝与重启恢复不需要额外开关；只要 `pipeline.store-file` 指向持久化路径（默认 `data/`），重启即自动回放。本地临时验证可把它指到临时目录。

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

# 按业务标识只读查询
curl localhost:8080/api/batch/records/a-1
# => {"businessId":"a-1","state":"CONFIRMED","confirmedStatus":"PROCESSED",
#     "reason":"processed","attempts":1}
curl localhost:8080/api/batch/records/not-here
# => {"businessId":"not-here","state":"NOT_FOUND","confirmedStatus":null,...}

# 状态统计、占用观察与守恒校验
curl localhost:8080/api/batch/stats
# => {"stats":{...},"submitted":3,"inFlight":0,"interrupted":0,"conserved":true}
```

容量满时重提的典型时序：

```bash
# 高峰期：a-3 拿到 REJECTED(backpressure) —— 之后查询是 NOT_FOUND，而不是被卡死
# 高峰过后：
curl -X POST localhost:8080/api/batch -H 'Content-Type: application/json' \
  -d '[{"businessId":"a-3","payload":"ok"}]'
# => PROCESSED（正常受理，真实结论）
```

## 本地验证

```bash
# 运行全部测试
mvn test
```

覆盖用例：

- 原有：并发提交守恒、重复提交（含重启后去重）、**在途去重**、容量饱和快速拒绝、失败分类（retryable/fatal）、停机收敛；
- 新增（`BatchRecoveryAndQueryTest`）：
  - 容量拒绝后同一标识可正常重提并拿到真实结论、终态后再提才判重；
  - 同一批标识被反复拒绝 200 次，在途/中断/终态记录与日志行数均不膨胀；
  - 重启后成功/可重试失败/不可恢复失败的查询结论与重启前一致，且不产生第二份结论；
  - 处理途中模拟强杀（仅有 `C` 行）：重启后查询为 `INTERRUPTED`，重提正常受理，全生命周期恰好一份终态；
  - 查询只读：多次查询不改状态、不占许可（容量满时新提交仍被背压拒绝）；
- API（`BatchApiIntegrationTest`）：提交/统计与按标识查询（`CONFIRMED` / `NOT_FOUND`、查询前后 stats 不变）。

测试日志会打印**输入规模、并发度、队列容量、超时设置与判定依据**，例如：

```
输入: 容量=1 被占满时，同一 businessId 反复冲击 200 次; 并发=1,队列=0,超时=20ms
判定依据: 200 次冲击全部 REJECTED; 被拒后在途数=1 (仅 holder=1), 中断数=0, 终态数=0
判定依据: 收敛后文件行数: C=1, R=1; 回放终态数=1, 在途=0, 中断=0
守恒判定依据: submitted=200 == sum(stats)=200
```

```bash
# 本地启动（随机端口见日志，或自行指定 server.port）
mvn spring-boot:run

# 打包
mvn package
```

手工模拟"处理途中重启"：把业务处理换成阻塞逻辑（或在调试中于处理过程中 kill 进程），重启后对同一标识先 `GET .../records/{id}` 应见 `INTERRUPTED`，再 `POST` 同一标识即可拿到最终结论。
