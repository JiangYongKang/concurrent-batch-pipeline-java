# concurrent-batch-pipeline-java

具备**背压**与**幂等**语义的本地并发批处理服务（Spring Boot 3 / Java 21）。

## 功能概览

- **批量提交、逐条反馈**：`POST /api/batch` 一次提交多条记录，逐条返回最终状态与原因。
- **幂等去重**：以 `businessId` 为幂等键，重复提交不产生重复副作用，并返回可区分原因。
- **有界并发 + 背压**：并发度与等待队列均有界，超时无法承接即快速拒绝，不无限堆积。
- **失败分类**：可重试失败按上限重试，不可恢复失败直接落终态。
- **停机收敛**：关闭时在途任务安全完成并落盘，重启后不丢已确认结果、不重复处理。
- **统计守恒**：每条提交恰好落一个终态，`sum(stats) == submitted` 可随时校验。

## 并发与背压模型

```
提交方 ──> 幂等占位(tryClaim) ──> 许可获取(Semaphore, 超时即拒) ──> 工作线程池 ──> 终态落盘
              │                        │
              ├─ 已存在 → DUPLICATE    └─ 超时/关闭 → REJECTED (backpressure)
```

- 系统内容纳上限 = `pipeline.concurrency`（处理中）+ `pipeline.queueCapacity`（排队中），
  由信号量统一控制。
- 提交方在 `pipeline.offer-timeout-ms` 内拿不到许可即返回 `REJECTED`，
  原因含 `backpressure`，调用方不会被无限阻塞。
- 每条记录的状态计数单调累加且互斥，高峰期也满足守恒：`sum(stats) == submitted`，
  可通过 `GET /api/batch/stats` 观察（含 `conserved` 校验位）。

## 幂等与失败分类规则

| 状态 | 含义 | 触发条件 |
|---|---|---|
| `PROCESSED` | 处理成功 | 业务逻辑正常返回 |
| `DUPLICATE` | 幂等去重 | 同一 `businessId` 已确认（`already confirmed as ...`）或正在处理（`already in-flight`） |
| `REJECTED` | 快速拒绝 | 记录非法（空 `businessId`）、背压超时、引擎关闭中 |
| `FAILED_RETRYABLE` | 可重试失败且次数耗尽 | 抛出 `RetryableProcessingException` 达 `pipeline.max-attempts` 次 |
| `FAILED_FATAL` | 不可恢复失败 | 抛出 `FatalProcessingException` 或未知异常，不重试 |

- 处理前先 `tryClaim` 占位，同一 `businessId` 全系统只有一个执行体，副作用最多发生一次。
- 终态结果**先追加落盘（`pipeline.store-file`）再对调用方可见**；重启时回放日志恢复，
  已确认结果不丢失；在途（未落终态）的标识重启后允许重新提交，不会重复处理。

## 配置项（`pipeline.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `pipeline.concurrency` | 4 | 处理线程并发度 |
| `pipeline.queue-capacity` | 64 | 等待队列容量（有界） |
| `pipeline.offer-timeout-ms` | 200 | 提交等待空位的最长毫秒数，超时背压拒绝 |
| `pipeline.max-attempts` | 3 | 可重试失败的最大尝试次数（含首次） |
| `pipeline.shutdown-timeout-ms` | 10000 | 关闭时等待在途任务收敛的最长毫秒数 |
| `pipeline.store-file` | `data/idempotency-store.log` | 幂等结果追加日志文件路径 |

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

# 状态统计与守恒校验
curl localhost:8080/api/batch/stats
# => {"stats":{"PROCESSED":1,...},"submitted":3,"conserved":true}
```

## 本地验证

```bash
# 运行全部测试（覆盖并发提交、重复提交、在途去重、容量饱和、失败分类、停机收敛）
mvn test

# 本地启动（随机端口见日志，或自行指定 server.port）
mvn spring-boot:run

# 打包
mvn package
```

测试日志中会打印**输入规模、并发度与判定依据**（如 `守恒判定依据: submitted=200 == sum(stats)=200`），
便于核对状态一致性。
