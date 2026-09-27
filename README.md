# concurrent-batch-pipeline-java

具备背压与幂等语义的本地并发批处理服务（Spring Boot 3 / Java 21）。

## 并发与背压模型

- **有界并发**：处理线程池固定为 `pipeline.concurrency` 大小；信号量许可数 =
  `concurrency + queueCapacity`，许可覆盖"处理中 + 排队中"的全部在途记录。
- **快速拒绝**：调用线程在 `pipeline.acquire-timeout-ms` 内拿不到许可，该条记录立即以
  `REJECTED_CAPACITY` 终态返回，原因串中携带当时的并发度/队列容量/等待时限，
  不无限堆积、不阻塞调用方。
- **同步批量反馈**：`POST /api/batch` 一次提交多条记录，响应中逐条给出终态、原因与
  实际执行次数（`attempts`），并附按状态聚合的计数。
- **统计守恒**：不变量 `submitted == 各终态计数之和 + inFlight`，由
  `PipelineMetrics.isConserved()` 校验，可通过 `GET /api/batch/metrics` 观察
  （`conserved=1` 表示守恒成立）。

## 幂等与失败分类规则

- **幂等键**：`BatchRecord.businessKey`。处理前先 `tryClaim` 落盘（WAL 追加 `C` 行），
  已占用的键直接返回 `DUPLICATE`，原因区分两种情形：
  - `业务标识已存在终态结果: <STATUS>` —— 重复提交已完成记录；
  - `业务标识正在处理中或为停机恢复的在途占用` —— 并发在途或停机恢复占用。
- **失败分类**（由 `RecordProcessor` 抛出的异常类型决定）：
  - `RetryableProcessingException` → 可重试，至多重试 `pipeline.max-retries` 次，
    耗尽后置 `DEAD_LETTER`；
  - `PermanentProcessingException` 及其他未分类异常 → 不重试，置 `FAILED_PERMANENT`。
- **停机收敛与重启**：关闭时停止接收新任务，在途任务在
  `pipeline.shutdown-timeout-ms` 内完成终态落盘（WAL 追加 `F` 行）后退出。
  重启时从 `pipeline.store-path` 恢复：已有终态的键直接返回原结果（已确认结果不丢失）；
  仅有 claim 无终态的键视为"结果未知"，保持占用并对重复提交返回 `DUPLICATE`，
  保证不产生重复副作用。

## 配置项（application.properties）

| 键 | 默认 | 含义 |
|---|---|---|
| `pipeline.concurrency` | 4 | 最大并发处理度 |
| `pipeline.queue-capacity` | 8 | 等待队列容量（计入许可总数） |
| `pipeline.acquire-timeout-ms` | 200 | 获取许可的最大等待毫秒数，超时快速拒绝 |
| `pipeline.max-retries` | 2 | 可重试失败的最大重试次数 |
| `pipeline.shutdown-timeout-ms` | 5000 | 停机时在途任务收敛等待毫秒数 |
| `pipeline.store-path` | data/idempotency-store.log | 幂等 WAL 文件路径 |

## 接口

- `POST /api/batch` —— 请求体为 `[{"businessKey":"k1","payload":"..."}]`，
  响应 `{total, counts, results:[{businessKey,status,reason,attempts}]}`。
  默认处理器中 payload 以 `RETRYABLE:` / `PERMANENT:` 开头可分别模拟两类失败。
- `GET /api/batch/metrics` —— 计数快照与守恒标志。

## 本地验证

```bash
# 运行全部测试（并发提交 / 重复提交 / 容量饱和 / 失败分类 / 停机恢复）
mvn test

# 启动服务（server.port=0 表示随机端口，日志中会打印实际端口）
mvn spring-boot:run

# 提交一批记录（含重复键与两类失败）
curl -X POST http://localhost:<port>/api/batch \
  -H 'Content-Type: application/json' \
  -d '[{"businessKey":"a","payload":"ok"},
       {"businessKey":"a","payload":"ok"},
       {"businessKey":"b","payload":"RETRYABLE:临时故障"},
       {"businessKey":"c","payload":"PERMANENT:数据非法"}]'

# 观察计数守恒
curl http://localhost:<port>/api/batch/metrics
```

测试日志会打印每个用例的输入规模、并发度与判定依据（如 `[容量饱和] 输入: 6 线程
各提交 1 条, 并发度=1 队列=0 等待=50ms ...`），并逐条打印去重/拒绝原因。
