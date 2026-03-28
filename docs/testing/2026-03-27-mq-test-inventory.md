# MQ 测试资产盘点

## 1. 执行命令

- 自动化测试：`.\\mvnw.cmd clean test`
- 性能 smoke：`.\\mvnw.cmd "-Dmq.perf.enabled=true" "-Dtest=MqPerformanceTests" test`

## 2. 当前测试类清单

| 模块 | 测试类 | 用例数 | 通过 | 覆盖重点 |
| --- | --- | --- | --- | --- |
| 路由规则 | `RouterTests` | 19 | 19 | TOPIC 通配符、非法 bindingKey、空交换机类型 |
| 虚拟主机 | `VirtualHostTest` | 17 | 17 | 非法 bindingKey、非法 routingKey、发布失败、ACK 失败、durable ACK |
| 客户端链路 | `MqClientTests` | 8 | 8 | 多 Channel、重复 consumer、Broker TCP 链路 |
| 文件存储 | `MessageFileManagerTest` | 8 | 8 | GC 阈值边界、读写删除、文件 GC |
| 磁盘数据中心 | `DiskDataCenterTests` | 10 | 10 | 队列、绑定、消息落盘 |
| 数据库管理 | `DataBaseManagerTests` | 7 | 7 | 元数据增删查 |
| 内存数据中心 | `MemoryDataCenterTests` | 7 | 7 | 内存态对象与恢复 |
| 应用启动 | `MqApplicationTests` | 1 | 1 | Spring 上下文 |

## 3. 原始执行摘要

- Tests run: 19, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.052 s -- in org.adam.mq.RouterTests
- Tests run: 17, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 17.10 s -- in org.adam.mq.VirtualHostTest
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 7.022 s -- in org.adam.mq.MqClientTests
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.709 s -- in org.adam.mq.MessageFileManagerTest
- Tests run: 10, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 14.01 s -- in org.adam.mq.DiskDataCenterTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13.79 s -- in org.adam.mq.DataBaseManagerTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.988 s -- in org.adam.mq.MemoryDataCenterTests
- Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.012 s -- in org.adam.mq.MqApplicationTests

## 4. 附加说明

- 盘点结果只统计白名单中的回归测试报告，不混入历史残留文件。
- 覆盖率摘要见 `docs/testing/2026-03-27-mq-coverage-summary.md`。
- 性能摘要见 `target/perf-results/mq-performance-summary.txt`。
