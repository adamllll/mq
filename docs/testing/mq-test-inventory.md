# MQ 测试资产盘点

## 1. 执行方式

- 自动化回归：`.\mvnw.cmd clean test`
- 性能 smoke：`.\mvnw.cmd "-Dmq.perf.enabled=true" "-Dtest=MqPerformanceTests" test`
- 说明：性能 smoke 独立执行，不计入 77 个日常回归用例

## 2. 回归白名单

| 层次 | 模块 | 测试类 | 用例数 | 通过 | 重点 |
| --- | --- | --- | --- | --- | --- |
| 单元 | 路由规则 | `RouterTests` | 19 | 19 | TOPIC 通配符匹配、非法 `bindingKey`、空交换机类型 |
| 集成 | 虚拟主机 | `VirtualHostTest` | 17 | 17 | 消息生命周期、负路径、durable ACK 清理 |
| 端到端 | 客户端链路 | `MqClientTests` | 8 | 8 | TCP 链路、单连接多 Channel、重复 Consumer 限制 |
| 单元 | 文件存储 | `MessageFileManagerTest` | 8 | 8 | GC 阈值边界、二进制读写删除 |
| 集成 | 磁盘数据中心 | `DiskDataCenterTests` | 10 | 10 | 元数据与消息落盘协同 |
| 集成 | 数据库管理 | `DataBaseManagerTests` | 7 | 7 | Exchange / Queue / Binding 元数据 CRUD |
| 单元 | 内存数据中心 | `MemoryDataCenterTests` | 7 | 7 | 内存态对象生命周期与恢复 |
| 启动 smoke | 应用启动 | `MqApplicationTests` | 1 | 1 | Spring Boot 上下文加载 |
| **合计** | **8 个测试类** | **回归套件** | **77** | **77** | **100% 通过** |

## 3. Fresh Surefire 摘要

- Tests run: 19, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.085 s -- in org.adam.mq.RouterTests
- Tests run: 17, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 12.02 s -- in org.adam.mq.VirtualHostTest
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 6.216 s -- in org.adam.mq.MqClientTests
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.764 s -- in org.adam.mq.MessageFileManagerTest
- Tests run: 10, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 12.20 s -- in org.adam.mq.DiskDataCenterTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13.90 s -- in org.adam.mq.DataBaseManagerTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.845 s -- in org.adam.mq.MemoryDataCenterTests
- Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.013 s -- in org.adam.mq.MqApplicationTests

## 4. 性能 smoke 资产

| 场景 | 定位 | 消息量 | 结果摘要 |
| --- | --- | --- | --- |
| `single-producer` | 单线程吞吐基线 | 500 | 1437.55 TPS，0.70 ms/条 |
| `multi-producer` | 并发扩展性 | 1000 | 4977.18 TPS，0.20 ms/条 |
| `publish-consume-ack` | 全链路延迟 | 100 | 233.83 TPS，4.28 ms/条 |

## 5. 说明

- 本文档只盘点显式纳入回归范围的白名单测试类，避免历史残留报告或手动性能测试污染统计结果。
- 覆盖率结论见 `docs/testing/mq-coverage-summary.md`。
- 性能结果以主报告第 9 节为准；性能 smoke 独立执行，不能直接与 `clean test` 的覆盖率结果混算。
