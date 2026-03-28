# MQ 项目测试报告

## 1. 项目背景与测试目标

- `mq` 是一个基于 Java 17、Spring Boot 3.5.8、SQLite 和自定义 TCP 协议实现的学习型消息队列项目。
- 本轮测试工作的目标是补齐关键负路径与持久化回归，沉淀覆盖率、性能 smoke 和求职导向文档资产。

## 2. 测试环境与工具

- JDK：17
- Spring Boot：3.5.8
- SQLite JDBC：3.46.0.0
- JUnit：5 (Jupiter)
- JaCoCo：0.8.12
- 操作系统：Windows 11 / PowerShell
- 自动化命令：`.\\mvnw.cmd clean test`
- 性能命令：`.\\mvnw.cmd "-Dmq.perf.enabled=true" "-Dtest=MqPerformanceTests" test`

## 3. 测试策略与分层设计

- 单元层聚焦 `Router` 与 `MessageFileManager` 的纯逻辑、边界值和负路径。
- 集成层覆盖 `VirtualHost`、`DiskDataCenter`、`DataBaseManager` 的持久化与状态一致性。
- 端到端层覆盖 `MqClient` 与 Broker 的连接、发布、消费和 ACK 链路。
- 性能 smoke 采用 Java 测试脚本：吞吐场景走 Broker/客户端链路，ACK 延迟场景走 `VirtualHost` 服务端 ACK 路径以保证 smoke 稳定可复跑。

## 4. 测试数据隔离与运行时治理

- 通过 `TestRuntimeSupport` 统一管理 Spring 上下文、Broker 启停、端口就绪等待与 `./data` 清理。
- 通过 `@BeforeEach` / `@AfterEach` 保证 SQLite 与消息文件处于可预测状态，避免跨用例污染。
- 文档与统计只读取白名单中的 Surefire / JaCoCo / perf 产物，避免历史结果污染结论。

## 5. 功能测试执行结果

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

原始执行摘要：
- Tests run: 19, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.052 s -- in org.adam.mq.RouterTests
- Tests run: 17, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 17.10 s -- in org.adam.mq.VirtualHostTest
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 7.022 s -- in org.adam.mq.MqClientTests
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.709 s -- in org.adam.mq.MessageFileManagerTest
- Tests run: 10, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 14.01 s -- in org.adam.mq.DiskDataCenterTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13.79 s -- in org.adam.mq.DataBaseManagerTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.988 s -- in org.adam.mq.MemoryDataCenterTests
- Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.012 s -- in org.adam.mq.MqApplicationTests

## 6. 缺陷发现、修复与回归

- 发现 `Router.checkBindingKey` 允许 `order.a*`、`order.#suffix` 这类内联通配符通过校验。
- 修复方式是在 `bindingKey` 按 `.` 切分后，强制 `*` / `#` 只能作为独立 token 存在，并保留原有相邻通配符限制。
- `Router` 回归结果：
Tests run: 19, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.052 s -- in org.adam.mq.RouterTests
- 扩展回归还补齐了 `VirtualHost` 非法 `bindingKey`、非法 `routingKey`、缺失 exchange、ACK 失败和 durable ACK 清理场景。

## 7. 性能 smoke 结果

- `single-producer` 与 `multi-producer` 场景验证了 Broker/客户端发布吞吐。
- `publish-consume-ack` 场景验证了服务端 publish-consume-ACK 基础链路与 durable ACK 清理时延。
- 本次摘要：
- single-producer | total=500 | success=500 | failure=0 | elapsedMs=384.04 | tps=1301.95 | avgLatencyMs=0.77
- multi-producer | total=1000 | success=1000 | failure=0 | elapsedMs=172.92 | tps=5783.19 | avgLatencyMs=0.17
- publish-consume-ack | total=100 | success=100 | failure=0 | elapsedMs=461.18 | tps=216.83 | avgLatencyMs=4.61

## 8. 覆盖率与质量评估

- 行覆盖率：87.74%
- 核心路由、客户端链路、虚拟主机异常路径和消息文件 GC 边界均已建立自动化回归保护。
- 当前剩余风险主要在更高并发、更长时长稳定性、Broker 重启恢复与工业级压测方面。

## 9. 工程实践沉淀

- 抽取 `TestRuntimeSupport` 降低了测试运行时样板代码和环境抖动。
- JaCoCo、测试盘点、正式报告、简历亮点与面试问答统一沉淀到 `docs/testing`。
- 所有统计数据均来自真实执行产物，避免手填数字和模板化描述。

## 10. 结论与后续优化

- 当前仓库已经具备围绕路由、发布、消费/ACK、持久化文件 GC 和基础性能验证的可执行测试资产。
- 后续建议补充 durable / non-durable 性能对比、Broker 重启恢复测试和更细粒度的覆盖率拆分分析。

## 11. 附录

- 覆盖率摘要：`docs/testing/2026-03-27-mq-coverage-summary.md`
- 测试资产盘点：`docs/testing/2026-03-27-mq-test-inventory.md`
- 性能摘要：`target/perf-results/mq-performance-summary.txt`
- JaCoCo 报告：`target/site/jacoco/index.html`
