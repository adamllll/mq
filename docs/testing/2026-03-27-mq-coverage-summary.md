# MQ 覆盖率摘要

## 1. 数据来源

- 回归命令：`.\mvnw.cmd clean test`
- 统计范围：8 个回归测试类、77 个自动化用例
- 统计来源：`target/site/jacoco/jacoco.xml` 与 `target/surefire-reports` 白名单报告
- 说明：本摘要只统计日常回归套件，不包含手动 gated 的性能 smoke 场景

## 2. 结果总览

| 指标 | 数值 |
| --- | --- |
| 行覆盖率 | 84.60% |
| 已覆盖行 | 1242 |
| 未覆盖行 | 226 |
| 总行数 | 1468 |
| 与 85% 目标差距 | 0.40 个百分点 |

## 3. 回归执行摘要

- Tests run: 19, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.085 s -- in org.adam.mq.RouterTests
- Tests run: 17, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 12.02 s -- in org.adam.mq.VirtualHostTest
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 6.216 s -- in org.adam.mq.MqClientTests
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.764 s -- in org.adam.mq.MessageFileManagerTest
- Tests run: 10, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 12.20 s -- in org.adam.mq.DiskDataCenterTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13.90 s -- in org.adam.mq.DataBaseManagerTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.845 s -- in org.adam.mq.MemoryDataCenterTests
- Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.013 s -- in org.adam.mq.MqApplicationTests

## 4. 已覆盖重点

- `Router`：覆盖 TOPIC 通配符匹配、非法 `bindingKey`、空交换机类型等关键路由逻辑。
- `VirtualHost`：覆盖发布、消费、ACK、durable ACK 清理，以及非法 `bindingKey` / `routingKey`、缺失 exchange、缺失消息 ACK 等负路径。
- `MqClient`：覆盖真实 TCP 链路、单连接多 Channel、同一 Channel 禁止重复注册 Consumer 等客户端场景。
- `MessageFileManager`：覆盖 GC 触发阈值边界、二进制消息文件读写删除与文件整理。
- `DiskDataCenter` / `DataBaseManager` / `MemoryDataCenter`：覆盖元数据 CRUD、消息落盘与内存态恢复。

## 5. 主要未覆盖路径

- `MqApplication.main()`：测试通过 `TestRuntimeSupport` 直接管理 Spring 上下文，不经过生产入口。
- `BrokerServer` 网络异常分支：需要故障注入或异常 Socket 场景才能稳定覆盖。
- `ConsumerManager` 线程池中断与异常传播路径：当前回归覆盖了正常异步消费链路，但未对线程级故障做注入。

## 6. 结论

- 当前回归套件已经对消息路由、发布消费 ACK、持久化与客户端链路形成了稳定保护。
- 覆盖率达到 84.60%，接近 85% 目标，但尚未完全达成。
- 后续如果要继续提升覆盖率，优先补网络异常、线程中断和 Broker 重启恢复等韧性场景，而不是继续堆简单正例。
