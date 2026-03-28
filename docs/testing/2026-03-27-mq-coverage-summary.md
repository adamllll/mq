# MQ 覆盖率摘要

- 执行命令：`.\\mvnw.cmd clean test`
- JaCoCo HTML 报告：`target/site/jacoco/index.html`
- 行覆盖率：84.6%
- 测试执行摘要：
- Tests run: 19, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.052 s -- in org.adam.mq.RouterTests
- Tests run: 17, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 17.10 s -- in org.adam.mq.VirtualHostTest
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 7.022 s -- in org.adam.mq.MqClientTests
- Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.709 s -- in org.adam.mq.MessageFileManagerTest
- Tests run: 10, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 14.01 s -- in org.adam.mq.DiskDataCenterTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13.79 s -- in org.adam.mq.DataBaseManagerTests
- Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.988 s -- in org.adam.mq.MemoryDataCenterTests
- Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.012 s -- in org.adam.mq.MqApplicationTests

## 重点观察

- `Router`：已覆盖非法 `bindingKey` 与空交换机类型回归。
- `VirtualHost`：已覆盖非法 `bindingKey`、非法 `routingKey`、发布失败、ACK 失败与 durable ACK 清理。
- `MqClient`：已覆盖多 Channel 与重复 consumer 回调限制。
- `MessageFileManager`：已覆盖 GC 阈值边界。
