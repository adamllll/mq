package org.adam.mq;

import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.*;
import org.adam.mq.mqserver.datacenter.DiskDataCenter;
import org.adam.mq.mqserver.datacenter.MemoryDataCenter;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryDataCenterTests {
    private MemoryDataCenter memoryDataCenter = null;

    @BeforeEach
    public void setUp() {
        memoryDataCenter = new MemoryDataCenter();
    }

    @AfterEach
    public void tearDown() {
        memoryDataCenter = null;
    }

    // 创建一个测试交换机
    private Exchange createTestExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.DIRECT);
        exchange.setAutoDelete(false);
        exchange.setDurable(true);
        return exchange;
    }
    // 创建一个测试队列
    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(false);
        return queue;
    }

    // 针对交换机进行测试
    @Test
    public void testExchange() {
        // 1. 构造一个交换机并插入
        Exchange expectedExchange = createTestExchange("test-exchange");
        memoryDataCenter.insertExchange(expectedExchange);
        // 2. 查询出这个交换机，比较结果是否一直
        Exchange actualExchange = memoryDataCenter.getExchange("test-exchange");
        // 直接比较这两个引用指向同一个对象
        Assertions.assertEquals(expectedExchange, actualExchange);
        // 3. 删除这个交换机
        memoryDataCenter.deleteExchange("test-exchange");
        // 4. 再次查询，验证删除成功
        actualExchange = memoryDataCenter.getExchange("test-exchange");
        Assertions.assertNull(actualExchange);
    }

    // 针对队列进行测试
    @Test
    public void testQueue() {
        // 1. 构造一个队列，并插入
        MSGQueue expectedQueue = createTestQueue("test-queue");
        memoryDataCenter.inserQueue(expectedQueue);
        // 2. 查询出这个队列，比较结果是否一致
        MSGQueue actualQueue = memoryDataCenter.getQueue("test-queue");
        Assertions.assertEquals(expectedQueue, actualQueue);
        // 3. 删除这个队列
        memoryDataCenter.deleteQueue("test-queue");
        // 4. 再次查询，验证删除成功
        actualQueue = memoryDataCenter.getQueue("test-queue");
        Assertions.assertNull(actualQueue);
    }

    // 针对绑定进行测试
    @Test
    public void testBinding() throws MqException {
        // 1. 构造一个绑定，并插入
        Binding expectedBinding = new Binding();
        expectedBinding.setExchangeName("test-exchange");
        expectedBinding.setQueueName("test-queue");
        expectedBinding.setBindingKey("test-key");
        memoryDataCenter.insertBinding(expectedBinding);
        // 2. 查询出这个绑定，比较结果是否一致
        Binding actualBinding = memoryDataCenter.getBinding("test-exchange", "test-queue");
        Assertions.assertEquals(expectedBinding, actualBinding);

        ConcurrentHashMap<String, Binding> bindingMap = memoryDataCenter.getBindings("test-exchange");
        Assertions.assertEquals(1, bindingMap.size());
        Assertions.assertEquals(expectedBinding, bindingMap.get("test-queue"));

        // 3. 删除这个绑定
        memoryDataCenter.deleteBinding(expectedBinding);
        // 4. 再次查询，验证删除成功
        actualBinding = memoryDataCenter.getBinding("test-exchange", "test-queue");
        Assertions.assertNull(actualBinding);
    }

    // TODO: 针对消息进行测试
    private Message createTestMessage(String content) {
        Message message = Message.createMessageWithId("test-RoutinigKey", null, content.getBytes());
        return message;
    }
    @Test
    public void testMessage() throws MqException {
        // 1. 构造一个消息，并插入
        Message expectedMessage = createTestMessage("Hello, World!");
        memoryDataCenter.addMessage(expectedMessage);
        System.out.println("Inserted Message ID: " + expectedMessage.getMessageId());
        // 2. 查询出这个消息，比较结果是否一致
        Message actualMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage, actualMessage);
        System.out.println("Retrieved Message ID: " + actualMessage.getMessageId());
        // 3. 删除这个消息
        memoryDataCenter.deleteMessage(expectedMessage.getMessageId());
        // 4. 再次查询，验证删除成功
        actualMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    @Test
    public void testSendMessage() {
        // 1. 创建一个队列，创建10条消息，把这些消息都插入队列中
        MSGQueue queue = createTestQueue("test-queue");
        List<Message> expectedMessages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("Message " + i);
            memoryDataCenter.sendMessage(queue, message);
            expectedMessages.add(message);
        }
        // 2. 从队列中取出这些消息
        List<Message> actualMessages = new ArrayList<>();
        while (true) {
            Message message = memoryDataCenter.pollMessage("test-queue");
            // 队列为空，退出循环
            if (message == null) {
                break;
            }
            actualMessages.add(message);
        }
        // 3. 验证取出的消息和插入的消息一致
        Assertions.assertEquals(expectedMessages.size(), actualMessages.size());
        // 比较每个元素
        for (int i = 0; i < expectedMessages.size(); i++) {
            Assertions.assertEquals(expectedMessages.get(i), actualMessages.get(i));
        }
    }
    @Test
    public void testMessageWaitAck() {
        // 1. 创建一条消息，放入等待ack队列中
        Message expectedMessage = createTestMessage("Ack Message");
        memoryDataCenter.addMessageWaitAck("test-queue", expectedMessage);

        // 2. 从等待ack队列中取出这条消息，验证一致
        Message actualMessage = memoryDataCenter.getMessageWaitAck("test-queue", expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage, actualMessage);

        // 3. 从等待ack队列中删除这条消息，验证删除成功
        memoryDataCenter.removeMessageWaitAck("test-queue", expectedMessage.getMessageId());
        actualMessage = memoryDataCenter.getMessageWaitAck("test-queue", expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    @Test
    public void testRecovery() throws IOException, MqException, ClassNotFoundException {
        // 0. 启动SpringBoot应用上下文
        // 由于后续需要进行数据库操作，依赖MybatisPlus，所以需要先启动SpringBoot应用上下文，这样才能进行数据库操作
        MqApplication.context = SpringApplication.run(MqApplication.class);
        // 1. 在硬盘上构造好数据
        DiskDataCenter diskDataCenter = new DiskDataCenter();
        diskDataCenter.init();
        // 1.1 构造交换机
        Exchange expectedExchange = createTestExchange("recovery-exchange");
        diskDataCenter.insertExchange(expectedExchange);
        // 1.2 构造队列
        MSGQueue expectedQueue = createTestQueue("recovery-queue");
        diskDataCenter.insertQueue(expectedQueue);
        // 1.3 构造绑定
        Binding expectedBinding = new Binding();
        expectedBinding.setExchangeName("recovery-exchange");
        expectedBinding.setQueueName("recovery-queue");
        expectedBinding.setBindingKey("recovery-key");
        diskDataCenter.insertBinding(expectedBinding);
        // 1.4 构造消息
        Message expectedMessage = createTestMessage("Recovery Message");
        diskDataCenter.sendMessage(expectedQueue, expectedMessage);
        // 2. 执行恢复操作
        memoryDataCenter.recovery(diskDataCenter);
        // 3. 验证恢复结果
        // 3.1 验证交换机
        Exchange actualExchange = memoryDataCenter.getExchange("recovery-exchange");
        // 交换机的对比不能直接对比对象，因为恢复出来的交换机是新创建的对象，所以只能对比属性值
        Assertions.assertEquals(expectedExchange.getName(), actualExchange.getName());
        Assertions.assertEquals(expectedExchange.getType(), actualExchange.getType());
        Assertions.assertEquals(expectedExchange.isAutoDelete(), actualExchange.isAutoDelete());
        Assertions.assertEquals(expectedExchange.isDurable(), actualExchange.isDurable());
        // 3.2 验证队列
        MSGQueue actualQueue = memoryDataCenter.getQueue("recovery-queue");
        // 队列的对比同理，不能直接对比对象
        Assertions.assertEquals(expectedQueue.getName(), actualQueue.getName());
        Assertions.assertEquals(expectedQueue.isDurable(), actualQueue.isDurable());
        Assertions.assertEquals(expectedQueue.isExclusive(), actualQueue.isExclusive());
        Assertions.assertEquals(expectedQueue.isAutoDelete(), actualQueue.isAutoDelete());
        // 3.3 验证绑定
        Binding actualBinding = memoryDataCenter.getBinding("recovery-exchange", "recovery-queue");
        // 绑定的对比同理，不能直接对比对象
        Assertions.assertEquals(expectedBinding.getExchangeName(), actualBinding.getExchangeName());
        Assertions.assertEquals(expectedBinding.getQueueName(), actualBinding.getQueueName());
        Assertions.assertEquals(expectedBinding.getBindingKey(), actualBinding.getBindingKey());
        // 3.4 验证消息
        Message actualMessage = memoryDataCenter.pollMessage("recovery-queue");
        // 消息的对比同理，不能直接对比对象
        Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
        Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
        Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
        Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());

        // 4. 清理硬盘上的数据,把整个data目录(meta.db和队列的目录)都删除
        // 关闭SpringBoot应用上下文
        MqApplication.context.close();
        File dataDir = new File("./data");
        // 递归删除data目录
        FileUtils.deleteDirectory(dataDir);

    }
}
