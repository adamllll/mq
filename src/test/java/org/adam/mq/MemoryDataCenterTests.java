package org.adam.mq;

import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.Binding;
import org.adam.mq.mqserver.core.Exchange;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.datacenter.MemoryDataCenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
