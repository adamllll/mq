package org.adam.mq;

import org.adam.mq.mqserver.core.Binding;
import org.adam.mq.mqserver.core.Exchange;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.datacenter.DataBaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

import java.util.List;

// 设计单元测试，要求单元测试用例之间是相互独立的，不会互相干扰
public class DataBaseManagerTests {
    private DataBaseManager dbManager = new DataBaseManager();

    // 下面的每一个方法都是一个/一组测试用例
    // 还需要做一个准备工作，需要写两个方法，分别用于进行“准备工作”和“收尾工作”

    // 使用这个方法，来执行准备工作，每个用例执行前，都需要调用这个方法
    @BeforeEach
    public void setUp() {
        // 清理上一次异常中断留下的数据，确保当前用例从空库开始
        dbManager.deleteDB();
        // 由于init()中需要通过 context对象拿到 metaMapper实例
        // 所以就需要先把context对象初始化好
        MqApplication.context = SpringApplication.run(MqApplication.class);
        dbManager.init();
    }
    // 使用这个方法，来执行收尾工作，每个用例执行后，都需要调用这个方法
    @AfterEach
    public void tearDown() {
        // 把数据库文件删除掉，以保证每个用例都是在一个干净的环境下运行的
        // 此处不能直接删除，而需要先关闭上述 context 对象
        // 此处的 context 对象，持有了 MetaMapper的实例，而 MetaMapper实例持有了数据库连接(打开了meta.db文件)
        // 如果 meta.db文件正在被使用，那么就无法删除这个文件
        // 另一方面，获取 context 操作会占用8080 端口，此处的close也是释放端口的操作
        MqApplication.context.close();
        MqApplication.context = null;
        dbManager.deleteDB();
    }
    @Test
    public void testInnitTable() {
        // 由于init方法在上面setUp已经调用过，直接在测试用例代码中检查当前的数据库状态即可
        // 直接从数据库中，查询，看数据是否符合预期
        // 查交换机表，看是否有默认的交换机
        // 查队列表，看是否有默认的队列
        // 查绑定表，看是否有默认的绑定
        List<Exchange> exchangeList = dbManager.selectAllExchanges();
        List<MSGQueue> queueList = dbManager.selectAllQueues();
        List<Binding> bindingList = dbManager.selectAllBindings();

        // 直接打印结果，人工检查虽然可以但是不优雅，不方便
        // System.out.println(exchangeList.size());
        // 应该使用断言来进行自动化检查
        // assertEquals(预期值, 实际值),判定结果是不是相等
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
        Assertions.assertEquals(ExchangeType.DIRECT, exchangeList.get(0).getType());
        Assertions.assertEquals(0, queueList.size());
        Assertions.assertEquals(0, bindingList.size());
    }
    private Exchange createTestExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName("testExchange");
        exchange.setType(ExchangeType.FANOUT);
        exchange.setDurable(false);
        exchange.setAutoDelete(true);
        exchange.setArguments("aaa",1);
        exchange.setArguments("bbb",2);
        return exchange;
    }

    @Test
    public void testInsertExchange() {
        // 构造一个 Exchange 对象，插入到数据库中，再查询出来，检查是否插入成功
        Exchange exchange = createTestExchange("testExchange");
        dbManager.insertExchange(exchange);
        // 查询结果
        List<Exchange> exchangeList = dbManager.selectAllExchanges();
        // 断言检查
        Assertions.assertEquals(2, exchangeList.size());
        Exchange newExchange = exchangeList.get(1);
        Assertions.assertEquals("testExchange", newExchange.getName());
        Assertions.assertEquals(ExchangeType.FANOUT, newExchange.getType());
        Assertions.assertFalse(newExchange.isDurable());
        Assertions.assertTrue(newExchange.isAutoDelete());
        Assertions.assertEquals(1, newExchange.getArguments("aaa"));;
        Assertions.assertEquals(2, newExchange.getArguments("bbb"));
    }
    @Test
    public void deleteExchange() {
        Exchange exchange = createTestExchange("testExchange");
        dbManager.insertExchange(exchange);
        List<Exchange> exchangeList = dbManager.selectAllExchanges();
        Assertions.assertEquals(2, exchangeList.size());
        Assertions.assertEquals("testExchange", exchangeList.get(1).getName());
        // 删除刚才插入的交换机
        dbManager.deleteExchange("testExchange");
        // 再次查询结果
        exchangeList = dbManager.selectAllExchanges();
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
    }
    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        queue.setArguments("x",100);
        queue.setArguments("y",200);
        return queue;
    }
    @Test
    public void testInsertQueue() {
        MSGQueue queue = createTestQueue("testQueue");
        dbManager.insertQueue(queue);
        List<MSGQueue> queueList =  dbManager.selectAllQueues();
        Assertions.assertEquals(1, queueList.size());
        // 断言检查
        MSGQueue newQueue = queueList.get(0);
        Assertions.assertEquals("testQueue", newQueue.getName());
        Assertions.assertTrue(newQueue.isDurable());
        Assertions.assertFalse(newQueue.isAutoDelete());
        Assertions.assertFalse(newQueue.isExclusive());
        Assertions.assertEquals(100, newQueue.getArguments("x"));;
        Assertions.assertEquals(200, newQueue.getArguments("y"));
    }
    @Test
    public void deleteQueue() {
        MSGQueue queue = createTestQueue("testQueue");
        dbManager.insertQueue(queue);
        List<MSGQueue> queueList = dbManager.selectAllQueues();
        Assertions.assertEquals(1, queueList.size());
         // 删除刚才插入的队列
        dbManager.deleteQueue("testQueue");
        // 再次查询结果
        queueList = dbManager.selectAllQueues();
        Assertions.assertEquals(0, queueList.size());
    }

    private Binding createTestBinding(String exchangeName, String queueName) {
        Binding binding = new Binding();
        binding.setExchangeName(exchangeName);
        binding.setQueueName(queueName);
        binding.setBindingKey("testBindingKey");
        return binding;
    }

    @Test
    public void testInsertBinding() {
        Binding binding = createTestBinding("testExchange", "testQueue");
        dbManager.insertBinding(binding);
        // 插入之后执行一次查询
        List<Binding> bindingList =  dbManager.selectAllBindings();
        Assertions.assertEquals(1, bindingList.size());
        Assertions.assertEquals("testExchange", bindingList.get(0).getExchangeName());
        Assertions.assertEquals("testQueue", bindingList.get(0).getQueueName());
        Assertions.assertEquals("testBindingKey", bindingList.get(0).getBindingKey());
    }

    @Test
    public void testDeleteBinding() {
        Binding binding = createTestBinding("testExchange", "testQueue");
        dbManager.insertBinding(binding);
        // 依旧插入之后完成一次查询
        List<Binding> bindingList = dbManager.selectAllBindings();
        Assertions.assertEquals(1, bindingList.size());

        // 这种写法也可以，但不够严谨，因为它依赖于“持有插入时的那个对象引用”
       /* // 删除刚才插入的绑定
        dbManager.deleteBinding(binding);
        // 再次查询结果
        bindingList = dbManager.selectAllBindings();
        Assertions.assertEquals(0, bindingList.size());*/

        //通常删除操作发生时，前端或上游传来的只是参数，我们需要根据这些参数构造一个新的对象（或 DTO）去执行删除，
        // 而不是永远持有着插入时的那个原始对象引用。它能验证“只要属性值对，就能删除成功”。
        Binding toDeleteBinding = createTestBinding("testExchange", "testQueue");
        dbManager.deleteBinding(toDeleteBinding);
        // 再次查询结果
        bindingList = dbManager.selectAllBindings();
        Assertions.assertEquals(0, bindingList.size());


    }
}
