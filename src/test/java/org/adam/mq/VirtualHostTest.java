package org.adam.mq;

import org.adam.mq.common.Consumer;
import org.adam.mq.mqserver.VirtualHost;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;

@SpringBootTest
public class VirtualHostTest {
    private VirtualHost virtualHost = null;

    @BeforeEach
    public void setup() {
        MqApplication.context = SpringApplication.run(MqApplication.class); // 启动SpringBoot应用程序
        virtualHost = new VirtualHost("test_vhost");
    }

    @AfterEach
    public void teardown() throws IOException {
        MqApplication.context.close();
        virtualHost = null;
        // 把硬盘的目录删除掉
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
    }

    @Test
    public void testExchangeDeclare() {
        boolean success = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);
    }

    @Test
    public void testExchangeDelete() {
        boolean success = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        boolean success_delete = virtualHost.exchangeDelete("test_Exchange");
        Assertions.assertTrue(success_delete);
    }

    @Test
    public void testQueueDeclare() {
        boolean success = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(success);
    }

    @Test
    public void testQueueDelete() {
        boolean success = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = virtualHost.queueDelete("test_Queue");
        Assertions.assertTrue(success);
    }

    @Test
    public void testQueueBind() {
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);
    }

    @Test
    public void testQueueUnBind() {
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);

        boolean queueUnBind = virtualHost.queueUnbind("test_Queue", "test_Exchange");
        Assertions.assertTrue(queueUnBind);
    }

    @Test
    public void testBasicPublish() {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);

        String message = "Hello, World!";
        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "test_BindingKey", null, message.getBytes());
        Assertions.assertTrue(publishSuccess);
    }

    @Test
    // 先订阅队列，后发送消息
    public void testBasicConsume1() throws InterruptedException {
        // 1. 创建交换机
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        // 2. 创建队列
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        // 3. 绑定队列（应该在订阅之前，这样消息才能路由到队列）
        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "testQueue");
        Assertions.assertTrue(queueBind);

        // 4. 订阅队列
        boolean ok = virtualHost.basicConsume("testConsumerTag", "test_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("[Consumer] 消息ID: " + basicProperties.getMessageId());
                System.out.println("[Consumer] 消息体: " + new String(body, 0, body.length));

                Assertions.assertEquals("testQueue", basicProperties.getRoutingKey());
                Assertions.assertEquals(1, basicProperties.getDeliveryMode());
                Assertions.assertArrayEquals("hello".getBytes(), body);
            }
        });
        Assertions.assertTrue(ok);

        // 5. ⭐ 发送消息（这是你缺少的关键步骤！）
        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "testQueue", null, "hello".getBytes());
        Assertions.assertTrue(publishSuccess);

        // 6. 等待消费者处理消息
        Thread.sleep(1000);
    }

    @Test
    // 先发送消息，再订阅队列
    public void testBasicConsume2() throws InterruptedException {
        // 1. 创建交换机
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        // 2. 创建队列
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        // 3. 绑定队列
        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "testQueue");
        Assertions.assertTrue(queueBind);

        // 4. ⭐ 先发送消息（消息会进入队列等待消费）
        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "testQueue", null, "hello".getBytes());
        Assertions.assertTrue(publishSuccess);

        // 5. 再订阅队列（订阅后应该立即收到之前发送的消息）
        boolean ok = virtualHost.basicConsume("testConsumerTag", "test_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("[Consumer] 消息ID: " + basicProperties.getMessageId());
                System.out.println("[Consumer] 消息体: " + new String(body, 0, body.length));

                Assertions.assertEquals("testQueue", basicProperties.getRoutingKey());
                Assertions.assertEquals(1, basicProperties.getDeliveryMode());
                Assertions.assertArrayEquals("hello".getBytes(), body);
            }
        });
        Assertions.assertTrue(ok);

        // 6. 等待消费者处理消息
        Thread.sleep(1000);
    }

    @Test
    public void testBasicComsumeFanout() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("fanout_Exchange", ExchangeType.FANOUT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);
        // 创建两个队列，并绑定到fanout交换机上
        boolean queueDeclare1 = virtualHost.queueDeclare("fanout_Queue1", true, false, false, null);
        Assertions.assertTrue(queueDeclare1);
        boolean queueDeclareBind1 = virtualHost.queueBind("fanout_Queue1", "fanout_Exchange", "");
        Assertions.assertTrue(queueDeclareBind1);
        // 第二个队列
        boolean queueDeclare2 = virtualHost.queueDeclare("fanout_Queue2", true, false, false, null);
        Assertions.assertTrue(queueDeclare2);
        boolean queueDeclareBind2 = virtualHost.queueBind("fanout_Queue2", "fanout_Exchange", "");
        Assertions.assertTrue(queueDeclareBind2);

        // 往交换机中发布一个消息
        String message = "Hello, Fanout!";
        boolean publishSuccess = virtualHost.basicPublish("fanout_Exchange", "", null, message.getBytes());
        Assertions.assertTrue(publishSuccess);

        Thread.sleep(500); // 等待消息路由完成

        // 两个消费者订阅上述两个队列
        boolean consumer1 = virtualHost.basicConsume("fanoutConsumerTag1", "fanout_Queue1", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("[Consumer1] 消息ID: " + basicProperties.getMessageId());
                System.out.println("[Consumer1] 消息体: " + new String(body, 0, body.length));
                Assertions.assertArrayEquals("Hello, Fanout!".getBytes(), body);
            }
        });
        Assertions.assertTrue(consumer1);

        boolean consumer2 = virtualHost.basicConsume("fanoutConsumerTag2", "fanout_Queue2", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("[Consumer2] 消息ID: " + basicProperties.getMessageId());
                System.out.println("[Consumer2] 消息体: " + new String(body, 0, body.length));
                Assertions.assertArrayEquals("Hello, Fanout!".getBytes(), body);
            }
        });
        Assertions.assertTrue(consumer2);

        Thread.sleep(500); // 等待消费者处理消息
    }

    @Test
    public void testBasicConsumeTopic() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("topic_Exchange", ExchangeType.TOPIC, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("topic_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("topic_Queue", "topic_Exchange", "user.*.update");
        Assertions.assertTrue(queueBind);

        // 订阅队列
        boolean consumer = virtualHost.basicConsume("topicConsumerTag", "topic_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("[TopicConsumer] 消息ID: " + basicProperties.getMessageId());
                System.out.println("[TopicConsumer] 路由键: " + basicProperties.getRoutingKey());
                System.out.println("[TopicConsumer] 消息体: " + new String(body, 0, body.length));
                Assertions.assertArrayEquals("User update message".getBytes(), body);
            }
        });
        Assertions.assertTrue(consumer);

        // 发送符合路由键的消息
        boolean publishSuccess1 = virtualHost.basicPublish("topic_Exchange", "user.123.update", null, "User update message".getBytes());
        Assertions.assertTrue(publishSuccess1);

        // 发送不符合路由键的消息
        boolean publishSuccess2 = virtualHost.basicPublish("topic_Exchange", "user.123.delete", null, "User delete message".getBytes());
        Assertions.assertTrue(publishSuccess2);

        Thread.sleep(500); // 等待消费者处理消息
    }

    @Test
    public void testBasicAck() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("ack_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("ack_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("ack_Queue", "ack_Exchange", "ack_BindingKey");
        Assertions.assertTrue(queueBind);

        // 发送一条需要手动确认的消息
        boolean publishSuccess = virtualHost.basicPublish("ack_Exchange", "ack_BindingKey", null, "Message needing ack".getBytes());
        Assertions.assertTrue(publishSuccess);

        // 订阅队列，使用手动确认模式(autoAck=false)
        boolean consumer = virtualHost.basicConsume("ackConsumerTag", "ack_Queue", false, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("[AckConsumer] 消息ID: " + basicProperties.getMessageId());
                System.out.println("[AckConsumer] 消息体: " + new String(body, 0, body.length));
                Assertions.assertArrayEquals("Message needing ack".getBytes(), body);

                // 模拟处理完消息后进行确认(手动调用basicAck)
                boolean ackResult = virtualHost.basicAck("ack_Queue" ,basicProperties.getMessageId());
                Assertions.assertTrue(ackResult);
            }
        });
        Assertions.assertTrue(consumer);

        Thread.sleep(500); // 等待消费者处理消息
    }
}
