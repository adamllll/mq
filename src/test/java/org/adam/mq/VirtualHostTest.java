package org.adam.mq;

import org.adam.mq.common.Consumer;
import org.adam.mq.mqserver.VirtualHost;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.support.TestRuntimeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class VirtualHostTest {
    private VirtualHost virtualHost;

    @BeforeEach
    public void setup() throws IOException {
        TestRuntimeSupport.deleteDataDirectory();
        TestRuntimeSupport.startApplicationContext();
        virtualHost = new VirtualHost("test_vhost");
    }

    @AfterEach
    public void teardown() throws IOException {
        TestRuntimeSupport.stopApplicationContext();
        TestRuntimeSupport.deleteDataDirectory();
        virtualHost = null;
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

        boolean successDelete = virtualHost.exchangeDelete("test_Exchange");
        Assertions.assertTrue(successDelete);
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
    public void testBasicConsume1() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "testQueue");
        Assertions.assertTrue(queueBind);

        boolean ok = virtualHost.basicConsume("testConsumerTag", "test_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testQueue", basicProperties.getRoutingKey());
                Assertions.assertEquals(1, basicProperties.getDeliveryMode());
                Assertions.assertArrayEquals("hello".getBytes(), body);
            }
        });
        Assertions.assertTrue(ok);

        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "testQueue", null, "hello".getBytes());
        Assertions.assertTrue(publishSuccess);

        Thread.sleep(1000);
    }

    @Test
    public void testBasicConsume2() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "testQueue");
        Assertions.assertTrue(queueBind);

        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "testQueue", null, "hello".getBytes());
        Assertions.assertTrue(publishSuccess);

        boolean ok = virtualHost.basicConsume("testConsumerTag", "test_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testQueue", basicProperties.getRoutingKey());
                Assertions.assertEquals(1, basicProperties.getDeliveryMode());
                Assertions.assertArrayEquals("hello".getBytes(), body);
            }
        });
        Assertions.assertTrue(ok);

        Thread.sleep(1000);
    }

    @Test
    public void testBasicComsumeFanout() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("fanout_Exchange", ExchangeType.FANOUT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare1 = virtualHost.queueDeclare("fanout_Queue1", true, false, false, null);
        Assertions.assertTrue(queueDeclare1);
        boolean queueDeclareBind1 = virtualHost.queueBind("fanout_Queue1", "fanout_Exchange", "");
        Assertions.assertTrue(queueDeclareBind1);

        boolean queueDeclare2 = virtualHost.queueDeclare("fanout_Queue2", true, false, false, null);
        Assertions.assertTrue(queueDeclare2);
        boolean queueDeclareBind2 = virtualHost.queueBind("fanout_Queue2", "fanout_Exchange", "");
        Assertions.assertTrue(queueDeclareBind2);

        String message = "Hello, Fanout!";
        boolean publishSuccess = virtualHost.basicPublish("fanout_Exchange", "", null, message.getBytes());
        Assertions.assertTrue(publishSuccess);

        Thread.sleep(500);

        boolean consumer1 = virtualHost.basicConsume("fanoutConsumerTag1", "fanout_Queue1", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertArrayEquals("Hello, Fanout!".getBytes(), body);
            }
        });
        Assertions.assertTrue(consumer1);

        boolean consumer2 = virtualHost.basicConsume("fanoutConsumerTag2", "fanout_Queue2", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertArrayEquals("Hello, Fanout!".getBytes(), body);
            }
        });
        Assertions.assertTrue(consumer2);

        Thread.sleep(500);
    }

    @Test
    public void testBasicConsumeTopic() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("topic_Exchange", ExchangeType.TOPIC, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("topic_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("topic_Queue", "topic_Exchange", "user.*.update");
        Assertions.assertTrue(queueBind);

        boolean consumer = virtualHost.basicConsume("topicConsumerTag", "topic_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertArrayEquals("User update message".getBytes(), body);
            }
        });
        Assertions.assertTrue(consumer);

        boolean publishSuccess1 = virtualHost.basicPublish("topic_Exchange", "user.123.update", null, "User update message".getBytes());
        Assertions.assertTrue(publishSuccess1);

        boolean publishSuccess2 = virtualHost.basicPublish("topic_Exchange", "user.123.delete", null, "User delete message".getBytes());
        Assertions.assertTrue(publishSuccess2);

        Thread.sleep(500);
    }

    @Test
    public void testQueueBindRejectsInlineWildcardBindingKey() {
        Assertions.assertTrue(virtualHost.exchangeDeclare("invalid_exchange", ExchangeType.TOPIC, true, false, null));
        Assertions.assertTrue(virtualHost.queueDeclare("invalid_queue", true, false, false, null));
        Assertions.assertFalse(virtualHost.queueBind("invalid_queue", "invalid_exchange", "order.a*"));
    }

    @Test
    public void testBasicPublishRejectsInvalidRoutingKey() {
        Assertions.assertTrue(virtualHost.exchangeDeclare("invalid_routing_exchange", ExchangeType.TOPIC, true, false, null));
        Assertions.assertFalse(virtualHost.basicPublish("invalid_routing_exchange", "order.*.created", null, "hello".getBytes()));
    }

    @Test
    public void testBasicPublishFailsWhenExchangeMissing() {
        boolean publishSuccess = virtualHost.basicPublish("missing_exchange", "order.created", null, "hello".getBytes());
        Assertions.assertFalse(publishSuccess);
    }

    @Test
    public void testBasicAckFailsWhenMessageMissing() {
        Assertions.assertTrue(virtualHost.queueDeclare("ack_queue", true, false, false, null));
        Assertions.assertFalse(virtualHost.basicAck("ack_queue", "missing-message-id"));
    }

    @Test
    public void testDurableMessageIsRemovedFromDiskAfterAck() throws Exception {
        Assertions.assertTrue(virtualHost.exchangeDeclare("durable_exchange", ExchangeType.DIRECT, true, false, null));
        Assertions.assertTrue(virtualHost.queueDeclare("durable_queue", true, false, false, null));
        Assertions.assertTrue(virtualHost.queueBind("durable_queue", "durable_exchange", "durable_queue"));

        BasicProperties basicProperties = new BasicProperties();
        basicProperties.setDeliveryMode(2);
        Assertions.assertTrue(virtualHost.basicPublish("durable_exchange", "durable_queue", basicProperties, "durable".getBytes()));

        String[] messageIdHolder = new String[1];
        Assertions.assertTrue(virtualHost.basicConsume("durable_consumer", "durable_queue", false,
                (consumerTag, properties, body) -> {
                    messageIdHolder[0] = properties.getMessageId();
                    Assertions.assertTrue(virtualHost.basicAck("durable_queue", properties.getMessageId()));
                }));

        Thread.sleep(500);
        Assertions.assertNotNull(messageIdHolder[0]);
        Assertions.assertTrue(virtualHost.getDiskDataCenter()
                .loadAllMessageFromQueue("test_vhost-durable_queue")
                .isEmpty());
    }

    @Test
    public void testBasicAck() throws InterruptedException {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("ack_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("ack_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("ack_Queue", "ack_Exchange", "ack_BindingKey");
        Assertions.assertTrue(queueBind);

        boolean publishSuccess = virtualHost.basicPublish("ack_Exchange", "ack_BindingKey", null, "Message needing ack".getBytes());
        Assertions.assertTrue(publishSuccess);

        boolean consumer = virtualHost.basicConsume("ackConsumerTag", "ack_Queue", false, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertArrayEquals("Message needing ack".getBytes(), body);

                boolean ackResult = virtualHost.basicAck("ack_Queue", basicProperties.getMessageId());
                Assertions.assertTrue(ackResult);
            }
        });
        Assertions.assertTrue(consumer);

        Thread.sleep(500);
    }
}
