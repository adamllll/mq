package org.adam.mq.demo;

import org.adam.mq.common.Consumer;
import org.adam.mq.common.MqException;
import org.adam.mq.mqclient.Channel;
import org.adam.mq.mqclient.Connection;
import org.adam.mq.mqclient.ConnectionFactory;
import org.adam.mq.mqserver.core.BasicProperties;

import org.adam.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/**
 * 表示一个消费者
 * 通常这是一个单独的服务器程序
 */
public class DemoConsumer {
    public static void main(String[] args) throws IOException, MqException, InterruptedException {
        System.out.println("启动消费者");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        System.out.println("消费者已连接到消息队列服务器");

        channel.exchangeDeclare("demo_exchange", ExchangeType.DIRECT, true, false, null);
        channel.queueDeclare("demo_queue", true, false, false, null);
        channel.queueBind("demo_queue", "demo_exchange", "demo_bindingkey");

        channel.basicConsume("demo_queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties properties, byte[] body) throws MqException, IOException {
                System.out.println("[消费数据] 开始！");
                System.out.println("[消费数据] consumerTag=" + consumerTag);
                System.out.println("[消费数据] properties=" + properties);
                String bodyStr = new String(body, 0, body.length);
                System.out.println("[消费数据] body=" + bodyStr);
                System.out.println("[消费数据] 结束！");
            }
        });
        // 保持程序运行以接收消息(使用循环模拟一直等待消费)
        while (true) {
            Thread.sleep(500);
        }
    }
}
