package org.adam.mq.common;

import org.adam.mq.mqserver.core.BasicProperties;

/**
 * 只是一个单纯的函数式接口(回调函数)，收到消息之后要处理消息时的调用方法
 */
@FunctionalInterface
public interface Consumer {
    // Delivery 这个方法的预期是在服务器收到消息之后调用
    // 通过这个方法把消息推送给对应的消费者
    // 注意：这里的方法名和参数都是参考RabbitMQ的设计
    void handleDelivery(String consumerTag, BasicProperties properties, byte[] body);
}
