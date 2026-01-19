package org.adam.mq.common;

import org.adam.mq.mqserver.core.BasicProperties;

import java.io.Serializable;

public class BasicPublishArguments extends BasicArguments implements Serializable {
    private String exchangeName; // 交换机名称
    private String routingKey; // 路由键
    private BasicProperties basicProperties; // 基本属性
    private byte[] body; // 消息体

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
