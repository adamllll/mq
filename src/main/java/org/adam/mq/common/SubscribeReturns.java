package org.adam.mq.common;

import org.adam.mq.mqserver.core.BasicProperties;

import java.io.Serializable;

public class SubscribeReturns extends BasicReturns implements Serializable {
    private String consumerTag; // 消费者标签
    private BasicProperties basicProperties; // 基本属性
    private byte[] body; // 消息体

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
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
