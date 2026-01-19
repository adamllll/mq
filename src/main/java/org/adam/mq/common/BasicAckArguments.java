package org.adam.mq.common;

import java.io.Serializable;

public class BasicAckArguments extends BasicArguments implements Serializable {
    private String queueName; // 队列名称
    private String messageId; // 消息ID

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}
