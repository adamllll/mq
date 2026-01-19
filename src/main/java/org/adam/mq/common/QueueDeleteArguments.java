package org.adam.mq.common;

import java.io.Serializable;

public class QueueDeleteArguments extends BasicArguments implements Serializable {
    private String queueName; // 队列名称

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
