package org.adam.mq.mqserver.core;
/**
 * 这个类表示一个绑定（Binding），用于将交换机与消息队列关联起来，以便消息能够根据路由规则传递到正确的队列。
 */
public class Binding {
    private String exchangeName; // 交换机名称
    private String queueName;    // 队列名称
    // bindingKey 可以理解为路由规则，用于指定消息如何从交换机路由到队列
    private String bindingKey;   // 路由键，依附于Exchange和Queue之间的路由规则
    // 比如，对于持久化来说，如果Exchange和Queue任何一个都没有持久化，那么此时的Binding也不应该是持久化的


    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}
