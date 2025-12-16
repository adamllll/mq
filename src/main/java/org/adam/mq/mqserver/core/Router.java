package org.adam.mq.mqserver.core;

import org.adam.mq.common.MqException;

/**
 * 使用这个类来实现交换机的转发规则
 * 同时也借助这个类来验证 bindingkey是否合法
 */
public class Router {
    public boolean checkBindingKey(String bindingKey) {
        // TODO
        return true;
    }

    public boolean checkRoutingKey(String routingKey) {
        // TODO
        return true;
    }
    // 判定该消息是否可以转发给这个绑定对应的队列
    public boolean route(ExchangeType exchangeType, Binding binding, Message message) throws MqException {
        // 根据不同的交换机类型，使用不同的路由规则
        if (exchangeType == ExchangeType.FANOUT) {
            // FANOUT类型的交换机，直接转发
            return true;
        }else if (exchangeType == ExchangeType.TOPIC) {
            // TOPIC交换机单独使用一个方法来实现
            return routeTopic(binding, message);
        }else {
            // 其他情况应该是不存在的
            throw new MqException("[Router] 交换机类型不存在！exchangeType: " + exchangeType);
        }
    }

    private boolean routeTopic(Binding binding, Message message) {
        return true;
    }
}
