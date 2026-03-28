package org.adam.mq.mqserver.core;

import org.adam.mq.common.MqException;

/**
 * 用于实现交换机转发规则，并校验 bindingKey / routingKey 的合法性。
 */
public class Router {
    // bindingKey 规则：
    // 1. 允许字母、数字、下划线
    // 2. 使用 . 分割成多个 token
    // 3. 仅支持独立 token 形式的 * 和 #
    public boolean checkBindingKey(String bindingKey) {
        if (bindingKey.length() == 0) {
            // DIRECT / FANOUT 场景下 bindingKey 可以为空
            return true;
        }

        for (int i = 0; i < bindingKey.length(); i++) {
            char ch = bindingKey.charAt(i);
            if (ch >= 'A' && ch <= 'Z') {
                continue;
            }
            if (ch >= 'a' && ch <= 'z') {
                continue;
            }
            if (ch >= '0' && ch <= '9') {
                continue;
            }
            if (ch == '_' || ch == '.' || ch == '*' || ch == '#') {
                continue;
            }
            return false;
        }

        String[] words = bindingKey.split("\\.");
        for (String word : words) {
            // * / # 只能作为独立 token 出现，例如 aaa*bbb 或 #suffix 都应视为非法
            if (word.length() > 1 && (word.contains("*") || word.contains("#"))) {
                return false;
            }
        }

        // 限制相邻通配符组合，避免引入当前实现未支持的复杂匹配语义
        for (int i = 0; i < words.length - 1; i++) {
            if (words[i].equals("#") && words[i + 1].equals("#")) {
                return false;
            }
            if (words[i].equals("#") && words[i + 1].equals("*")) {
                return false;
            }
            if (words[i].equals("*") && words[i + 1].equals("#")) {
                return false;
            }
        }

        return true;
    }

    // routingKey 规则：
    // 1. 允许字母、数字、下划线
    // 2. 使用 . 分割成多个 token
    public boolean checkRoutingKey(String routingKey) {
        if (routingKey.length() == 0) {
            // FANOUT 场景下 routingKey 可以为空
            return true;
        }

        for (int i = 0; i < routingKey.length(); i++) {
            char ch = routingKey.charAt(i);
            if (ch >= 'A' && ch <= 'Z') {
                continue;
            }
            if (ch >= 'a' && ch <= 'z') {
                continue;
            }
            if (ch >= '0' && ch <= '9') {
                continue;
            }
            if (ch == '_' || ch == '.') {
                continue;
            }
            return false;
        }

        return true;
    }

    // 判定消息是否可以转发给当前绑定对应的队列
    public boolean route(ExchangeType exchangeType, Binding binding, Message message) throws MqException {
        if (exchangeType == ExchangeType.FANOUT) {
            return true;
        } else if (exchangeType == ExchangeType.TOPIC) {
            return routeTopic(binding, message);
        } else {
            throw new MqException("[Router] 交换机类型不存在，exchangeType: " + exchangeType);
        }
    }

    private boolean routeTopic(Binding binding, Message message) {
        String[] bindingTokens = binding.getBindingKey().split("\\.");
        String[] routingTokens = message.getRoutingKey().split("\\.");

        int bindingIndex = 0;
        int routingIndex = 0;
        while (bindingIndex < bindingTokens.length && routingIndex < routingTokens.length) {
            if (bindingTokens[bindingIndex].equals("*")) {
                bindingIndex++;
                routingIndex++;
                continue;
            } else if (bindingTokens[bindingIndex].equals("#")) {
                bindingIndex++;
                if (bindingIndex == bindingTokens.length) {
                    return true;
                }

                routingIndex = findNextMatch(routingTokens, routingIndex, bindingTokens[bindingIndex]);
                if (routingIndex == -1) {
                    return false;
                }

                bindingIndex++;
                routingIndex++;
            } else {
                if (!bindingTokens[bindingIndex].equals(routingTokens[routingIndex])) {
                    return false;
                }
                bindingIndex++;
                routingIndex++;
            }
        }

        return bindingIndex == bindingTokens.length && routingIndex == routingTokens.length;
    }

    private int findNextMatch(String[] routingTokens, int routingIndex, String bindingToken) {
        for (int i = routingIndex; i < routingTokens.length; i++) {
            if (routingTokens[i].equals(bindingToken)) {
                return i;
            }
        }
        return -1;
    }
}
