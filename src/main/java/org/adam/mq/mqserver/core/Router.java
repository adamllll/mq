package org.adam.mq.mqserver.core;

import org.adam.mq.common.MqException;

/**
 * 使用这个类来实现交换机的转发规则
 * 同时也借助这个类来验证 bindingkey是否合法
 */
public class Router {
    // bindingkey 构造规则
    // 1. 数字，字母，下划线 2. 使用 . 分割成若干部分 3. 支持通配符 * 和 #
    public boolean checkBindingKey(String bindingKey) {
        if (bindingKey.length() == 0) {
            // 空字符串也是合法情况，在使用DIRCT / FANOUT交换机的时候，bindingkey用不上可以为空
            return true;
        }
        // 检查字符串不能存在非法字符
        for (int i = 0; i < bindingKey.length(); i++) {
            char ch = bindingKey.charAt(i);
            // 判定该字符是否是大写字母
            if (ch >= 'A' && ch <= 'Z') {
                continue;
            }
            // 判定该字符是否是小写字母
            if (ch >= 'a' && ch <= 'z') {
                continue;
            }
            // 判定该字符是否是数字
            if (ch >= '0' && ch <= '9') {
                continue;
            }
            // 判定该字符是否是 _ . * #
            if (ch == '_' || ch == '.' || ch == '*' || ch == '#') {
                continue;
            }
            // 该字符不是上述任何一种合法情况，直接返回false
            return false;
        }
        // 检查 * 和 # 是否是独立的部分
        // aaa.*.bbb 是合法的，aaa*bbb是不合法的
        String[] words = bindingKey.split("\\.");
        for (String word : words) {
            if (word.length() == 0) {
                // 检查 word 长度 > 1并且包含了 * 或 #。就是非法格式
                if (word.length() > 1 && (word.contains("*") || word.contains("#"))) {
                    return false;
                }
            }
        }
        // 约定一下，通配符之间的相邻关系(人为(暂时)约定的)
        // 前三种实现匹配的逻辑比较繁琐而且功能性提升不大，所以直接禁止使用
        // 1. aaa.#.#.bbb 非法
        // 2. aaa.#.*.bbb 非法
        // 3. aaa.*.#.bbb 非法
        // 4. aaa.*.*.bbb 合法
        for (int i = 0; i < words.length - 1; i++) {
            // 判定是否是连续两个 #
            if (words[i].equals("#") && words[i + 1].equals("#")) {
                return false;
            }
            // 判定是否是 # 和 * 相邻
            if (words[i].equals("#") && words[i + 1].equals("*")) {
                return false;
            }
            // 判定是否是 * 和 # 相邻
            if (words[i].equals("*") && words[i + 1].equals("#")) {
                return false;
            }
        }
        // 把每个字符都检查过了，都是合法的，返回true
        return true;
    }
    // routingkey 构造规则
    // 1. 数字，字母，下划线 2. 使用 . 分割成若干部分
    public boolean checkRoutingKey(String routingKey) {
        if (routingKey.length() == 0) {
            // 空字符串合法的，比如在使用fanout交换机的时候，routingkey用不上可以为空
            return true;
        }
        for (int i = 0; i < routingKey.length(); i++) {
            char ch = routingKey.charAt(i);
            // 判定该字符是否是大写字母
            if (ch >= 'A' && ch <= 'Z') {
                continue;
            }
            // 判定该字符是否是小写字母
            if (ch >= 'a' && ch <= 'z') {
                continue;
            }
            // 判定该字符是否是数字
            if (ch >= '0' && ch <= '9') {
                continue;
            }
            // 判定该字符是否是 _ 或 .
            if (ch == '_' || ch == '.') {
                continue;
            }
            // 该字符不是上述任何一种合法情况，直接返回false
            return false;
        }
        // 把每个字符都检查过了，都是合法的，返回true
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
        // 先把这两个key进行切分
        String[] bindingTokens = binding.getBindingKey().split("\\.");
        String[] routingTokens = message.getRoutingKey().split("\\.");
        // 引入两个下标，指向上述两个数组，初始情况下都为0
        int bindingIdex = 0;
        int routingIdex = 0;
        // 此处使用while更合适，每次循环下标不一定就是+1
        while (bindingIdex < routingTokens.length && routingIdex < routingTokens.length) {
            if (bindingTokens[bindingIdex].equals("*")) {
                // 遇到*直接进入下一个，表示匹配任意单词
                bindingIdex++;
                routingIdex++;
                continue;
            }else if (bindingTokens[bindingIdex].equals("#")) {
                // 遇到#，需要先判定有没有下一个位置
                bindingIdex++;
                if (bindingIdex == bindingTokens.length) {
                    // #在最后一个位置，表示匹配剩余所有单词，直接返回true
                    return true;
                }
                // #不是最后一个位置，需要继续匹配下一个单词
                // findNextMatch方法用于在routingTokens中找到下一个和bindingTokens[bindingIdex]匹配的位置,没找到返回-1
                routingIdex = findNextMatch(routingTokens, routingIdex, bindingTokens, bindingIdex);
                if (routingIdex == -1) {
                    // 没有找到匹配的位置，直接返回false
                    return false;
                }
                // 找到了匹配的位置，继续进行下一轮匹配
                bindingIdex++;
                routingIdex++;
            }else {
                // 普通字符串，要求两边的内容是一样的
                if (!bindingTokens[bindingIdex].equals(routingTokens[routingIdex])) {
                    return false;
                }
                bindingIdex++;
                routingIdex++;
            }
        }
        return true;
    }

    private int findNextMatch(String[] routingTokens, int routingIdex, String[] bindingTokens, int bindingIdex) {

    }
}
