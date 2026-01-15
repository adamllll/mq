package org.adam.mq.mqserver.core;

import org.adam.mq.common.Consumer;
import org.adam.mq.common.ConsumerEnv;
import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.VirtualHost;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 *  通过这个类来实现消费消息的核心逻辑
 */
public class ConsumerManager {
    // 持有上层的VirtualHost引用，用来操作数据
    private VirtualHost parent;
    // 指定一个线程池，负责去执行具体的回调任务
    private ExecutorService workerPool = Executors.newFixedThreadPool(4);
    // 存放令牌的队列
    private BlockingDeque<String> tokenDeque = new LinkedBlockingDeque<>();
    // 负责扫描令牌队列的线程
    private Thread scannerThread = null;
    public ConsumerManager (VirtualHost parent) {
        this.parent = parent;
        // 启动扫描线程
        scannerThread = new Thread(() -> {
            while (true) {
                try {
                    // 1. 从令牌队列中获取一个队列名称
                    String queueName = tokenDeque.take();
                    // 2. 找到对应的队列
                    MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
                    if (queue == null) {
                        throw new MqException("[ConsumerManager] 队列 " + queueName + " 不存在，跳过消费.");
                    }
                    // 3. 调用一次就消费一条消息
                    synchronized (queue) {
                        consumerMessage(queue);
                    }
                } catch (InterruptedException | MqException e) {
                    e.printStackTrace();
                }
            }
        });
        // 把线程设为后台线程
        scannerThread.setDaemon(true);
        scannerThread.start();
    }

    // 这个方法的调用时机是发送消息的时候
    public void notifyConsume(String queueName) throws InterruptedException {
        tokenDeque.put(queueName);
    }
    // 添加一个消费者
    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        // 找到对应的队列
        MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
        if (queue == null) {
            throw new RuntimeException("[ConsumerManager] 队列 " + queueName + " 不存在！.");
        }
        // 创建一个消费者实例
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
        // 把消费者添加到队列中
        synchronized (queue) {
            queue.addConsumerEnv(consumerEnv);
            // 如果当前队列中已经有了一些消息，需要立即消费掉
            int n = parent.getMemoryDataCenter().getMessageCount(queueName);
            for (int i = 0; i < n; i++) {
                // 调用一次就消费一条消息
                consumerMessage(queue);
            }
        }
    }

    private void consumerMessage(MSGQueue queue) {
        // 1. 按照轮询的方式找到消费者
        ConsumerEnv luckyDog = queue.chooseConsumer();
        if (luckyDog == null) {
            // 当前没有消费者，直接返回,等待消费者出现
            return;
        }
        // 2. 从队列中获取一条消息
        Message message = parent.getMemoryDataCenter().pollMessage(queue.getName());
        if (message == null) {
            // 当前队列中没有消息，直接返回
            return;
        }
        // 3. 提交一个任务到线程池中去执行回调
        workerPool.submit(() -> {
            try {
                // 3.1 把消息带入消费者的回调方法中，这个操作在执行回调之前
                parent.getMemoryDataCenter().addMessageWaitAck(queue.getName(),message);
                // 3.2 执行回调操作
                luckyDog.getConsumer().handleDelivery(luckyDog.getConsumerTag(), message.getBasicProperties(), message.getBody());
                // 3.4 如果是自动确认模式，直接把消息删除了，如果是手动应答，则先不处理 交给后续消费者调用 basicAck 方法来处理
                if (luckyDog.isAutoAck()) {
                    // 1.删除硬盘上的消息
                    if (message.getDeliveryMode() == 2) {
                            parent.getDiskDataCenter().deleteMessage(queue, message);
                    }
                    // 2.删除上面的待确认集合中的消息
                    parent.getMemoryDataCenter().removeMessageWaitAck(queue.getName(), message.getMessageId());
                    // 3. 删除内存中消息中心里的消息
                    parent.getMemoryDataCenter().deleteMessage(message.getMessageId());
                    System.out.println("[ConsumerManager] 消息 " + message.getMessageId() + " 已经被消费者 " + luckyDog.getConsumerTag() + " 消费。");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
    }
}
