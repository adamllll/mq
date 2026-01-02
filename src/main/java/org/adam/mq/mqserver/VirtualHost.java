package org.adam.mq.mqserver;

import org.adam.mq.common.Consumer;
import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.*;
import org.adam.mq.mqserver.datacenter.DiskDataCenter;
import org.adam.mq.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通过这个类来表示虚拟主机
 * 每个虚拟主机下面都管理着自己的交换机、队列、绑定关系、消息、数据
 * 同时提供api调用
 * 真对VittualHost这个类作为业务逻辑的整合者，需要对代码中抛出的异常进行处理
 */
public class VirtualHost {
    private String virtualHostName;
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    private Router router = new Router();
    private ConsumerManager consumerManager = new ConsumerManager(this);

    // 操作交换机的锁对象
    private final Object exchangeLocker = new Object();
    // 操作队列的锁对象
    private final Object queueLocker = new Object();

    public String getVirtualHostName() {
        return virtualHostName;
    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }

    public VirtualHost(String name) {
        this.virtualHostName = name;
        // 对于 MenoryDataCenter来说并不需要额外的初始化操作，只要把对象new出来就行了
        // 对于 DiskDataCenter 来说需要进行数据加载操作，建库建表和初始数据的设定
        diskDataCenter.init();
        // 另外还需要针对硬盘的数据，进行恢复到内存中
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | ClassNotFoundException e ) {
            System.out.println("虚拟主机 " + virtualHostName + " 在恢复数据时发生异常：" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // 创建交换机
    // 如果交换机不存在就创建，如果存在就返回
    // 返回true表示创建了新的交换机，返回false表示交换机创建失败
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete,
                                   Map<String, Object> arguments) {
        // 把交换机的名字加上虚拟主机作为前缀
        exchangeName = virtualHostName + "-" + exchangeName;
        try {
            synchronized (exchangeLocker) {
                // 1. 判定改交换机是否已经存在,通过内存查询
                Exchange existsExchange = memoryDataCenter.getExchange(exchangeName);
                if (existsExchange != null) {
                    // 交换机已经存在，直接返回
                    System.out.println("[VirtualHost] 交换机 " + exchangeName + " 已经存在，无法创建新的交换机");
                    return true;
                }
                // 2. 交换机不存在，创建新的交换机对象
                Exchange exchange = new Exchange();
                exchange.setName("exchangeName");
                exchange.setType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);
                // 3. 把交换机对象插入到数据库(硬盘)中
                if (durable) {
                    diskDataCenter.insertExchange(exchange);
                }
                // 4. 把交换机对象插入到内存中
                memoryDataCenter.insertExchange(exchange);
                System.out.println("[VirtualHost] 交换机 " + exchangeName + " 创建成功");
                // 上述逻辑先写硬盘再写内存，防止写内存成功后写硬盘失败导致数据不一致的问题(因为硬盘更容易写失败，如果硬盘写失败了内存就不写了)
                // 要是先写内存，内存写成功了，硬盘写失败了，就会导致内存和硬盘数据不一致的问题(还需要把硬盘的数据删除掉太麻烦~)
                return true;
            }
        } catch (Exception e) {
            System.out.println("[VirtualHost] 交换机 " + exchangeName + " 创建失败，发生异常：" + e.getMessage());
            return false;
        }
    }
    // 删除交换机
    public boolean exchangeDelete(String exchangeName) {
        // 把交换机的名字加上虚拟主机作为前缀
        exchangeName = virtualHostName + "-" + exchangeName;
        try {
            synchronized (exchangeLocker) {
                // 1. 先找到对应的交换机
                Exchange toDelete = memoryDataCenter.getExchange(exchangeName);
                if (toDelete == null) {
                    throw new MqException("[VirtualHost] 交换机 " + exchangeName + " 不存在，无法删除");
                }
                // 2. 删除硬盘上的交换机数据
                if (toDelete.isDurable()) {
                    diskDataCenter.deleteExchange(exchangeName);
                }
                // 3. 删除内存中的交换机数据
                memoryDataCenter.deleteExchange(exchangeName);
                System.out.println("[VirtualHost] 交换机 " + exchangeName + " 删除成功");
                return true;
            }
        }catch (Exception e) {
            System.out.println("[VirtualHost] 交换机 " + exchangeName + " 删除失败，发生异常：" + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // 创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete,
                                Map<String, Object> arguments) {
        // 把队列的名字加上虚拟主机作为前缀
        queueName = virtualHostName + "-" + queueName;
        try {
            synchronized (queueLocker) {
                // 1. 判定改队列是否已经存在,通过内存查询
                MSGQueue existsQueue = memoryDataCenter.getQueue(queueName);
                if (existsQueue != null) {
                    // 队列已经存在，直接返回
                    System.out.println("[VirtualHost] 队列 " + queueName + " 已经存在，无法创建新的队列");
                    return true;
                }
                // 2. 队列不存在，创建新的队列对象
                MSGQueue queue = new MSGQueue();
                queue.setName(queueName);
                queue.setDurable(durable);
                queue.setExclusive(exclusive);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);
                // 3. 把队列对象插入到数据库(硬盘)中
                if (durable) {
                    diskDataCenter.insertQueue(queue);
                }
                // 4. 把队列对象插入到内存中
                memoryDataCenter.insertQueue(queue);
                System.out.println("[VirtualHost] 队列 " + queueName + " 创建成功");
                return true;
            }
        } catch (Exception e) {
            System.out.println("[VirtualHost] 队列 " + queueName + " 创建失败，发生异常：" + e.getMessage());
            return false;
        }
    }
    // 删除队列
    public boolean queueDelete(String queueName) {
        // 把队列的名字加上虚拟主机作为前缀
        queueName = virtualHostName + "-" + queueName;
        try {
            synchronized (queueLocker) {
                // 1. 先找到对应的队列
                MSGQueue toDelete = memoryDataCenter.getQueue(queueName);
                if (toDelete == null) {
                    throw new MqException("[VirtualHost] 队列 " + queueName + " 不存在，无法删除");
                }
                // 2. 删除硬盘上的队列数据
                if (toDelete.isDurable()) {
                    diskDataCenter.deleteQueue(queueName);
                }
                // 3. 删除内存中的队列数据
                memoryDataCenter.deleteQueue(queueName);
                System.out.println("[VirtualHost] 队列 " + queueName + " 删除成功");
                return true;
            }
        } catch (Exception e) {
            System.out.println("[VirtualHost] 队列 " + queueName + " 删除失败，发生异常：" + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // 创建绑定
    public boolean queueBind(String queueName, String exchangeName, String bindingKey) {
        queueName = virtualHostName + "-" + queueName;
        exchangeName = virtualHostName + "-" + exchangeName;
        try {
            synchronized (exchangeLocker) {
                synchronized (queueLocker)  {
                    // 1. 判定绑定是否已经存在
                    Binding exisistsBinding = memoryDataCenter.getBinding(queueName, exchangeName);
                    if (exisistsBinding != null) {
                        // 绑定已经存在，直接返回
                        System.out.println("[VirtualHost] binding 已经存在！ queueName=" + queueName + ", exchangeName=" + exchangeName);
                        return true;
                    }
                    // 2. 验证 bindingKey 是否合法
                    if (!router.checkBindingKey(bindingKey)) {
                        throw new MqException("[VirtualHost] bindingKey "+ bindingKey +" 不合法，无法创建绑定关系");
                    }
                    // 3. 创建绑定对象
                    Binding binding = new Binding();
                    binding.setQueueName(queueName);
                    binding.setExchangeName(exchangeName);
                    binding.setBindingKey(bindingKey);
                    // 4. 获取一下对应的交换机和队列，如果不存在是无法创建绑定的
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    if (queue == null) {
                        throw new MqException("[VirtualHost] 绑定失败，队列 " + queueName + " 不存在");
                    }
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    if (exchange == null) {
                        throw new MqException("[VirtualHost] 绑定失败，交换机 " + exchangeName + " 不存在");
                    }
                    // 5. 把绑定对象插入到数据库(硬盘)中
                    if (queue.isDurable() && exchange.isDurable()) {
                        diskDataCenter.insertBinding(binding);
                    }
                    // 6. 把绑定对象插入到内存中
                    memoryDataCenter.insertBinding(binding);
                    System.out.println("[VirtualHost] binding 创建成功！ queueName=" + queueName + ", exchangeName=" + exchangeName);
                    return true;
                }
            }
        }catch (Exception e) {
            System.out.println("[VirtualHost] binding 创建失败，发生异常：" + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    // 删除绑定
    public boolean queueUnbind(String queueName, String exchangeName) {
        queueName = virtualHostName + "-" + queueName;
        exchangeName = virtualHostName + "-" + exchangeName;
        try {
            synchronized (exchangeLocker) {
                synchronized (queueLocker) {
                    // 1. 先找到对应的绑定,获取binding是否已经存在
                    Binding toDelete = memoryDataCenter.getBinding(queueName, exchangeName);
                    if (toDelete == null) {
                        throw new MqException("[VirtualHost] binding 不存在，无法删除！ queueName=" + queueName + ", exchangeName=" + exchangeName);
                    }
                    // 2. 无论绑定是否持久化，都尝试从硬盘上删除，就算不存在，这个删除也不会报错(待优化)
                    diskDataCenter.deleteBinding(toDelete);
                    // 3. 删除内存中的绑定数据
                    memoryDataCenter.deleteBinding(toDelete);
                    System.out.println("[VirtualHost] binding 删除成功！ queueName=" + queueName + ", exchangeName=" + exchangeName);
                    return true;
                }
            }
        } catch (Exception e) {
            System.out.println("[VirtualHost] binding 删除失败，发生异常：" + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // 发送消息到指定的交换机中
    public  boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) {
        try {
            // 1. 转换交换机的名字
            exchangeName = virtualHostName + "-" + exchangeName;
            // 2. 检查 routingKey是否合法
            if (!router.checkRoutingKey(routingKey)) {
                throw new MqException("[VirtualHost] routingKey "+ routingKey +" 不合法，无法发送消息");
            }
            // 3. 查找交换机对象
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if (exchange == null) {
                throw new MqException("[VirtualHost] 交换机 " + exchangeName + " 不存在，无法发送消息");
            }
            // 4. 判定交换机的类型，并进行相应的路由转发
            if (exchange.getType() == ExchangeType.DIRECT) {
                // 按照直接交换机的方式来转发消息
                // 以 routingkey 作为队列的名字，直接把消息写入指定的队列中,可以无视绑定关系
                String queueName = virtualHostName + "-" + routingKey;
                // 4.1. 构造消息对象
                Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                // 4.2. 查找该队列名对应的对象
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if (queue == null) {
                    throw new MqException("[VirtualHost] 目标队列 " + queueName + " 不存在，无法发送消息");
                }
                // 4.3 队列存在，直接给队列中写入消息
                sendMessage(queue, message);
            }else {
                // 按照 fanout 和 topic 交换机的方式来转发消息
                // 5.  找到该交换机关联的所有绑定，并遍历这些绑定对象
                ConcurrentHashMap<String, Binding> bindingsMap = memoryDataCenter.getBindings(exchangeName);
                for (Map.Entry<String, Binding> entry : bindingsMap.entrySet()) {
                    // 5.1 获取绑定对象，判定对应的队列是否存在
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(binding.getQueueName());
                    if (queue == null) {
                        // 此处不抛出异常，此处可能有多个队列，某个队列不存在不影响其他队列接收消息
                        // 继续下一个队列避免阻塞
                        System.out.println("[VirtualHost] 发送消息目标队列 " + binding.getQueueName() + " 不存在，无法发送消息，跳过该队列");
                        continue;
                    }
                    // 5.2 构造消息对象
                    Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                    // 5.3 判定这消息是否能转发给该队列
                    // 如果是fanout交换机，所有绑定的队列都需要转发
                    // 如果是topic交换机，需要判定 routingkey 和 bindingkey 是否匹配
                    if (!router.route(exchange.getType(), binding, message)) {
                        continue;
                    }
                    // 5.4 真正转发消息给队列
                    sendMessage(queue, message);
                }
            }
            return true;
        }catch (Exception e) {
            System.out.println("[VirtualHost] 消息发送失败，发生异常：" + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void sendMessage(MSGQueue queue, Message message) throws IOException, MqException, InterruptedException {
        // 此处发送消息，就是把消息写入到 硬盘 和 内存 上
        int deliverMode = message.getDeliveryMode();
        // deliverMode 为1不持久化，为2持久化
        if (deliverMode == 2 && queue.isDurable()) {
            // 持久化消息，需要写入硬盘
            diskDataCenter.sendMessage(queue, message);
        }
        // 写入内存
        memoryDataCenter.sendMessage(queue, message);
        System.out.println("[VirtualHost] 消息发送成功，消息ID=" + message.getMessageId() + "，目标队列=" + queue.getName());
        // 此处还行需要补充一个逻辑，通知消费者可以消费消息了
        consumerManager.notifyConsume(queue.getName());
    }

    // 订阅消息
    // 添加一个队列的订阅者，当队列收到消息之后，通知该订阅者消费消息
    // 这里的 consumerTag 可以理解为订阅者的标识符
    // autoAck 消息被消费完毕后，是否自动发送确认回执
    // consumer 是一个回调函数，此处类型设定成函数式结构，后续调用 basicconsume 的时候传入一个 lambda 表达式即可
    public boolean basicConsume(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        // 构造一个 ConsumerEnv对象，把这个对于的小烈找到，再把这个Consumer对象添加到队列中
        queueName = virtualHostName + "-" + queueName;
        try {
            consumerManager.addConsumer(consumerTag, queueName, autoAck, consumer);
            System.out.println("[VirtualHost] 消费者 basicConsumer" + consumerTag + " 订阅队列 " + queueName + " 成功");
            return true;
        }catch (Exception e) {
            System.out.println("[VirtualHost] 消费者 basicConsumer " + consumerTag + " 订阅队列 " + queueName + " 失败，发生异常：" + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}
