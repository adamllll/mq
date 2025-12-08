package org.adam.mq.mqserver.datacenter;

import org.adam.mq.common.BinaryTool;
import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.core.Message;

import java.io.*;
import java.util.Scanner;

public class MessageFileManager {
    // 定义一个内部类来表示队列的统计信息
    // 优先考虑使用 static，静态内部类不依赖于外部类的实例，可以直接通过类名访问
    static public class Stat {
        // 直接定义为public，方便外部访问
        // 对于这样的简单类，就直接使用成员变量，不使用getter/setter方法
        public int totalCount; // 总消息数
        public int validCount; // 有效消息数
    }

    // 约定消息文件所在的目录和文件名
    // 这个方法用来获取指定队列对应的消息文件所在路径
    private String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }
    // 这个方法用来获取该队列的消息数据文件路径
    // 二进制文件使用txt后缀只是为了方便查看，实际存储可以使用其他格式
    private String getQueueDataPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data.txt";
    }

    // 这个方法用来获取该队列的消息统计文件路径
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "/queue_stat.txt";
    }

    private Stat readStat(String queueName) {
        // 由于当前的消息统计文件是文本文件，可以直接使用Scanner来读取文件
        Stat stat = new Stat();
        try {
            InputStream inputStream = new FileInputStream(getQueueStatPath(queueName));
            Scanner scanner = new Scanner(inputStream);
            stat.totalCount = scanner.nextInt();
            stat.validCount = scanner.nextInt();
            return  stat;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeStat(String queueName, Stat stat) {
        // 使用 PrintWriter 来写入文本文件
        // OutputStream打开文件，默认情况下会把原有内容覆盖掉，此时就相当于重写
        try {
            OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName));
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalCount + "\t" + stat.validCount);
            printWriter.flush();
            printWriter.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建队列对应的文件和目录
    public void createQueueFiles(String queueName) throws IOException {
        // 1. 先创建队列对应的消息目录
        File baseDir = new File(getQueueDir(queueName));
        if (!baseDir.exists()) {
            // 不存在就创建目录
           boolean ok =  baseDir.mkdirs();
           if (!ok) {
               throw new IOException("[MessageFileManager] 创建目录失败: " + baseDir.getAbsolutePath());
           }
        }
        // 2. 创建消息数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));
        if (!queueDataFile.exists()) {
           boolean ok = queueDataFile.createNewFile();
              if (!ok) {
                throw new IOException("[MessageFileManager] 创建文件失败: " + queueDataFile.getAbsolutePath());
              }
        }
        // 3. 创建消息统计文件
        File queueStatFile = new File(getQueueStatPath(queueName));
        if (!queueStatFile.exists()) {
            boolean ok = queueStatFile.createNewFile();
            if (!ok) {
                throw new IOException("[MessageFileManager]创建文件失败: " + queueStatFile.getAbsolutePath());
            }
        }
        // 4. 初始化统计信息,设定初始值,0\t0
        Stat stat = new Stat();
        stat.totalCount = 0;
        stat.validCount = 0;
        writeStat(queueName, stat);
    }

    // 删除队列对应的文件和目录
    // 队列也是可以删除的，当队列删除之后，对应的消息文件和目录也应该删除掉
    public void destroyQueueFiles(String queueName) throws IOException {
        // 1. 删除消息数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));
        boolean ok1 = queueDataFile.delete();
        // 2. 删除消息统计文件
        File queueStatFile = new File(getQueueStatPath(queueName));
        boolean ok2 = queueStatFile.delete();
        // 3. 删除队列目录
        File baseDir = new File(getQueueDir(queueName));
        boolean ok3 = baseDir.delete();
        if (!ok1 || !ok2 || !ok3) {
            // 有任意一个删除失败，就打印日志
            throw new IOException("[MessageFileManager] 删除队列文件失败: " + queueName);
        }
    }

    // 检查队列的目录和文件是否存在
    // 比如后续有生产者给 broker server 生产消息了，这个消息就可能需要记录到文件上(取决于消息是否需要持久化)
    public boolean checkFilesExists(String queueName) {
        // 文件存在隐含目录存在
        // 如果 queue_data.txt 和 queue_stat.txt 两个文件都存在,那么它们所在的目录必然存在
        // 文件不可能存在于一个不存在的目录中
        // 只检查消息数据文件和消息统计文件是否存在
        File queueDataFile = new File(getQueueDataPath(queueName));
        if (!queueDataFile.exists()) {
            return false;
        }
        File queueStatFile = new File(getQueueStatPath(queueName));
        if (!queueStatFile.exists()) {
            return false;
        }
        return true;
    }
    // 这个方法用来把一个新的消息放入到指定队列的消息文件中
    // queuem: 消息队列对象(要把消息写入的队列), message: 要存储的消息对象(要写的消息)
    public void sendMessage(MSGQueue queuem, Message message) throws MqException,IOException {
        // 1.检查一下当前要写入的队列对应的文件是否存在
        if (!checkFilesExists(queuem.getName())) {
            throw new MqException("[MessageFileManager] 消息队列对应的文件不存在！queueName= " + queuem.getName());
        }

        // 2.把 Message 对象进行序列化，转成二进制的字节数组
        byte[] messageBinary = BinaryTool.toBytes(message);

        // 3.先获取当前队列数据文件的长度，用来计算新消息的写入位置(计算出该 Message的offsetBeg和offsetEnd)
        // 把新的 Message数据，写入到队列数据文件的末尾，此时Message对象的offsetBeg就是文件当前的长度 + 4
        // offsetEnd就是当前文件长度 + 4 + messageBinary.length(message自身长度)
    }

}
