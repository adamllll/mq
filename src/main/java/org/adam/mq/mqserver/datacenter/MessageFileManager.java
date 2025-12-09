package org.adam.mq.mqserver.datacenter;

import org.adam.mq.common.BinaryTool;
import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.core.Message;

import java.io.*;
import java.util.Random;
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
    // queue: 消息队列对象(要把消息写入的队列), message: 要存储的消息对象(要写的消息)
    public void sendMessage(MSGQueue queue, Message message) throws MqException,IOException {
        // 1.检查一下当前要写入的队列对应的文件是否存在
        if (!checkFilesExists(queue.getName())) {
            throw new MqException("[MessageFileManager] 消息队列对应的文件不存在！queueName= " + queue.getName());
        }

        // 2.把 Message 对象进行序列化，转成二进制的字节数组
        byte[] messageBinary = BinaryTool.toBytes(message);

        // 由于可能有多个线程同时往同一个队列写消息，所以这里需要进行同步处理
        synchronized (queue) {
            // 3.先获取当前队列数据文件的长度，用来计算新消息的写入位置(计算出该 Message的offsetBeg和offsetEnd)
            // 把新的 Message数据，写入到队列数据文件的末尾，此时Message对象的offsetBeg就是文件当前的长度 + 4
            // offsetEnd就是当前文件长度 + 4 + messageBinary.length(message自身长度)
            File queueDataFile = new File(getQueueDataPath(queue.getName()));
            // 通过这个方法就能queueDataFile.length()就能获取到文件长度，单位字节
            message.setOffsetBeg(queueDataFile.length() + 4); // +4是因为还要存储消息长度的4个字节
            message.setOffsetEnd(queueDataFile.length() + 4 + messageBinary.length);

            // 4.写入消息到文件中,追加写入到文件末尾
            try (OutputStream outputStream = new FileOutputStream(queueDataFile, true)) {
                // 先写入消息的长度(4个字节)
                // 在流对象中写入一个int类型的数据，需要把int转换成4个字节的byte数组(把int的四个字节分别取出来，一个一个字节的写)
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    dataOutputStream.writeInt(messageBinary.length);
                    // 再写入消息的二进制数据(消息本体)
                    dataOutputStream.write(messageBinary);
                }
            }

            // 5.更新消息统计文件
            Stat stat = readStat(queue.getName());
            stat.totalCount += 1;
            stat.validCount += 1;
            writeStat(queue.getName(), stat);
        }
    }
    // 这个方法用来删除指定队列中的指定消息,也就是把硬盘上存储这个数据的 isValid 标记改为 0
    // 此处参数中的 Message 对象，必须包含 offsetBeg 和 offsetEnd 两个属性
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {
        // 1.先把文件中的这一段数据，都取出来，返回原 Message对象
        // 2. 把 isValid 标记改为 0
        // 3. 把修改后的 Message 对象重新写回到文件的原位置
        synchronized (queue) { // 同步，防止多个线程同时修改同一个文件
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueStatPath(queue.getName()), "rw")) {
                // 1. 先从文件中读取对应的 Message 对象
                byte[] bufferSrc = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                randomAccessFile.seek(message.getOffsetBeg());
                randomAccessFile.read(bufferSrc); // 读取原始数据到bufferSrc中
                // 2. 把当前读出来的二进制数据转换为 Message 对象
                Message diskMessage = (Message) BinaryTool.fromBytes(bufferSrc);
                // 3. 把 isValid 标记改为 0
                diskMessage.setIsValid((byte) 0x0);
                //此处不需要给参数的这个 message的isValid设为0，因为这个参数代表的是内存中管理的Message对象
                // 而这个对象的isValid并不影响文件中的数据，文件中的数据才是最终的存储结果(马上要被从内存中销毁了)
                // 4. 把修改后的 Message 对象重新写回到文件的原位置
                byte[] bufferDest = BinaryTool.toBytes(diskMessage); // 把修改后的 Message 对象转换为二进制数据
                // 虽然上面的操作已经seek过了，但是上面的seek操作之后进行了读操作，读操作会改变文件指针的位置
                // 所以这里还是需要重新seek一下
                randomAccessFile.seek(message.getOffsetBeg()); // 定位到原来消息的起始位置
                randomAccessFile.write(bufferDest.length); // 先写入消息长度
                // 通过上述的操作，对于文件来说，只是有一个字节发生了改变
            }
            // 5.更新消息统计文件,有效消息数减1
            Stat stat = readStat(queue.getName());
            if (stat.validCount > 0) {
                stat.validCount -= 1;
            }
            writeStat(queue.getName(), stat);
        }
    }
}
