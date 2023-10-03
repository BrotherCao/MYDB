package com.brocao.dataManager.log;

import com.brocao.utils.Panic;
import com.brocao.utils.Parser;
import com.google.common.primitives.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ��־�ļ���д
 *
 * ��־�ļ���׼��ʽΪ��
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum Ϊ����������־�����Checksum��int����,4�ֽ�
 *
 * ÿ����ȷ��־�ĸ�ʽΪ��
 * [Size] [Checksum] [Data]
 * Size 4�ֽ�int ��ʶData����
 * Checksum 4�ֽ�int
 */
public class LoggerImpl implements Logger{

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private Lock lock;

    private long position;//��ǰ��־ָ���λ��
    private long fileSize;//
    private int xChecksum;//У���

    public LoggerImpl(RandomAccessFile raf, FileChannel fileChannel, int xChecksum) {
        this.raf = raf;
        this.fileChannel = fileChannel;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fileChannel) {
        this.raf = raf;
        this.fileChannel = fileChannel;
        lock = new ReentrantLock();
    }


    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fileChannel.position(fileChannel.size());
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum,log);
        try {
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size,checksum,data);
    }

    private int calChecksum(int xCheck, byte[] data) {
        for (byte b : data) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * �ض���־�ļ���Ϊ������С
     * @param x �ضϺ����־�ļ���С
     * @throws Exception �쳣
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fileChannel.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log,OF_DATA,log.length);
        } finally {
            lock.unlock();
        }
    }

    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fileChannel.position(position);
            fileChannel.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());//��һ����־�Ĵ�С
        if (position + size + OF_DATA > fileSize) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fileChannel.position(position);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checksum1 = calChecksum(0,Arrays.copyOfRange(log,OF_DATA,log.length));
        int checksum2 = Parser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA));
        if (checksum1 != checksum2) {
            return null;
        }

        position += log.length;
        return log;
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            raf.close();;
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public void init() {
        long size = 0;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(new Exception("log file error"));//У��;�4���ֽڲ��ܱ������С����
        }
        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(buf.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;


        checkAndRemoveTail();
    }

    //��鲢�Ƴ�bad tail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null)break;//ûд�����־ͨ������У��ͣ��᷵��null
            xCheck = calChecksum(xCheck,log);
        }
        if (xCheck != xChecksum) {
            Panic.panic(new Exception("bad log file!"));
        }
        try {
            truncate(position);//�����ȡ
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            raf.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        rewind();
    }
}
