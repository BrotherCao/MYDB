package com.brocao.transactionManager;

import com.brocao.transactionManager.impl.TransactionManagerImpl;
import com.brocao.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 事务管理器接口
 */
public interface TransactionManager {
    /**
     * 开启一个新事务
     */
    long begin();

    /**
     * 提交一个新事务
     */
    void commit(long xid);

    /**
     * 取消一个事务
     */
    void abort(long xid);

    /**
     * 查询一个事务的状态是否还在进行
     * @param xid 事务id
     * @return true//false
     */
    boolean isActive(long xid);

    /**
     * 查询一个事务的状态是否为已提交
     * @param xid 事务id
     * @return 布尔值
     */
    boolean isCommited(long xid);

    /**
     * 查询一个事务的状态是否是已取消
     * @param xid 事务id
     * @return true/false
     */
    boolean isAborted(long xid);

    /**
     * 关闭TM
     */
    void close();

    /**
     * 创建xid文件
     * @param path 路径
     * @return tm管理器
     */
    public static TransactionManager create(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(new Exception("error file has already exist!"));
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(new Exception("error,file cannot rw!"));
        }

        FileChannel fileChannel = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            fileChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        //写空文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf,fileChannel);
    }

    public static TransactionManager open(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!file.exists()) {
            Panic.panic(new Exception("file not exists!"));
        }

        if (!file.canWrite() || !file.canRead()) {
            Panic.panic(new Exception("file cannot rw!"));
        }
        FileChannel fileChannel = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fileChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf,fileChannel);
    }
}
