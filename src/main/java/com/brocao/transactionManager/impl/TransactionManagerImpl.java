package com.brocao.transactionManager.impl;

import com.brocao.transactionManager.TransactionManager;
import com.brocao.utils.Panic;
import com.brocao.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {

    /**
     * XID头文件长度：记录文件管理的事务个数
     */
    public static final int LEN_XID_HEADER_LENGTH = 8;

    /**
     * 每个事务的占用长度
     */
    private static final int XID_FIELD_SIZE = 1;

    /**
     * 事务的三种状态
     */
    private static final byte FIELD_TRAN_ACTIVE = 0;

    private static final byte FIELD_TRAN_COMMITTED = 1;

    private static final byte FIELD_TRAN_ABORTED = 2;

    /**
     * 超级事务，永远为commited状态
     */
    public static final long SUPER_XID = 0;

    /**
     * XID文件后缀
     */
    public static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private long xidCounter;
    private Lock counterLock;

    /**
     *
     * 构造方法
     * @param file 随机访问文件
     * @param fileChannel 通道
     */
    public TransactionManagerImpl(RandomAccessFile file,FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 校验XID文件是否合法
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            //连头文件都不完整
            Panic.panic(new Exception("file error!"));
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(new Exception("error file!"));
        }
    }

    //根据事务id取得其在文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }


    @Override
    public long begin() {
        //设置xidCounter + 1的事务状态为active,随后xidCount自增，更新文件头
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid,FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 自增xidCounter
     */
    private void incrXIDCounter() {
        xidCounter++;
        //更新头
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            //强制同步缓存内容到文件中
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void updateXID(long xid, byte status) {
        long xidPosition = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fileChannel.position(xidPosition);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        return checkXID(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommited(long xid) {
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        return checkXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private boolean checkXID(long xid,byte status) {
        long offset = getXidPosition(xid);
        //fileChannel.read(offset);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

}
