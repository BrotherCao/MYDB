package com.brocao.dataManager.page;

import com.brocao.common.AbstractCache;
import com.brocao.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;

    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile raf;

    private FileChannel fileChannel;

    private Lock lock;

    private AtomicInteger pageNumbers;//��ǰ�򿪵�ҳ��

    public PageCacheImpl(RandomAccessFile raf,FileChannel fileChannel,int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(new Exception("mem too small"));
        }
        long length = 0;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.raf = raf;
        this.fileChannel = fileChannel;
        this.lock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length/PAGE_SIZE);
    }
    @Override
    public int newPage(byte[] initData) {
        int pgNo = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pgNo, initData, null);
        flush(page);
        return pgNo;
    }

    @Override
    public Page getPage(int pgNo) throws Exception {
        //ͨ��ҳ�������page
        return get(pgNo);
    }

    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByPgNo(int maxPgNo) {
        long size = pageOffset(maxPgNo + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }


    /**
     * ������Դ��ȡ���ݵ�����
     * @param key ��
     * @return ����ҳ
     * @throws Exception �쳣
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgNo = (int) key;
        long offset = pageOffset(pgNo);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        lock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        return new PageImpl(pgNo,buf.array(),this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    private void flush(Page pg) {
        int pgNo = pg.getPageNumber();
        long offset = pageOffset(pgNo);

        lock.lock();
        try {
            //ˢ�ش���
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            //��ͨ������δд����̵�����ǿ��д��������
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    private static long pageOffset(int pgNo) {
        //ҳ�Ŵ�1��ʼ
        return (pgNo - 1) * PAGE_SIZE;
    }
}
