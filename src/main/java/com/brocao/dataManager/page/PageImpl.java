package com.brocao.dataManager.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page{
    private int pageNumber;//页号

    private byte[] data;//数据

    private boolean dirty;//页面是否为脏页面

    private Lock lock;

    private PageCache pc;

    public PageImpl(int pgNo, byte[] array, PageCache pageCache) {
        this.pageNumber = pgNo;
        this.data = array;
        this.pc = pageCache;
        this.lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unLock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return this.dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
