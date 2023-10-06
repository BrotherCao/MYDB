package com.brocao.dataManager.dataItem;

import com.brocao.common.SubArray;
import com.brocao.dataManager.DataManagerImpl;
import com.brocao.dataManager.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * [ValidFlag][DataSize][Data]
 * ValidFlag 1�ֽڣ�0Ϊ�Ϸ���1Ϊ�Ƿ�
 * DataSize 2�ֽڣ���ʶData�ĳ���
 */
public class DataItemImpl implements DataItem{

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw,byte[] oldRaw,Page page,long uid,DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        rLock = new ReentrantLock();
        wLock = new ReentrantLock();
        this.dm = dm;
        this.uid = uid;
        this.page = page;
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw,raw.start,oldRaw,0,oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw,0,raw.raw,raw.start,oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid,this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unLock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
