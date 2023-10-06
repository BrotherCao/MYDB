package com.brocao.versionManager;

import com.brocao.common.AbstractCache;
import com.brocao.dataManager.DataManager;
import com.brocao.dataManager.DataManagerImpl;
import com.brocao.transactionManager.TransactionManager;
import com.brocao.transactionManager.impl.TransactionManagerImpl;
import com.brocao.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{

    TransactionManager tm;
    DataManager dm;
    Map<Long,Transaction> activeTransaction;
    Lock lock;
    LockTable lockTable;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID,Transaction.newTransaction(TransactionManagerImpl.SUPER_XID,0,null));
        this.lock = new ReentrantLock();
        this.lockTable = new LockTable();
    }
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        if (transaction.exception != null) {
            throw transaction.exception;
        }
        Entry entry = super.get(uid);
        try {
            if (Visibility.isVisible(tm,transaction,entry)) {
                return entry.data();
            } else {
                return null;//
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        if (transaction.exception != null) {
            throw transaction.exception;
        }
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid,raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        if (transaction.exception != null) {
            throw transaction.exception;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            throw e;
        }
        try {
            if (!Visibility.isVisible(tm,transaction,entry)) {
                return false;
            }
            Lock lock1 = null;
            try {
                lock1 = lockTable.add(xid, uid);
            } catch (Exception e) {
                transaction.exception = new Exception("concurrentUpdateException!");
                internAbort(xid,true);
                transaction.autoAborted = true;
                throw transaction.exception;
            }

            if (lock1 != null) {
                lock1.lock();
                lock1.unlock();
            }

            if (entry.getXMax() == xid) {
                return false;
            }

            if (Visibility.isVersionSkip(tm,transaction,entry)) {
                transaction.exception = new Exception("concurrentUpdateException!");
                internAbort(xid,true);
                transaction.autoAborted = true;
                throw transaction.exception;
            }
            entry.setXMax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid,transaction);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        try {
            if (transaction.exception != null) {
                throw transaction.exception;
            }
        } catch (NullPointerException e) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();
        lockTable.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {

    }

    private void internAbort(long xid,boolean autoAborted) {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        if (transaction.autoAborted) {
            return;
        }
        lockTable.remove(xid);
        tm.abort(xid);
    }

    @Override
    protected Entry getForCache(long key) throws Exception {
        Entry entry = Entry.loadEntry(this, key);
        if (entry == null) {
            throw new Exception("null entry!");
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry obj) {
        obj.remove();
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
