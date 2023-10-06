package com.brocao.versionManager;

import com.brocao.transactionManager.impl.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * vm对事务的抽象
 */
public class Transaction {

    public long xid;
    public int level;
    public Map<Long,Boolean> snapShot;
    public Exception exception;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid,int level,Map<Long,Transaction> active) {
        Transaction transaction = new Transaction();
        transaction.xid = xid;
        transaction.level = level;
        if (level != 0) {
            transaction.snapShot = new HashMap<>();
            for (Long x : active.keySet()) {
                transaction.snapShot.put(x,true);
            }
        }
        return transaction;
    }

    public boolean isInSnapShot(long xid) {
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapShot.containsKey(xid);
    }
}
