package com.brocao.versionManager;

import com.brocao.transactionManager.TransactionManager;

public class Visibility {

    //�汾��Ծ�ļ��
    public static boolean isVersionSkip(TransactionManager tm,Transaction transaction,Entry entry) {
        long xMax = entry.getXMax();
        if (transaction.level == 0) {
            return false;
        } else {
            return tm.isCommited(xMax) && (xMax > transaction.xid || transaction.isInSnapShot(xMax));
        }
    }

    public static boolean isVisible(TransactionManager tm,Transaction transaction,Entry entry) {
        if (transaction.level == 0) {
            return readCommitted(tm,transaction,entry);
        } else {
            return repeatableRead(tm,transaction,entry);
        }
    }
    private static boolean readCommitted(TransactionManager tm,Transaction transaction,Entry entry) {
        long xid = transaction.xid;
        long xMin = entry.getXMin();
        long xMax = entry.getXMax();
        if (xMin == xid && xMax == 0)return true;//�ɼ�

        if (tm.isCommited(xMin)) {
            if (xMax == 0) return true;
            if (xMax != xid) {
                if (!tm.isCommited(xMax)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm,Transaction transaction,Entry entry) {
        long xid = transaction.xid;
        long xMin = entry.getXMin();
        long xMax = entry.getXMax();
        if (xMin == xid && xMax == 0)return true;//��¼Ϊ��ǰ���񴴽�

        if (tm.isCommited(xMin) && xMin < xid && !transaction.isInSnapShot(xMin)) {
            if (xMax == 0) return true;
            if (xMax != xid) {
                if (!tm.isCommited(xMax) || xMax > xid || transaction.isInSnapShot(xMax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
