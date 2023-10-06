package com.brocao.versionManager;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * �������
 * ͨ������ͼ������Ƿ��л����������
 */
public class LockTable {

    private Map<Long, List<Long>> x2u;//ĳ��XID�Ѿ���õ���Դ��UID�б�

    private Map<Long,Long> u2x; //UID��ĳ��XID����

    private Map<Long,List<Long>> wait; //���ڵȴ�UID��XID�б�

    private Map<Long, Lock> waitLock; //���ڵȴ���Դ��XID����

    private Map<Long,Long> waitU; //XID���ڵȴ���UID

    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }
    /**
     * ÿ����Ҫ�ȴ�ʱ��������ͼ������һ���ߣ��������������
     * ��⵽�������ͳ��������ߣ���������ӣ�������������
     * @param xid ����id
     * @param uid ��Դuid
     * @return ����Ҫ�ȴ��򷵻�null,���򷵻�������
     * @throws Exception ����������׳��쳣
     */
    public Lock add(long xid,long uid) throws Exception {
        lock.lock();
        try {
            if (isInList(x2u,xid,uid)) {
                return null;
            }
            //��Դû���κ������ȡ
            if (!u2x.containsKey(uid)) {
                u2x.put(uid,xid);
                putIntoList(x2u,xid,uid);
                return null;
            }
            waitU.put(xid,uid);//xid�ȴ�uid
            putIntoList(wait,uid,xid);
            if (hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait,uid,xid);
                throw new Exception("deadLock here!");
            }
            Lock lock2 = new ReentrantLock();
            lock2.lock();
            waitLock.put(xid,lock2);
            return lock2;
        } finally {
            lock.unlock();
        }
    }


    private Map<Long,Integer> xidStamp;
    private int stamp;
    /**
     * ����������ж�ͼ���Ƿ��л�
     * @return �Ƿ��л�
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) {
                continue;
            }
            stamp ++;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if (stp != null && stp == stamp) {
            return true;
        }
        if (stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid,stamp);

        Long uid = waitU.get(xid);
        if (uid == null) {
            return false;
        }
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }


    private void removeFromList(Map<Long, List<Long>> wait, long uid, long xid) {
        List<Long> list = wait.get(uid);
        if (list == null)return;
        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()) {
            long e = iterator.next();
            if (e == xid) {
                iterator.remove();
                break;
            }
        }
        if (list.size() == 0) {
            wait.remove(uid);
        }
    }

    private void putIntoList(Map<Long, List<Long>> x2u, long xid, long uid) {
        if (!x2u.containsKey(xid)) {
            x2u.put(xid,new ArrayList<>());
        }
        x2u.get(xid).add(uid);
    }

    private boolean isInList(Map<Long, List<Long>> x2u, long xid, long uid) {
        return x2u.containsKey(xid) && x2u.get(xid).contains(uid);
    }

    /**
     * һ������commit����abortʱ���ͷ������ӵȴ�ͼ��ɾ��
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if (l != null) {
                while (l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    //�ӵȴ�������ѡ��һ��xid��ռ��uid
    private void selectNewXID(Long uid) {
        u2x.remove(uid);
        List<Long> list = wait.get(uid);
        if (list == null)return;
        assert list.size() > 0;
        while (list.size() > 0) {
            Long xid = list.remove(0);
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid,xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }
        if (list.size() == 0) wait.remove(uid);
    }
}
