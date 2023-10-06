package com.brocao.versionManager;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 死锁检测
 * 通过构建图，检查是否有环来检测死锁
 */
public class LockTable {

    private Map<Long, List<Long>> x2u;//某个XID已经获得的资源的UID列表

    private Map<Long,Long> u2x; //UID被某个XID持有

    private Map<Long,List<Long>> wait; //正在等待UID的XID列表

    private Map<Long, Lock> waitLock; //正在等待资源的XID的锁

    private Map<Long,Long> waitU; //XID正在等待的UID

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
     * 每次需要等待时，尝试向图中增加一条边，并进行死锁检测
     * 监测到死锁，就撤销这条边，不允许添加，并撤销该事务
     * @param xid 事务id
     * @param uid 资源uid
     * @return 不需要等待则返回null,否则返回锁对象
     * @throws Exception 造成死锁则抛出异常
     */
    public Lock add(long xid,long uid) throws Exception {
        lock.lock();
        try {
            if (isInList(x2u,xid,uid)) {
                return null;
            }
            //资源没被任何事务获取
            if (!u2x.containsKey(uid)) {
                u2x.put(uid,xid);
                putIntoList(x2u,xid,uid);
                return null;
            }
            waitU.put(xid,uid);//xid等待uid
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
     * 检测死锁，判断图中是否有环
     * @return 是否有环
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
     * 一个事务commit或者abort时，释放锁，从等待图中删除
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

    //从等待队列中选择一个xid来占用uid
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
