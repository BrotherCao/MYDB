package com.brocao.dataManager.PageIndex;

import com.brocao.dataManager.page.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 尚有些许未明
 */
public class PageIndex {

    //将一页分成40个区间
    private static final int INTERVALS_NO = 40;

    //每个区间的字节大小？
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;

    //每页都是40个区间
    //41条链表，链表每个节点是页信息
    private List<PageInfo>[] lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0;i < INTERVALS_NO + 1;i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgNo,int freeSpace) {
        lock.lock();
        try {
            //空闲空间/每区间大小
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgNo,freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select (int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number ++;
            while (number <= THRESHOLD) {
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
