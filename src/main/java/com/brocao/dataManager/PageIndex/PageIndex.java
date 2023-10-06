package com.brocao.dataManager.PageIndex;

import com.brocao.dataManager.page.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ����Щ��δ��
 */
public class PageIndex {

    //��һҳ�ֳ�40������
    private static final int INTERVALS_NO = 40;

    //ÿ��������ֽڴ�С��
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;

    //ÿҳ����40������
    //41����������ÿ���ڵ���ҳ��Ϣ
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
            //���пռ�/ÿ�����С
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
