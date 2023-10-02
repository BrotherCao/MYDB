package com.brocao.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;

public abstract class AbstractCache<T> {

    private HashMap<Long,T> cache; //ʵ�ʻ�������

    private HashMap<Long,Integer> references; //��Դ�����ø���

    private HashMap<Long,Boolean> getting; //���ڱ���ȡ����Դ

    private int maxResource; //�������󻺴���Դ��

    private int count  = 0; //�����л���ĸ���

    private Lock lock; //��

    /**
     * ��ȡһ������
     * @param key ��
     * @return ����
     * @throws Exception �쳣
     */
    protected T get(long key) throws Exception{
        while (true) {
            //����
            lock.lock();
            //��������ڻ�ȡ���͵ȵ�
            if(getting.containsKey(key)) {
                //�����߳����ڻ�ȡ
                lock.unlock();
                try{
                    Thread.sleep(1);//��ϢƬ��
                }catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //û���߳��ڻ�ȡ�����ȴӻ����л�ȡ
            if (cache.containsKey(key)) {
                //��Դ�ڻ����У�ֱ�ӷ���
                T t = cache.get(key);
                references.put(key,references.get(key) + 1);
                lock.unlock();
                return t;
            }

            //û�ڻ������ȡһ��
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw new Exception("cache has been full!");
            }
            //������Դ�л�ȡ��������
            count++;
            getting.put(key,true);
            lock.unlock();
            break;//����ѭ��
        }
        T t = null;
        try {
            t = getForCache(key);
        } catch (Exception e) {
            //�쳣����ȡʧ�ܣ��ع�
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //��ȡ�ɹ������뻺��
        lock.lock();
        getting.remove(key);
        cache.put(key,t);
        references.put(key,1);
        lock.unlock();
        return t;
    }

    /**
     * ǿ���ͷ�һ������
     * @param key ��
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                cache.remove(key);
                references.remove(key);
                count--;
            } else {
                references.put(key,ref);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void close () {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
    /**
     * ����Դ���ڻ���ʱ�Ļ�ȡ��Ϊ
     * @param key ��
     * @return ����
     * @throws Exception �쳣
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * ����Դ������ʱ��д����Ϊ
     * @param obj ��Դ
     */
    protected abstract void releaseForCache(T obj);
}
