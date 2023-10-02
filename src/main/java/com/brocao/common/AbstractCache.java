package com.brocao.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;

public abstract class AbstractCache<T> {

    private HashMap<Long,T> cache; //实际缓存数据

    private HashMap<Long,Integer> references; //资源的引用个数

    private HashMap<Long,Boolean> getting; //正在被获取的资源

    private int maxResource; //缓存的最大缓存资源数

    private int count  = 0; //缓存中缓存的个数

    private Lock lock; //锁

    /**
     * 获取一个数据
     * @param key 键
     * @return 数据
     * @throws Exception 异常
     */
    protected T get(long key) throws Exception{
        while (true) {
            //上锁
            lock.lock();
            //如果有正在获取，就等等
            if(getting.containsKey(key)) {
                //其他线程正在获取
                lock.unlock();
                try{
                    Thread.sleep(1);//休息片刻
                }catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //没有线程在获取，首先从缓存中获取
            if (cache.containsKey(key)) {
                //资源在缓存中，直接返回
                T t = cache.get(key);
                references.put(key,references.get(key) + 1);
                lock.unlock();
                return t;
            }

            //没在缓存里，获取一下
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw new Exception("cache has been full!");
            }
            //从数据源中获取，到缓存
            count++;
            getting.put(key,true);
            lock.unlock();
            break;//跳出循环
        }
        T t = null;
        try {
            t = getForCache(key);
        } catch (Exception e) {
            //异常，获取失败，回滚
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //获取成功，放入缓存
        lock.lock();
        getting.remove(key);
        cache.put(key,t);
        references.put(key,1);
        lock.unlock();
        return t;
    }

    /**
     * 强行释放一个缓存
     * @param key 键
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
     * 当资源不在缓存时的获取行为
     * @param key 键
     * @return 数据
     * @throws Exception 异常
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     * @param obj 资源
     */
    protected abstract void releaseForCache(T obj);
}
