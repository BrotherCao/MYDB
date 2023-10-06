package com.brocao.dataManager.page;

import com.brocao.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {

    public static final int PAGE_SIZE = 1 << 14;//¼´16k->16*1024BYTE

    int newPage(byte[] initData);

    Page getPage(int pgNo) throws Exception;

    void close();

    void release(Page page);

    void truncateByPgNo(int maxPgNo);

    int getPageNumber();

    void flushPage(Page pg);

    public static PageCacheImpl create(String path,long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(new Exception("file already exists!"));
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (!file.canWrite() || !file.canRead()) {
            Panic.panic(new Exception("file cannot rw!"));
        }

        FileChannel fileChannel = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            fileChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf,fileChannel,(int)memory / PAGE_SIZE);
    }

    public static PageCacheImpl open(String path,long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!file.exists()) {
            Panic.panic(new Exception("file not exists!"));
        }

        if (!file.canWrite() || !file.canRead()) {
            Panic.panic(new Exception("file cannot rw!"));
        }

        FileChannel fileChannel = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            fileChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf,fileChannel,(int)memory / PAGE_SIZE);

    }
}
