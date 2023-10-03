package com.brocao.dataManager.page;

import javax.swing.*;
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
}
