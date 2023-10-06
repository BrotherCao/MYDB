package com.brocao.dataManager;

import com.brocao.dataManager.dataItem.DataItem;
import com.brocao.dataManager.log.Logger;
import com.brocao.dataManager.page.PageCache;
import com.brocao.dataManager.page.PageCacheImpl;
import com.brocao.dataManager.page.PageOne;
import com.brocao.transactionManager.TransactionManager;

public interface DataManager {

    DataItem read(long uid) throws Exception;
    long insert(long xid,byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pageCache = PageCache.create(path,mem);
        Logger logger = Logger.create(path);
        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, tm);
        dataManager.initPageOne();
        return dataManager;
    }

    public static DataManager open(String path,long mem,TransactionManager tm) {
        PageCacheImpl pageCache = PageCache.open(path, mem);
        Logger logger = Logger.open(path);
        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, tm);
        if (!dataManager.loadCheckPageOne()) {
            Recover.recover(tm,logger,pageCache);
        }
        dataManager.fillPageIndex();
        PageOne.setVcOpen(dataManager.pageOne);
        dataManager.pageCache.flushPage(dataManager.pageOne);

        return dataManager;
    }

}
