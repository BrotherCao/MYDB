package com.brocao.dataManager;


import com.brocao.common.AbstractCache;
import com.brocao.dataManager.PageIndex.PageIndex;
import com.brocao.dataManager.PageIndex.PageInfo;
import com.brocao.dataManager.dataItem.DataItem;
import com.brocao.dataManager.dataItem.DataItemImpl;
import com.brocao.dataManager.log.Logger;
import com.brocao.dataManager.page.Page;
import com.brocao.dataManager.page.PageCache;
import com.brocao.dataManager.page.PageOne;
import com.brocao.dataManager.page.PageX;
import com.brocao.transactionManager.TransactionManager;
import com.brocao.utils.Panic;
import com.brocao.utils.Types;


public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager tm) {
        super(0);
        this.tm = tm;
        this.pageCache = pageCache;
        this.logger = logger;
        this.pageIndex = new PageIndex();
    }
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if (!dataItem.isValid()) {
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    /**
     * TODO 尚未明白
     * @param xid 事务id
     * @param data 数据
     * @return 返回uid
     * @throws Exception 异常
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw new Exception("data too large!");
        }

        PageInfo pageInfo = null;
        for (int i = 0;i < 5;i++) {
            pageInfo = pageIndex.select(raw.length);
            if (pageInfo != null) {
                break;
            } else {
                int newPgNo = pageCache.newPage(PageX.initRaw());
                pageIndex.add(newPgNo,PageX.MAX_FREE_SPACE);
            }
        }

        if (pageIndex == null) {
            throw new Exception("database busy!");
        }

        Page page = null;
        int freeSpace = 0;
        try {
            page = pageCache.getPage(pageInfo.pgNo);
            byte[] log = Recover.insertLog(xid,page,raw);
            logger.log(log);

            short offset = PageX.insert(page, raw);

            page.release();
            return Types.addressToUid(pageInfo.pgNo,offset);

        } finally {
            //将取出的page重新插入pageIndex
            if (page != null) {
                pageIndex.add(pageInfo.pgNo,PageX.getFreeSpace(page));
            } else {
                pageIndex.add(pageInfo.pgNo,freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pageCache.close();;
    }

    /**
     * 为xid生成update日志
     * @param xid 事务id
     * @param dataItem 数据项
     */
    public void logDataItem(long xid,DataItem dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }

    @Override
    protected DataItem getForCache(long key) throws Exception {
        short offset = (short) (key & ((1L << 16) - 1));//为何要-1？因为1<<16是10000000000000000，-1才是16个1
        key >>>= 32;
        int pgNo = (int) (key & ((1L << 32) - 1));
        Page page = pageCache.getPage(pgNo);
        return DataItem.parseDataItem(page,offset,this);
    }

    @Override
    protected void releaseForCache(DataItem obj) {
        obj.page().release();
    }

    //创建文件时初始化PageOne
    void initPageOne() {
        int pgNo = pageCache.newPage(PageOne.initRaw());
        assert  pgNo == 1;//是true才继续执行
        try {
            pageOne = pageCache.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageOne);
    }

    //在打开已有文件时读入PageOne,并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    //初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pageCache.getPageNumber();
        for (int i = 2;i <= pageNumber;i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(page.getPageNumber(),PageX.getFreeSpace(page));
            page.release();
        }
    }
}
