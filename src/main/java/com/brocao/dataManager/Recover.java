package com.brocao.dataManager;

import com.brocao.common.SubArray;
import com.brocao.dataManager.dataItem.DataItem;
import com.brocao.dataManager.log.Logger;
import com.brocao.dataManager.page.Page;
import com.brocao.dataManager.page.PageCache;
import com.brocao.dataManager.page.PageX;
import com.brocao.transactionManager.TransactionManager;
import com.brocao.utils.Panic;
import com.brocao.utils.Parser;
import com.google.common.primitives.Bytes;
import org.checkerframework.checker.units.qual.A;

import java.util.*;

/**
 * 数据恢复类
 * 主要有两种操做，插入和更新
 */
public class Recover {
    /**
     * updateLog:
     * [LogType][XID][UID][OldRaw][NewRaw]
     *
     * insertLog:
     * [LogType][XID][PgNo][Offset][Raw]
     */
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    static class InsertLogInfo {
        long xid;
        int pgNo;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache) {
        System.out.println("Recovering...");

        logger.rewind();
        int maxPgNo = 0;
        while (true) {
            byte[] log = logger.next();
            if (log == null)break;//日志记录遍历完了
            int pgNo;
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                pgNo = insertLogInfo.pgNo;
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                pgNo = updateLogInfo.pgNo;
            }
            if (pgNo > maxPgNo) {
                maxPgNo = pgNo;
            }
        }
        //解析完了日志
        if (maxPgNo == 0) {
            maxPgNo = 1;
        }

        pageCache.truncateByPgNo(maxPgNo);
        System.out.println("Truncate to" + maxPgNo + "Pages");

        redoTransactions(tm,logger,pageCache);
        System.out.println("Redo Transaction Over!");

        undoTransactions(tm,logger,pageCache);
        System.out.println("Undo Transactions Over!");

        System.out.println("Recovery Over!");
    }

    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null)break;
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                //获取事务id
                long xid = insertLogInfo.xid;
                //是否还在活跃时停机的
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid,new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid,new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }

            //对所有active log进行倒叙undo
            for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
                List<byte[]> logs = entry.getValue();
                for (int i = logs.size() - 1;i >= 0;i--) {
                    byte[] data = logs.get(i);
                    if (isInsertLog(log)) {
                        doInsertLog(pageCache,data,UNDO);
                    } else {
                        doUpdateLog(pageCache,log,UNDO);
                    }
                }
                tm.abort(entry.getKey());
            }

        }
    }

    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pageCache,log,REDO);
                }
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pageCache,log,REDO);
                }
            }
        }
    }

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        int pgNo;
        short offset;
        byte[] raw;
        UpdateLogInfo updateLogInfo = parseUpdateLog(log);
        pgNo = updateLogInfo.pgNo;
        offset = updateLogInfo.offset;
        if (flag == REDO) {
            raw = updateLogInfo.newRaw;
        } else {
            //回滚，写旧值
            raw = updateLogInfo.oldRaw;
        }
        Page page = null;
        try {
            page = pageCache.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(page,raw,offset);
        } finally {
            page.release();
        }
    }

    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo insertLogInfo = parseInsertLog(log);
        Page page = null;
        try {
            page = pageCache.getPage(insertLogInfo.pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(insertLogInfo.raw);
            }
            PageX.recoverInsert(page,insertLogInfo.raw,insertLogInfo.offset);
        } finally {
            page.release();
        }
    }

    /**
     * 解析插入日志
     * @param log 日志
     * @return InsertLogInfo
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID,OF_INSERT_PGNO));
        insertLogInfo.pgNo = Parser.parseInt(Arrays.copyOfRange(log,OF_INSERT_PGNO,OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log,OF_INSERT_OFFSET,OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log,OF_INSERT_RAW,log.length);
        return insertLogInfo;
    }

    /**
     * 解析更新日志
     * @param log [LogType][XID][PgNo][Offset][Raw]
     * @return UpdateLogInfo
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID,OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log,OF_UPDATE_UID,OF_UPDATE_RAW));
        updateLogInfo.offset = (short) (uid & ((1L << 16) - 1));//低16位
        uid >>>= 32;
        updateLogInfo.pgNo = (int) (uid & ((1L << 32) - 1));//利用高32位
        int len = (log.length - OF_UPDATE_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW,OF_UPDATE_RAW + len);
        updateLogInfo.newRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW + len,OF_UPDATE_RAW + 2 * len);
        return updateLogInfo;
    }

    /**
     * 判断是否是插入日志
     * @param log 日志
     * @return 结果
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    public static byte[] insertLog(long xid,Page page,byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgNoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(page));
        return Bytes.concat(logTypeRaw,xidRaw,pgNoRaw,offsetRaw,raw);
    }

    public static byte[] updateLog(long xid,DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType,xidRaw,uidRaw,oldRaw,newRaw);
    }

}
