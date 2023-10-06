package com.brocao.dataManager.page;

import com.brocao.utils.Parser;

import java.util.Arrays;

import static com.brocao.dataManager.page.PageCache.PAGE_SIZE;

/**
 * PageX管理普通页
 * 普通页结构
 * [freeSpaceOffset][Data]
 * freeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PAGE_SIZE - OF_DATA;

    public static void setFSO(byte[] raw,short ofData) {
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    //获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] data) {
        return Parser.parseShort(Arrays.copyOfRange(data,0,2));
    }

    /**
     * 获取页面的空闲空间大小
     * @param pg 页面
     * @return 页面空闲空间大小
     */
    public static int getFreeSpace(Page pg) {
        return PAGE_SIZE - getFSO(pg.getData());
    }

    //将raw插入pg中，返回插入位置
    public static short insert(Page pg,byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);// 不要 + 2吗？不要，offset指示的就是空闲空间的偏移位置，而不是存储的数据大小
        setFSO(pg.getData(),(short) (offset + raw.length));
        return offset;
    }

    //将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg,byte[] raw,short offset) {
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);

        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(),(short) (offset + raw.length));
        }


    }

    //将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg,byte[] raw,short offset) {
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
    }

    public static byte[] initRaw() {
        byte[] raw = new byte[PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }
}
