package com.brocao.dataManager.dataItem;

import com.brocao.common.SubArray;
import com.brocao.dataManager.DataManagerImpl;
import com.brocao.dataManager.page.Page;
import com.brocao.utils.Parser;
import com.brocao.utils.Types;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

public interface DataItem {

    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unLock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid,size,raw);
    }

    //从页面的offset处解析dataItem
    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw,offset + DataItemImpl.OF_SIZE,offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(page.getPageNumber(),offset);
        return new DataItemImpl(new SubArray(raw,offset,offset + length),new byte[length],page,uid,dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }

}
