package com.brocao.dataManager.page;

import com.brocao.utils.Parser;

import java.util.Arrays;

import static com.brocao.dataManager.page.PageCache.PAGE_SIZE;

/**
 * PageX������ͨҳ
 * ��ͨҳ�ṹ
 * [freeSpaceOffset][Data]
 * freeSpaceOffset: 2�ֽ� ����λ�ÿ�ʼƫ��
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PAGE_SIZE - OF_DATA;

    public static void setFSO(byte[] raw,short ofData) {
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    //��ȡpg��FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] data) {
        return Parser.parseShort(Arrays.copyOfRange(data,0,2));
    }

    /**
     * ��ȡҳ��Ŀ��пռ��С
     * @param pg ҳ��
     * @return ҳ����пռ��С
     */
    public static int getFreeSpace(Page pg) {
        return PAGE_SIZE - getFSO(pg.getData());
    }

    //��raw����pg�У����ز���λ��
    public static short insert(Page pg,byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);// ��Ҫ + 2�𣿲�Ҫ��offsetָʾ�ľ��ǿ��пռ��ƫ��λ�ã������Ǵ洢�����ݴ�С
        setFSO(pg.getData(),(short) (offset + raw.length));
        return offset;
    }

    //��raw����pg�е�offsetλ�ã�����pg��offset����Ϊ�ϴ��offset
    public static void recoverInsert(Page pg,byte[] raw,short offset) {
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);

        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(),(short) (offset + raw.length));
        }


    }

    //��raw����pg�е�offsetλ�ã�������update
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
