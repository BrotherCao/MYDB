package com.brocao.dataManager.page;


import com.brocao.utils.RandomUtil;

import java.util.Arrays;

/**
 * 对于第一个页特殊管理
 * db启动时给100-107字节填入随机字节，关闭时拷贝到108-115
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {

    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    public static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] data) {
        System.arraycopy(data,OF_VC,data,OF_VC + LEN_VC,LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] data) {
        return Arrays.equals(Arrays.copyOfRange(data,OF_VC,OF_VC + LEN_VC),
                Arrays.copyOfRange(data,OF_VC + LEN_VC,OF_VC + 2 * LEN_VC));
    }
}
