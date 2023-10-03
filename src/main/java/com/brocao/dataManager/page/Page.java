package com.brocao.dataManager.page;

public interface Page {

    void lock();

    void unLock();

    void release();

    void setDirty(boolean dirty);

    /**
     * 判断是否脏页面
     * 和磁盘不一致，未同步到磁盘即为脏页面
     * @return 是否脏页
     */
    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
