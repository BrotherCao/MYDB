package com.brocao.dataManager.page;

public interface Page {

    void lock();

    void unLock();

    void release();

    void setDirty(boolean dirty);

    /**
     * �ж��Ƿ���ҳ��
     * �ʹ��̲�һ�£�δͬ�������̼�Ϊ��ҳ��
     * @return �Ƿ���ҳ
     */
    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
