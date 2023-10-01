package com.brocao.transactionManager;

/**
 * 事务管理器接口
 */
public interface TransactionManager {
    /**
     * 开启一个新事务
     */
    long begin();

    /**
     * 提交一个新事务
     */
    void commit(long xid);

    /**
     * 取消一个事务
     */
    void abort(long xid);

    /**
     * 查询一个事务的状态是否还在进行
     * @param xid 事务id
     * @return true//false
     */
    boolean isActive(long xid);

    /**
     * 查询一个事务的状态是否为已提交
     * @param xid 事务id
     * @return 布尔值
     */
    boolean isCommited(long xid);

    /**
     * 查询一个事务的状态是否是已取消
     * @param xid 事务id
     * @return true/false
     */
    boolean isAborted(long xid);

    /**
     * 关闭TM
     */
    void close();
}
