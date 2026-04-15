package com.zhan.cyberdb.execution;

import com.zhan.cyberdb.storage.Tuple;

/**
 * 执行器最高宪法：火山模型 (Volcano Model) 接口
 * 所有的算子 (Scan, Filter, Limit, Join) 都必须遵守这个契约。
 */
public interface Executor {

    /**
     * 1. 唤醒工人，准备干活。
     * 比如：把游标重置到第一行、打开底层文件、清空临时缓存等。
     */
    void init();

    /**
     * 2. 核心流水线动作：给我下一条合格的数据！
     * @return 返回下一条 Tuple；如果底层数据已经被彻底抽干了，返回 null (代表 EOF)。
     */
    Tuple next();
}