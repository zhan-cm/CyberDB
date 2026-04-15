package com.zhan.cyberdb.execution;

import com.zhan.cyberdb.storage.TablePage;
import com.zhan.cyberdb.storage.Tuple;

/**
 * 顺序扫描执行器 (SeqScan)
 * 它是流水线的最底层打工人，负责从物理的 TablePage 里面，把数据一行一行抠出来。
 */
public class SeqScanExecutor implements Executor {

    // 也就是我们第二关写的"分槽页"，里面装着真实数据
    private TablePage tablePage;
    
    // 当前扫描到了第几个槽位 (游标)
    private int currentSlotId;
    
    // 这个页面一共有多少个槽位
    private int maxSlotId;

    public SeqScanExecutor(TablePage tablePage) {
        this.tablePage = tablePage;
    }

    @Override
    public void init() {
        // 老板喊开工了！把游标清零，准备从第 0 个槽位开始向下扫描
        this.currentSlotId = 0;
        this.maxSlotId = tablePage.getTupleCount();
    }

    @Override
    public Tuple next() {
        // 只要游标还没到底，就继续往下找
        while (currentSlotId < maxSlotId) {
            // 🌟 直接调用我们第二关写好的底层读取方法！
            Tuple tuple = tablePage.getTuple(currentSlotId);
            
            // 游标推进 (指向下一行)
            currentSlotId++;
            
            // ⚠️ 极其关键：你还记得我们写的"逻辑删除"吗？
            // 已经被删除的数据，getTuple 会返回 null。我们必须跳过这些幽灵数据！
            if (tuple != null) {
                return tuple; // 找到一条活着的真实数据，向上层汇报！
            }
        }
        
        // 循环结束了都没 return，说明整个页面的槽位都扫光了，返回 null 通知上层没数据了
        return null; 
    }
}