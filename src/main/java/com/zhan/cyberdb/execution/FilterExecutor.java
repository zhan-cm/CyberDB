package com.zhan.cyberdb.execution;

import com.zhan.cyberdb.storage.Tuple;
import java.util.function.Predicate;

/**
 * 过滤执行器 (Filter)
 * 相当于 SQL 里的 WHERE 条件。
 */
public class FilterExecutor implements Executor {

    // 它的下级工人 (可能是 SeqScan，也可能是别的算子，它不在乎，只要是 Executor 就行)
    private Executor child;
    
    // 过滤条件 (Java 8 的函数式接口，返回 true/false)
    private Predicate<Tuple> predicate;

    public FilterExecutor(Executor child, Predicate<Tuple> predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void init() {
        // 自己开工前，必须先叫醒下级工人！这是一层层往下传导的。
        child.init();
    }

    @Override
    public Tuple next() {
        Tuple tuple;
        
        // 🌟 这就是你刚才准确猜中的 while 循环！
        // 只要下级还能交出数据 (不为 null)，我就不断地要！
        while ((tuple = child.next()) != null) {
            
            // 掏出我们的过滤条件来检验这条数据
            if (predicate.test(tuple)) {
                // 如果条件符合 (true)，太棒了！把这条数据原封不动地返回给我的上级！
                return tuple;
            }
            
            // 如果走到这里，说明条件不符合 (false)。
            // 发生丢弃 (Discard)！什么都不 return，直接进入下一次 while 循环，
            // 无情地向 child 吼一句：“这条不行，再给我拿一条新的 next()！”
        }
        
        // 如果 while 循环结束了，说明 child 返回了 null。
        // 这意味着底层已经被彻底抽干了，我也只能无奈地向上层返回 null。
        return null;
    }
}