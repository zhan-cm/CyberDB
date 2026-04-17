package com.zhan.cyberdb.execution;

import com.zhan.cyberdb.storage.Tuple;

import java.util.function.Predicate;

/**
 * 过滤执行器（Filter Operator）。
 * <p>
 * 对应 SQL 查询中的 {@code WHERE} 子句，负责对下层算子产生的元组流进行逐行筛选，
 * 仅将通过过滤条件的元组向上传递。
 * </p>
 *
 * <h2>在火山模型中的角色</h2>
 * <p>
 * {@code FilterExecutor} 是一个典型的<b>一元算子（Unary Operator）</b>，即它只拥有一个子算子（child）。
 * 它不改变元组的内部结构（字段数量、类型均不变），仅根据给定的布尔条件决定元组的去留。
 * 其执行逻辑完美诠释了火山模型“拉取、过滤、传递”的流水线思想。
 * </p>
 *
 * <h2>执行逻辑图解</h2>
 * <pre>
 * ┌─────────────────┐
 * │  上层算子 (Parent) │
 * └────────┬────────┘
 *          │ next()
 *          ▼
 * ┌─────────────────┐
 * │  FilterExecutor │  ──── 持有 Predicate 条件
 * └────────┬────────┘
 *          │ next() (反复调用直到通过或耗尽)
 *          ▼
 * ┌─────────────────┐
 * │  子算子 (Child)  │  (例如 SeqScanExecutor)
 * └─────────────────┘
 * </pre>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>{@code SELECT * FROM users WHERE age > 18}</li>
 *   <li>{@code SELECT id, name FROM products WHERE price < 100 AND stock > 0}</li>
 *   <li>作为更复杂算子（如 Join、Aggregate）的子节点，先过滤再处理以降低数据量。</li>
 * </ul>
 *
 * <p>
 * 注意：本类<b>不关心具体过滤逻辑</b>，过滤条件通过构造函数中的 {@link Predicate}&lt;{@link Tuple}&gt; 注入，
 * 符合单一职责原则和策略模式，极大提高了灵活性。
 * </p>
 *
 * @author Zhan
 * @version 1.0
 * @see Executor
 * @see Predicate
 * @see Tuple
 */
public class FilterExecutor implements Executor {

    /**
     * 子执行器（下级工人）。
     * <p>
     * 作为一元算子，{@code FilterExecutor} 依赖于一个且仅一个子算子提供原始元组流。
     * 该子算子可以是任何实现了 {@link Executor} 接口的对象，例如：
     * <ul>
     *   <li>{@code SeqScanExecutor} —— 全表扫描</li>
     *   <li>{@code IndexScanExecutor} —— 索引扫描</li>
     *   <li>另一个 {@code FilterExecutor} —— 多个过滤条件的链式组合（对应 {@code WHERE ... AND ...}）</li>
     *   <li>{@code ProjectionExecutor}、{@code SortExecutor} 等</li>
     * </ul>
     * 这种设计使得过滤算子可以与任何数据源无缝对接，无需修改自身代码。
     * </p>
     */
    private Executor child;

    /**
     * 过滤谓词（Predicate）。
     * <p>
     * 一个函数式接口，定义了“什么样的元组可以通过”的规则。
     * 其 {@code test(Tuple)} 方法返回 {@code true} 表示该元组满足条件，应被保留并向上传递；
     * 返回 {@code false} 表示该元组不符合条件，应被丢弃，直接请求下一条。
     * </p>
     * <p>
     * 使用 Java 8 的 {@link java.util.function.Predicate} 而非自定义接口，
     * 使得上层构造查询计划时可以直接使用 Lambda 表达式或方法引用，代码简洁直观。
     * 例如：
     * <pre>{@code
     * Predicate<Tuple> ageFilter = tuple -> tuple.getInt(1) > 18;
     * FilterExecutor filter = new FilterExecutor(scan, ageFilter);
     * }</pre>
     * </p>
     */
    private Predicate<Tuple> predicate;

    /**
     * 构造一个过滤执行器。
     * <p>
     * 在构造时即绑定了数据来源（子算子）和过滤条件（谓词），
     * 此后调用 {@link #init()} 和 {@link #next()} 即可开始流水线处理。
     * </p>
     *
     * @param child     下级执行器，提供待过滤的原始元组流。不能为 {@code null}。
     * @param predicate 过滤条件，决定元组去留。若为 {@code null}，则行为等价于透传所有元组
     *                  （但通常不建议为 {@code null}，应使用更明确的算子）。
     */
    public FilterExecutor(Executor child, Predicate<Tuple> predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    /**
     * 初始化过滤执行器及其子执行器。
     * <p>
     * 遵循火山模型的“初始化传导”原则：上层算子在初始化自身时，
     * <b>必须主动调用子算子的 {@code init()}</b>，确保整个算子树从根到叶都被正确重置。
     * </p>
     * <p>
     * 本方法本身无额外状态需要初始化（因为过滤是无状态的），直接委托给子算子。
     * 若未来扩展支持缓存或统计信息，可在此处添加相应逻辑。
     * </p>
     * <p>
     * 典型调用时机：在执行查询计划的最外层（如 {@code plan.init()}）时，
     * 初始化信号会逐层向下传播，直到叶子节点（如打开表文件、重置游标）。
     * </p>
     */
    @Override
    public void init() {
        // 过滤算子自身无状态，仅需唤醒下级算子。
        // 这一行是火山模型初始化的核心传播链路。
        child.init();
    }

    /**
     * 获取下一条通过过滤条件的元组。
     * <p>
     * 这是过滤算子的核心流水线方法，体现了经典的“拉取-过滤”循环逻辑。
     * 执行流程如下：
     * </p>
     * <ol>
     *   <li>进入一个无限循环（通过 {@code while} 实现）。</li>
     *   <li>调用子算子的 {@code child.next()}，尝试获取下一条原始元组。</li>
     *   <li>若 {@code child.next()} 返回 {@code null}，表示子算子数据源已枯竭，
     *       循环终止，本方法返回 {@code null} 给上层，标志 EOF。</li>
     *   <li>若获取到有效元组 {@code tuple}，则调用 {@code predicate.test(tuple)} 进行条件判断：
     *       <ul>
     *           <li>若返回 {@code true}（条件满足），则<b>立即将该元组返回给上层</b>，
     *               结束本次 {@code next()} 调用。</li>
     *           <li>若返回 {@code false}（条件不满足），则<b>丢弃该元组</b>，
     *               不向上返回任何内容，并继续执行循环，向子算子索取下一条元组。</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * <h3>关于 {@code while} 循环的必要性</h3>
     * <p>
     * 由于子算子返回的元组中可能只有部分满足过滤条件，不满足的必须被跳过。
     * 如果仅做单次判断（例如 {@code if (predicate.test(tuple)) return tuple; else return null;}），
     * 那么在遇到第一条不满足条件的元组时就会错误地返回 {@code null} 导致查询提前终止。
     * 因此必须使用循环持续索取，直到找到一条满足条件的元组，或者子算子彻底耗尽。
     * </p>
     *
     * <h3>性能考量</h3>
     * <p>
     * 该方法不会缓存任何元组，每次调用都可能触发一次或多次子算子的 {@code next()} 调用。
     * 在最坏情况下（过滤率极低），一次 {@code next()} 可能遍历大量无效元组，
     * 但整体仍保持 O(1) 空间复杂度，符合火山模型对内存友好的设计初衷。
     * </p>
     *
     * @return 下一条通过过滤条件的 {@link Tuple}；若无更多数据则返回 {@code null}。
     */
    @Override
    public Tuple next() {
        Tuple tuple;

        // 持续向子算子索取元组，直到找到一条满足条件的，或子算子返回 null
        while ((tuple = child.next()) != null) {

            // 应用过滤谓词检验当前元组
            if (predicate.test(tuple)) {
                // 条件满足：立即将元组向上传递，结束本次 next() 调用
                return tuple;
            }

            // 条件不满足：元组被丢弃（Discard），继续循环索取下一条
            // 此处的“丢弃”是隐式的——没有任何代码将 tuple 向上返回或存储，
            // 它将在下一次循环中被新的 tuple 覆盖，并最终被垃圾回收器回收。
        }

        // while 循环结束的唯一原因：child.next() 返回了 null
        // 意味着下层数据源已彻底耗尽，本算子也向上返回 null，通知上层停止迭代。
        return null;
    }
}