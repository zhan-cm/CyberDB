package com.zhan.cyberdb.execution;

import com.zhan.cyberdb.storage.Tuple;

/**
 * 执行器最高宪法：火山模型（Volcano Model / Iterator Model）接口。
 * <p>
 * 所有执行算子（Operator）——如全表扫描（SeqScan）、过滤（Filter）、投影（Projection）、
 * 连接（Join）、排序（Sort）、聚合（Aggregation）、限制（Limit）等——都必须实现此接口，
 * 以遵循统一的迭代器契约。
 * </p>
 *
 * <h2>火山模型（Volcano Model）简介</h2>
 * <p>
 * 火山模型是数据库查询执行引擎中最经典的设计范式，得名于其“自顶向下拉取数据，自底向上喷发元组”
 * 的流水线处理方式。
 * </p>
 * <ul>
 *   <li><b>核心思想</b>：将 SQL 查询计划树中的每个节点抽象为一个迭代器（Iterator），
 *       每个节点只提供 {@code init()} 和 {@code next()} 两个核心方法。</li>
 *   <li><b>拉取模式（Pull-Based）</b>：上层算子通过调用下层算子的 {@code next()} 主动“拉取”数据。
 *       数据流的方向是自底向上（从叶子节点向根节点流动），而控制流是自顶向下（根节点驱动叶子节点）。</li>
 *   <li><b>流水线处理（Pipelining）</b>：数据以元组为单位逐级向上传递，无需将中间结果物化到磁盘，
 *       极大减少了内存占用和 I/O 开销。</li>
 *   <li><b>易于组合</b>：每个算子只关心自己的局部逻辑（如过滤、投影），通过标准接口与上下层解耦，
 *       可像搭积木一样组合出任意复杂的查询计划。</li>
 * </ul>
 *
 * <h2>典型执行流程示例</h2>
 * <pre>{@code
 * Executor plan = new Limit(
 *                      new Filter(
 *                          new SeqScan(table), predicate
 *                      ), 10);
 * plan.init();
 * Tuple tuple;
 * while ((tuple = plan.next()) != null) {
 *     System.out.println(tuple);
 * }
 * }</pre>
 * <p>
 * 执行时，根节点（Limit）的 {@code next()} 被反复调用，每次调用会向下层（Filter）请求一条元组；
 * Filter 再向 SeqScan 请求，直到找到满足条件的元组或数据源枯竭。
 * </p>
 *
 * <h2>实现者的责任</h2>
 * <ul>
 *   <li>在 {@link #init()} 中完成所有一次性准备工作：打开文件、初始化游标、重置状态变量等。
 *      该方法通常在执行计划开始时调用一次。</li>
 *   <li>在 {@link #next()} 中返回下一条满足条件的元组，并维护内部迭代状态。
 *       当数据源耗尽时，必须返回 {@code null} 表示 EOF（End Of File）。</li>
 *   <li>建议实现类在内部持有对下层算子的引用（若为多目算子如 Join，则持有多个引用），
 *       并在自己的 {@code init()} 和 {@code next()} 中酌情调用下层算子的对应方法。</li>
 * </ul>
 *
 * @author Zhan
 * @version 1.0
 * @see Tuple
 * @see com.zhan.cyberdb.execution.SeqScanExecutor
 * @see com.zhan.cyberdb.execution.FilterExecutor
 */
public interface Executor {

    /**
     * 初始化执行器，准备开始迭代。
     * <p>
     * 此方法应在首次调用 {@link #next()} 之前被调用，且通常在整个查询生命周期中只调用一次。
     * 其职责包括但不限于：
     * </p>
     * <ul>
     *   <li><b>打开底层资源</b>：例如打开表文件、建立索引游标、分配缓冲区等。</li>
     *   <li><b>重置内部状态</b>：将迭代位置重置到起始处，清空缓存，初始化计数器。</li>
     *   <li><b>向下层算子传播初始化</b>：若当前算子依赖于子算子的输出，应在此处调用子算子的 {@code init()}。</li>
     *   <li><b>预取第一条数据（可选）</b>：部分算子（如 Sort、Hash Join）可能在此处进行全量数据加载或构建哈希表，
     *       但大部分简单算子仍将实际读取动作推迟到 {@code next()} 中。</li>
     * </ul>
     * <p>
     * 注意：若多次调用 {@code init()}，行为取决于具体实现。典型的正确实现是幂等的，或者至少能正确重置到初始状态。
     * </p>
     */
    void init();

    /**
     * 获取下一条满足条件的元组（Tuple）。
     * <p>
     * 这是火山模型的核心流水线方法。调用者通过反复调用此方法来驱动整个查询计划的执行。
     * 每次调用时，当前算子会：
     * </p>
     * <ol>
     *   <li>向下层算子请求一条或多条元组（通过调用子算子的 {@code next()}）。</li>
     *   <li>对获取的元组应用本算子的逻辑（例如过滤、投影、聚合、连接）。</li>
     *   <li>若当前元组符合输出条件，则将其返回给上层；否则继续循环请求下一条，直到找到合格元组或数据源耗尽。</li>
     * </ol>
     *
     * <h3>返回值语义</h3>
     * <ul>
     *   <li><b>非 {@code null}</b>：返回一个有效的、符合本算子条件的 {@link Tuple} 对象。
     *       调用者可以读取元组中的字段值。</li>
     *   <li><b>{@code null}</b>：表示数据源已枯竭，后续不会再产生任何新的元组。
     *       调用者应停止循环，结束查询处理。</li>
     * </ul>
     *
     * <h3>EOF（End of File）处理</h3>
     * <p>
     * 一旦返回 {@code null}，意味着该算子的迭代已终止。后续再次调用 {@code next()} 的行为是未定义的，
     * 可能继续返回 {@code null}，也可能抛出异常。建议调用者在收到 {@code null} 后即停止迭代，
     * 或显式调用 {@link #init()} 重置后再复用。
     * </p>
     *
     * <h3>阻塞与非阻塞</h3>
     * <p>
     * 在传统磁盘数据库（如 CyberDB）中，此方法可能因磁盘 I/O 而阻塞。若实现为异步 I/O，
     * 则可能立即返回空并设置状态标志。当前版本假设为同步阻塞模型。
     * </p>
     *
     * @return 下一条符合条件的 {@link Tuple}；若数据源耗尽则返回 {@code null}。
     */
    Tuple next();
}