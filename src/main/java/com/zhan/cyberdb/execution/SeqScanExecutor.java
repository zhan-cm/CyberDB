package com.zhan.cyberdb.execution;

import com.zhan.cyberdb.storage.TablePage;
import com.zhan.cyberdb.storage.Tuple;

/**
 * 顺序扫描执行器（Sequential Scan Operator / SeqScan）。
 * <p>
 * 对应 SQL 查询中的全表扫描（Table Scan）或堆表顺序扫描。它是执行计划树中最底层的<b>叶子算子</b>，
 * 负责直接从物理存储结构——{@link TablePage}（分槽页）中逐行读取元组，并将有效数据向上层算子输送。
 * </p>
 *
 * <h2>在火山模型中的角色</h2>
 * <p>
 * {@code SeqScanExecutor} 是典型的<b>零目算子（Leaf Operator）</b>，即它没有下层子算子，
 * 而是直接与存储层交互。其 {@link #next()} 方法实现了对单个数据页内所有元组的线性遍历，
 * 是查询流水线的数据源头。
 * </p>
 * <ul>
 *   <li><b>数据来源</b>：一个 {@link TablePage} 对象，该对象封装了磁盘上 4KB 页面内的元组存储结构。</li>
 *   <li><b>扫描粒度</b>：每次 {@code next()} 返回一条有效的、未被删除的元组。若当前页扫描完毕，
 *       则返回 {@code null}。更上层的算子（如迭代器包装器）可能负责切换至下一个数据页。</li>
 * </ul>
 *
 * <h2>执行逻辑图解</h2>
 * <pre>
 * ┌─────────────────┐
 * │  上层算子 (Parent) │  (例如 FilterExecutor)
 * └────────┬────────┘
 *          │ next()
 *          ▼
 * ┌─────────────────┐
 * │ SeqScanExecutor │  ──── 持有 TablePage 引用与槽位游标
 * └────────┬────────┘
 *          │ 调用 tablePage.getTuple(slotId)
 *          ▼
 * ┌─────────────────┐
 * │   TablePage     │  (物理存储：槽式页面)
 * └─────────────────┘
 * </pre>
 *
 * <h2>关于逻辑删除的处理</h2>
 * <p>
 * 数据库中的删除操作通常分为“物理删除”和“逻辑删除”。在 CyberDB 的 {@link TablePage} 实现中，
 * 采用逻辑删除策略：被删除的元组并不会立即从槽位中抹除数据，而是在槽位头部打上删除标记（例如置位
 * 或长度字段置为特殊值）。当调用 {@link TablePage#getTuple(int)} 获取该槽位的元组时，
 * 底层方法会检测删除标记并返回 {@code null}。
 * </p>
 * <p>
 * 本扫描器在遍历过程中<b>必须显式跳过这些返回 {@code null} 的槽位</b>，因为它们代表已删除的“幽灵数据”，
 * 不应出现在查询结果中。这是通过 {@code while} 循环中的空值检查实现的。
 * </p>
 *
 * <h2>游标管理</h2>
 * <p>
 * 通过 {@code currentSlotId} 维护当前扫描进度。每次成功返回一条有效元组后，游标会向前推进（自增 1）。
 * 遇到被删除的槽位时，游标同样会推进，但不返回数据，直接进入下一轮循环。
 * </p>
 *
 * @author Zhan
 * @version 1.0
 * @see Executor
 * @see TablePage
 * @see Tuple
 */
public class SeqScanExecutor implements Executor {

    /**
     * 待扫描的数据页（分槽页，Slotted Page）。
     * <p>
     * 这是扫描器的数据来源，封装了一个 4KB 磁盘页面的内存镜像。{@link TablePage} 内部维护了：
     * <ul>
     *   <li>页面头部信息（如元组数量、空闲空间起始偏移等）。</li>
     *   <li>槽位数组（Slot Array），记录每个元组的起始偏移和长度。</li>
     *   <li>实际元组数据存储区。</li>
     * </ul>
     * 本扫描器不关心页面内部实现细节，仅通过 {@link TablePage#getTuple(int)} 获取元组。
     * </p>
     * <p>
     * 注意：当前实现仅支持扫描<b>单个</b> {@code TablePage}。若需要扫描整张表（多个页），
     * 应由上层算子（例如一个表扫描迭代器）负责在多个 {@code SeqScanExecutor} 之间切换。
     * </p>
     */
    private TablePage tablePage;

    /**
     * 当前扫描游标（Current Slot Index）。
     * <p>
     * 指示下一次调用 {@link #next()} 时应当尝试从哪个槽位开始读取。
     * 游标从 0 开始，每读取一个槽位（无论该槽位的元组是否有效、是否被删除）都会自增 1，
     * 直至达到 {@link #maxSlotId}。
     * </p>
     * <p>
     * 游标的值严格小于 {@link #maxSlotId} 时表示仍有槽位未扫描；
     * 当游标等于 {@link #maxSlotId} 时表示整个页面的槽位已全部遍历完毕，
     * 后续调用 {@code next()} 将直接返回 {@code null}。
     * </p>
     */
    private int currentSlotId;

    /**
     * 页面内的元组总数（总槽位数）。
     * <p>
     * 该值在 {@link #init()} 时从 {@link TablePage#getTupleCount()} 获取并缓存。
     * 它表示该页面上槽位数组的有效长度，即历史上曾插入过的元组数量（包括已逻辑删除的元组）。
     * </p>
     * <p>
     * 注意：由于逻辑删除的存在，{@code maxSlotId} <b>不等于</b>有效元组数。
     * 扫描器需要遍历全部 {@code maxSlotId} 个槽位，并通过空值检查过滤出真正存活的元组。
     * </p>
     */
    private int maxSlotId;

    /**
     * 构造一个针对指定数据页的顺序扫描执行器。
     * <p>
     * 在构造时仅绑定目标 {@link TablePage}，并不执行任何扫描操作。
     * 调用者必须在首次调用 {@link #next()} 之前调用 {@link #init()} 以初始化内部状态。
     * </p>
     *
     * @param tablePage 待扫描的数据页对象，不能为 {@code null}。
     *                  该页面应当已从磁盘加载至内存并处于可读状态。
     */
    public SeqScanExecutor(TablePage tablePage) {
        this.tablePage = tablePage;
    }

    /**
     * 初始化扫描器，重置游标至起始位置。
     * <p>
     * 此方法应在每次开始新的扫描流程前调用（通常由上层执行计划在 {@code plan.init()} 时逐层触发）。
     * 具体执行以下操作：
     * </p>
     * <ol>
     *   <li>将扫描游标 {@link #currentSlotId} 重置为 0，表示从页面的第一个槽位开始。</li>
     *   <li>从目标 {@link TablePage} 获取当前元组总数，并缓存至 {@link #maxSlotId}。
     *       该总数在扫描期间假设不会变化（页面在此期间被锁定或只读）。</li>
     * </ol>
     * <p>
     * 若多次调用 {@code init()}，扫描状态将被完全重置，可实现对同一页面的重复扫描。
     * </p>
     * <p>
     * 注意：作为叶子算子，本方法没有子算子需要初始化，因此不涉及向下传播。
     * </p>
     */
    @Override
    public void init() {
        // 重置游标：准备从第 0 号槽位开始顺序扫描
        this.currentSlotId = 0;
        // 缓存槽位总数：避免每次 next() 都调用 tablePage.getTupleCount()，提升微小性能
        this.maxSlotId = tablePage.getTupleCount();
    }

    /**
     * 获取页面中下一条有效的（未被删除的）元组。
     * <p>
     * 这是顺序扫描的核心方法，实现了带“逻辑删除跳过”机制的线性遍历。
     * 执行逻辑如下：
     * </p>
     * <ol>
     *   <li>检查游标是否已到达页面末尾（{@code currentSlotId < maxSlotId}）。
     *       若已到达，直接返回 {@code null}，表示 EOF。</li>
     *   <li>若未到达，调用底层 {@link TablePage#getTuple(int)} 读取当前游标指向槽位的元组。
     *       <b>无论读取结果如何，游标均立即向前推进一位（{@code currentSlotId++}）</b>。</li>
     *   <li>判断获取到的元组：
     *       <ul>
     *           <li>若为 {@code null}，说明该槽位对应的元组已被逻辑删除（或槽位为空）。
     *               此时不返回任何内容，继续循环执行步骤 1~3，直至找到有效元组或扫描结束。</li>
     *           <li>若非 {@code null}，说明获取到一条活着的元组，立即将其返回给上层调用者，
     *               并结束本次 {@code next()} 调用。</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * <h3>关于 {@code while} 循环</h3>
     * <p>
     * 循环条件 {@code currentSlotId < maxSlotId} 确保不会越界访问。
     * 循环内部每次迭代处理一个槽位，遇到被删除元组时自动跳过，直到找到有效数据或耗尽槽位。
     * 这种方式保证了上层算子永远不会看到已删除的元组，实现了逻辑删除的透明性。
     * </p>
     *
     * <h3>返回值语义</h3>
     * <ul>
     *   <li>返回有效 {@link Tuple}：表示成功读取到下一条存活数据，上层可继续调用 {@code next()} 获取后续数据。</li>
     *   <li>返回 {@code null}：表示当前页面所有槽位均已扫描完毕，无更多数据可提供。
     *       此时上层应停止调用或切换至下一数据页。</li>
     * </ul>
     *
     * <h3>性能与并发考量</h3>
     * <p>
     * 本方法不持有任何锁，仅读取共享的 {@link TablePage} 对象。在多线程环境下，
     * 调用方必须确保在扫描期间该页面不会被并发修改（例如插入、删除、更新导致槽位数组重组），
     * 否则可能导致脏读或越界。通常由上层 BufferPool 或事务管理器通过闩锁（Latch）保证。
     * </p>
     *
     * @return 下一条未删除的有效 {@link Tuple}；若页面已无更多数据则返回 {@code null}。
     */
    @Override
    public Tuple next() {
        // 持续扫描槽位，直到找到一个有效元组，或游标越界
        while (currentSlotId < maxSlotId) {
            // 从 TablePage 中读取当前游标指向槽位的元组
            // 注意：getTuple 内部会检查槽位的删除标记，若已删除则返回 null
            Tuple tuple = tablePage.getTuple(currentSlotId);

            // 无论元组是否有效，游标都必须向前推进，避免死循环并确保最终能扫描完所有槽位
            currentSlotId++;

            // 若元组有效（非 null），立即返回给上层
            if (tuple != null) {
                return tuple; // 找到一条存活数据，向上级汇报！
            }
            // 若元组为 null（已被逻辑删除），则继续循环，自动跳过该槽位
        }

        // 循环条件失败（currentSlotId >= maxSlotId），说明整个页面的所有槽位均已遍历完毕
        // 向上层返回 null，表示本页数据已耗尽（EOF）
        return null;
    }
}