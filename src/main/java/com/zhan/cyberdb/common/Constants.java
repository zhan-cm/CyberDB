package com.zhan.cyberdb.common;

/**
 * CyberDB 全局常量配置类。
 *
 * <h2>设计目的</h2>
 * 集中管理系统中所有模块共享的常量定义，避免硬编码分散在各处，便于统一调整和维护。
 * 例如页面大小、无效标识符等关键参数，一旦修改可全局生效。
 *
 * <h2>使用规范</h2>
 * 所有需要跨模块共享的常量均应在此类中定义为 {@code public static final} 字段，
 * 并通过 {@code Constants.XXX} 的方式进行引用。
 *
 * <h2>扩展说明</h2>
 * 未来如需添加配置项（如日志开关、调试标志等），应继续在本类中追加，保持命名风格一致。
 *
 * @author Zhan
 * @version 1.0
 * @see com.zhan.cyberdb.storage.Page
 * @see com.zhan.cyberdb.buffer.BufferPoolManager
 */
public class Constants {

    /**
     * 页面大小（Page Size）。
     *
     * <h3>取值：4096 字节（4KB）</h3>
     *
     * <h3>为什么选择 4KB？</h3>
     * <ul>
     *   <li><b>操作系统/文件系统对齐</b>：绝大多数现代操作系统和文件系统的内存页（Page）与磁盘块（Block）
     *       默认大小为 4KB。将数据库页面大小设置为 4KB 可以完美对齐，避免读写跨越多个物理块，
     *       减少 I/O 次数和碎片化。</li>
     *   <li><b>磁盘 I/O 效率</b>：机械硬盘（HDD）和固态硬盘（SSD）的物理扇区大小通常为 512 字节或 4KB，
     *       以 4KB 为单位进行读写能达到最佳吞吐量。</li>
     *   <li><b>内存管理友好</b>：Java 虚拟机虽不直接暴露物理页，但 4KB 的对象数组分配与垃圾回收压力较小，
     *       且便于与操作系统的内存映射（Memory-Mapped File）机制对接。</li>
     *   <li><b>数据库惯例</b>：多数经典数据库（如 PostgreSQL 默认 8KB，MySQL InnoDB 默认 16KB）
     *       均采用 2 的幂次大小。4KB 是常见的最小粒度，适合轻量级存储引擎。</li>
     * </ul>
     *
     * <h3>影响范围</h3>
     * <ul>
     *   <li>{@link com.zhan.cyberdb.storage.Page#data} 数组的长度即由此常量决定。</li>
     *   <li>{@link com.zhan.cyberdb.storage.DiskManager} 在读写磁盘文件时，以此大小作为单次 I/O 的传输单位。</li>
     *   <li>缓冲池中每个槽位占用的内存大小约等于 {@code PAGE_SIZE} 加上对象头开销。</li>
     *   <li>若修改此值，必须<b>重建数据库文件</b>，因为已有文件的页面布局将不再兼容。</li>
     * </ul>
     */
    public static final int PAGE_SIZE = 4096;

    /**
     * 无效页面标识符（Invalid Page ID）。
     *
     * <h3>取值：-1</h3>
     *
     * <h3>作用与语义</h3>
     * <ul>
     *   <li><b>槽位空闲标志</b>：在 {@link com.zhan.cyberdb.buffer.BufferPoolManager#pages} 数组中，
     *       若某槽位对应的 {@link com.zhan.cyberdb.storage.Page#getPageId()} 返回此值，
     *       表示该槽位当前未被任何有效页面占用，处于“空闲”状态。</li>
     *   <li><b>初始化占位</b>：新创建的 {@code Page} 对象的 {@code pageId} 初始值即为此常量，
     *       表明其尚未与任何磁盘页面关联。</li>
     *   <li><b>边界条件判断</b>：在淘汰、查找、重置等流程中，通过检查 {@code pageId == INVALID_PAGE_ID}
     *       来识别可复用的空槽位。</li>
     *   <li><b>哨兵值（Sentinel Value）</b>：{@link com.zhan.cyberdb.buffer.LRUCache} 的虚拟头尾节点
     *       也使用 {@code -1} 作为无效页面 ID，与此常量语义一致。</li>
     * </ul>
     *
     * <h3>为什么是 -1？</h3>
     * 因为合法的页面 ID 通常由 {@link com.zhan.cyberdb.storage.DiskManager#allocatePage()} 生成，
     * 从 0 开始递增，因此任何负数都可作为无效标识。选用 -1 是业界通用惯例，简洁直观。
     *
     * <h3>注意事项</h3>
     * 严禁将任何有效页面分配为 -1，否则会导致逻辑错误。{@code DiskManager} 必须保证分配的 ID 永远不为负数。
     */
    public static final int INVALID_PAGE_ID = -1;
}