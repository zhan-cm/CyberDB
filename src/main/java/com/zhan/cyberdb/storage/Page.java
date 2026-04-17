package com.zhan.cyberdb.storage;

import com.zhan.cyberdb.common.Constants;

import java.util.Arrays;

/**
 * 数据库内存中的基本存储单元：页（Page）。
 * <p>
 * 页是 CyberDB 存储引擎在内存与磁盘之间交换数据的最小单位，固定大小为 4KB（{@link Constants#PAGE_SIZE}）。
 * 无论是表数据、索引节点还是其他元数据，最终都以页的形式存在于缓冲池和磁盘文件中。
 * </p>
 *
 * <h2>页的生命周期</h2>
 * <ol>
 *   <li><b>创建</b>：由 {@link com.zhan.cyberdb.buffer.BufferPoolManager} 在初始化缓冲池时批量创建空页，
 *       其 {@code pageId} 为 {@link Constants#INVALID_PAGE_ID}，处于“空闲”状态。</li>
 *   <li><b>分配</b>：当需要加载新数据时，缓冲池会从空闲槽位中挑选一个页，调用
 *       {@link com.zhan.cyberdb.storage.DiskManager#readPage(int, Page)} 将磁盘数据填充进 {@code data}，
 *       并设置有效的 {@code pageId}。</li>
 *   <li><b>使用</b>：上层模块通过缓冲池获取页的引用，此时页的 {@code pinCount} 增加，
 *       防止被淘汰。期间可能修改数据，导致 {@code isDirty} 被标记为 {@code true}。</li>
 *   <li><b>释放</b>：使用完毕后，上层调用 {@code unpinPage} 减少引用计数。当 {@code pinCount} 降为 0，
 *       页面进入 LRU 淘汰候选队列。</li>
 *   <li><b>淘汰</b>：若缓冲池空间不足，LRU 会选出淘汰页。若该页为脏页，则先写回磁盘，
 *       然后调用 {@link #resetMemory()} 清空内容，使其重新变为空闲状态。</li>
 * </ol>
 *
 * <h2>字段详解</h2>
 * <ul>
 *   <li><b>pageId</b>：页面的全局唯一标识符，对应磁盘文件中的逻辑页号。
 *       有效值从 0 开始递增，{@link Constants#INVALID_PAGE_ID}（-1）表示空闲页。</li>
 *   <li><b>data</b>：4KB 的字节数组，承载页面的实际内容。上层模块通过包装类
 *       （如 {@link com.zhan.cyberdb.index.bplus.BPlusTreePage}）来解析此数组中的结构化数据。</li>
 *   <li><b>isDirty</b>：脏页标记。若为 {@code true}，表示内存中的数据比磁盘上的新，
 *       必须在页被淘汰或系统关闭前写回磁盘，否则修改将丢失。</li>
 *   <li><b>pinCount</b>：引用计数（钉住计数）。表示当前有多少线程/任务正在使用此页。
 *       任何大于 0 的计数都会阻止页被 LRU 淘汰，是并发控制与缓存管理的基础保障。</li>
 * </ul>
 *
 * <h2>线程安全性</h2>
 * <p>
 * 本类的字段（如 {@code pinCount}、{@code isDirty}）并非线程安全。
 * 对它们的并发修改必须由外部锁（例如缓冲池的页级闩锁）来同步。
 * </p>
 *
 * @author Zhan
 * @version 1.0
 * @see Constants#PAGE_SIZE
 * @see Constants#INVALID_PAGE_ID
 * @see com.zhan.cyberdb.buffer.BufferPoolManager
 * @see com.zhan.cyberdb.storage.DiskManager
 */
public class Page {

    /**
     * 页面标识符（Page ID）。
     * <p>
     * 该值在页被分配有效数据时设置，与磁盘文件中的逻辑页号一一对应。
     * 当页面处于空闲状态（未被任何数据占用）时，此值为 {@link Constants#INVALID_PAGE_ID}（-1）。
     * </p>
     * <p>
     * 页面 ID 由 {@link DiskManager#allocatePage()} 生成，从 0 开始单调递增。
     * 在数据库生命周期内，每个页面 ID 唯一标识一个 4KB 的存储单元。
     * </p>
     */
    private int pageId;

    /**
     * 页面数据缓冲区（4KB 字节数组）。
     * <p>
     * 这是页面的核心存储区域，所有持久化数据都存放在这个字节数组中。
     * 上层模块（如 B+ 树、堆表）通过特定的包装类（如 {@code BPlusTreeLeafPage}）
     * 来读写此数组中的结构化字段。
     * </p>
     * <p>
     * 数组长度固定为 {@link Constants#PAGE_SIZE}（4096 字节），在页对象构造时分配，
     * 此后长度不变。当页面被淘汰复用时，数组内容会被清零，但数组对象本身不会被回收，
     * 以减少 GC 压力。
     * </p>
     */
    private byte[] data;

    /**
     * 脏页标记（Dirty Flag）。
     * <p>
     * 标记此页面在内存中是否已被修改，且修改尚未同步到磁盘。
     * <ul>
     *   <li>{@code true}：内存数据比磁盘新。在页面被淘汰或系统关闭前，必须通过
     *       {@link DiskManager#writePage(int, Page)} 写回磁盘。</li>
     *   <li>{@code false}：内存数据与磁盘一致（或页面从未被写入过磁盘），淘汰时无需回写。</li>
     * </ul>
     * 任何对 {@code data} 数组的写操作都应显式调用 {@link #setDirty(boolean)} 设置为 {@code true}。
     * 在通过 {@link DiskManager#writePage(int, Page)} 成功写入磁盘后，该标记会被重置为 {@code false}。
     * </p>
     */
    private boolean isDirty;

    /**
     * 引用计数 / 钉住计数（Pin Count）。
     * <p>
     * 记录当前有多少个线程或任务正在使用（引用）此页面。
     * 这是缓冲池淘汰策略的“安全锁”——任何引用计数大于 0 的页面都<b>不能被淘汰</b>，
     * 即使它是 LRU 链表中最久未使用的页面。
     * </p>
     * <p>
     * 典型生命周期：
     * <ul>
     *   <li>当页面被 {@link com.zhan.cyberdb.buffer.BufferPoolManager#fetchPage(int)} 返回给调用者时，
     *       引用计数 +1。</li>
     *   <li>当调用者完成操作，调用 {@code unpinPage} 时，引用计数 -1。</li>
     *   <li>引用计数降为 0 时，页面被加入 LRU 淘汰候选队列，允许被淘汰。</li>
     * </ul>
     * 引用计数的正确维护是防止数据丢失和系统死锁的关键。任何未配对的 pin/unpin 都会导致
     * 页面“永久钉住”，最终耗尽缓冲池。
     * </p>
     */
    private int pinCount;

    /**
     * 构造一个空的页对象。
     * <p>
     * 初始化时，页面 ID 被设置为无效值（{@link Constants#INVALID_PAGE_ID}），
     * 数据数组分配 4KB 空间并填充 0，脏标记为 {@code false}，引用计数为 0。
     * 此状态表示一个“空闲”页槽，可供缓冲池分配使用。
     * </p>
     */
    public Page() {
        this.pageId = Constants.INVALID_PAGE_ID;
        this.data = new byte[Constants.PAGE_SIZE];
        this.isDirty = false;
        this.pinCount = 0;
    }

    /**
     * 重置页面内容，使其恢复到空闲状态。
     * <p>
     * 当缓冲池需要淘汰一个页面，并将其槽位复用于加载新页时，会调用此方法。
     * 执行以下清理操作：
     * <ul>
     *   <li>将 {@code data} 数组的所有字节置为 0（清除旧数据）。</li>
     *   <li>重置脏标记为 {@code false}。</li>
     *   <li>重置引用计数为 0。</li>
     *   <li>将页面 ID 设为 {@link Constants#INVALID_PAGE_ID}。</li>
     * </ul>
     * 调用此方法前，应确保页面若为脏页，已通过 {@link DiskManager#writePage(int, Page)} 写回磁盘。
     * 此方法不释放 {@code data} 数组本身，仅清空其内容，以便复用。
     * </p>
     */
    public void resetMemory() {
        // 使用高效的系统级方法将整个数组清零
        Arrays.fill(data, (byte) 0);
        isDirty = false;
        pinCount = 0;
        pageId = Constants.INVALID_PAGE_ID;
    }

    // ==================== 标准 Getter 和 Setter ====================

    /**
     * 获取当前页面的标识符。
     *
     * @return 页面 ID；若页面空闲则返回 {@link Constants#INVALID_PAGE_ID}（-1）。
     */
    public int getPageId() {
        return pageId;
    }

    /**
     * 设置当前页面的标识符。
     * <p>
     * 通常在从磁盘读取页面数据后，由 {@link DiskManager#readPage(int, Page)} 调用。
     * </p>
     *
     * @param pageId 页面 ID，必须为非负整数或 {@link Constants#INVALID_PAGE_ID}。
     */
    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    /**
     * 获取页面数据缓冲区的引用。
     * <p>
     * 返回的是内部 {@code data} 数组的直接引用，非防御性拷贝。
     * 调用者可以修改数组内容，但修改后<b>必须</b>调用 {@link #setDirty(boolean)} 标记脏页，
     * 否则修改可能丢失。
     * </p>
     *
     * @return 长度为 {@link Constants#PAGE_SIZE} 的字节数组。
     */
    public byte[] getData() {
        return data;
    }

    /**
     * 查询页面是否为脏页（即内存数据比磁盘新）。
     *
     * @return {@code true} 如果页面自上次从磁盘读取或写入后已被修改；否则 {@code false}。
     */
    public boolean isDirty() {
        return isDirty;
    }

    /**
     * 设置页面的脏标记。
     * <p>
     * 任何对 {@code data} 数组的修改操作都应调用此方法并传入 {@code true}。
     * 当页面被成功写回磁盘后，应调用此方法并传入 {@code false}。
     * </p>
     *
     * @param dirty {@code true} 表示页面已修改；{@code false} 表示页面与磁盘同步。
     */
    public void setDirty(boolean dirty) {
        isDirty = dirty;
    }

    /**
     * 获取当前页面的引用计数（被钉住的次数）。
     *
     * @return 当前引用计数。0 表示页面可被安全淘汰。
     */
    public int getPinCount() {
        return pinCount;
    }

    /**
     * 设置页面的引用计数。
     * <p>
     * 通常由缓冲池管理器在获取或释放页面时调用。建议使用原子操作或加锁保护。
     * </p>
     *
     * @param pinCount 新的引用计数值，必须为非负整数。
     */
    public void setPinCount(int pinCount) {
        this.pinCount = pinCount;
    }
}