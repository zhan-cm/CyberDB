package com.zhan.cyberdb.storage;

import com.zhan.cyberdb.common.Constants;

import java.nio.ByteBuffer;

/**
 * 表数据页（Table Page / Slotted Page）。
 * <p>
 * 采用经典的分槽页（Slotted Page）结构，用于在单个 4KB 磁盘页内存储多条变长记录（{@link Tuple}）。
 * 它本身不拥有底层物理存储，而是作为一个“工具类”包装一个原始的 {@link Page} 对象，
 * 通过 {@link ByteBuffer} 对该页的 {@code data} 字节数组进行结构化的读写。
 * </p>
 *
 * <h2>分槽页设计思想</h2>
 * <p>
 * 分槽页是数据库存储引擎中实现堆表（Heap Table）的经典内存布局。它将页面划分为两个相向增长的区域：
 * <ul>
 *   <li><b>槽位数组（Slot Array）</b>：从页面头部向后增长，每个槽位（8 字节）记录一条元组的起始偏移和长度。
 *       相当于目录，提供了 O(1) 的随机访问能力。</li>
 *   <li><b>元组数据区（Tuple Data Area）</b>：从页面底部（偏移 4096）向前增长，存储元组的实际字节内容。</li>
 *   <li><b>空闲空间（Free Space）</b>：位于槽位数组尾部与元组数据区顶部之间的连续区域。
 *       新插入的元组从空闲空间的底部开始分配，同时槽位数组在顶部追加一条目录项。</li>
 * </ul>
 * 这种设计带来两个关键优势：
 * <ol>
 *   <li><b>元组移动无需更新索引</b>：当页面内发生空间整理（Compact）时，只需更新槽位中的偏移量，
 *       上层索引通过槽位 ID（Slot ID）访问元组，不受物理移动影响。</li>
 *   <li><b>高效空间利用</b>：空闲空间连续，分配和释放操作均为 O(1)，且支持变长记录。</li>
 * </ol>
 *
 * <h2>页面物理布局</h2>
 * <pre>
 * ┌──────────────────────────────┬───────────────────────────────┬──────────────────────────────────────┐
 * │       Page Header (24 字节)   │      槽位数组 (Slot Array)       │              空闲空间                 │
 * │  前页ID/后页ID/空闲指针/行数等  │  [偏移,长度] [偏移,长度] ...       │             (未使用)                  │
 * ├──────────────────────────────┼───────────────────────────────┼──────────────────────────────────────┤
 * │                              │   ← 向下增长                   │               向上增长 →               │
 * └──────────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
 *                                                                       ↑
 *                                                              元组数据区 (Tuple Data)
 *                                                         实际记录的字节从页面底部(4096)向前堆放
 * </pre>
 *
 * <h2>与 BufferPoolManager 的交互</h2>
 * <p>
 * 所有对 {@code TablePage} 的修改最终都会调用 {@code page.setDirty(true)} 标记脏页。
 * 上层 {@link com.zhan.cyberdb.buffer.BufferPoolManager} 在淘汰此页或执行检查点时，
 * 会根据脏页标记决定是否调用 {@link DiskManager#writePage(int, Page)} 将修改持久化。
 * </p>
 *
 * <h2>逻辑删除策略</h2>
 * <p>
 * 删除元组时，本类仅将对应槽位的偏移和长度字段置为 0，并不立即回收物理空间。
 * 这是典型的逻辑删除（Logical Deletion），优点在于：
 * <ul>
 *   <li>删除操作极快，无需移动数据。</li>
 *   <li>事务回滚时仅需恢复槽位信息，成本低廉。</li>
 * </ul>
 * 被标记删除的空间将在未来的空间整理（Vacuum / Compact）过程中回收。
 * </p>
 *
 * @author Zhan
 * @version 1.0
 * @see Page
 * @see Tuple
 * @see Constants#PAGE_SIZE
 */
public class TablePage {

    /**
     * 底层物理页对象。
     * <p>
     * 本类不持有数据，所有对页面内容的读写最终都通过操作 {@code page.getData()} 字节数组完成。
     * 脏页标记也直接设置在该对象上。
     * </p>
     */
    private Page page;

    // =========================================================
    // Page Header 结构定义（固定占用 24 字节）
    // =========================================================

    /**
     * 前一个兄弟页面的 ID（偏移量 0~3）。
     * <p>
     * 用于将同一张表的多个数据页串联成双向链表，以支持全表扫描和范围查询。
     * 若当前页为链表首部，则值为 {@link Constants#INVALID_PAGE_ID}（-1）。
     * </p>
     */
    private static final int OFFSET_PREV_PAGE_ID = 0;

    /**
     * 后一个兄弟页面的 ID（偏移量 4~7）。
     * <p>
     * 若当前页为链表尾部，则值为 {@code -1}。
     * </p>
     */
    private static final int OFFSET_NEXT_PAGE_ID = 4;

    /**
     * 空闲空间起始指针（偏移量 8~11）。
     * <p>
     * 记录当前空闲区域的“底部”偏移量，即下一条元组数据应当被放置的起始位置。
     * 初始值为 {@link Constants#PAGE_SIZE}（4096），表示空闲空间从页面最底部开始。
     * 每次插入元组时，该指针向低地址移动（减少），其值永远大于等于槽位数组尾部的偏移。
     * </p>
     * <p>
     * 计算公式：空闲空间大小 = {@code freeSpacePointer - (SIZE_PAGE_HEADER + tupleCount * SIZE_SLOT)}。
     * 若结果小于待插入元组所需空间，则插入失败。
     * </p>
     */
    private static final int OFFSET_FREE_SPACE = 8;

    /**
     * 当前页面中存储的元组总数（偏移量 12~15）。
     * <p>
     * 即槽位数组的有效长度。注意：此计数<b>包含已被逻辑删除的元组</b>。
     * 逻辑删除仅将槽位内容清零，并不减少此计数。
     * 若要获取有效元组数量，需遍历槽位数组并检查偏移量是否为 0。
     * </p>
     */
    private static final int OFFSET_TUPLE_COUNT = 12;

    /**
     * 日志序列号 / 事务相关预留字段（偏移量 16~19）。
     * <p>
     * 在支持事务和崩溃恢复的完整实现中，此字段用于存储页面的最新 LSN（Log Sequence Number），
     * 以实现 WAL（Write-Ahead Logging）协议。当前版本未使用，预留为 0。
     * </p>
     */
    private static final int OFFSET_LSN = 16;

    /**
     * 页面校验和 / 魔数（偏移量 20~23）。
     * <p>
     * 用于检测页面是否因磁盘故障或软件 Bug 而损坏。可从数据库启动时校验。
     * 当前版本未使用，预留为 0。
     * </p>
     */
    private static final int OFFSET_MAGIC = 20;

    /**
     * 页面头部总大小（24 字节）。
     * <p>
     * 槽位数组从此偏移量开始。
     * </p>
     */
    private static final int SIZE_PAGE_HEADER = 24;

    // =========================================================
    // Slot（槽位）结构定义
    // =========================================================

    /**
     * 单个槽位（Slot）占用的字节数（8 字节）。
     * <p>
     * 每个槽位由两个连续的 4 字节整数组成：
     * <ul>
     *   <li>前 4 字节：元组在页面中的起始偏移量（Offset）。</li>
     *   <li>后 4 字节：元组的实际字节长度（Size）。</li>
     * </ul>
     * 若偏移量和长度均为 0，表示该槽位对应的元组已被逻辑删除。
     * </p>
     */
    private static final int SIZE_SLOT = 8;

    /**
     * 构造一个表数据页视图，绑定到底层物理页。
     *
     * @param page 底层物理页对象，其 {@code data} 数组必须已分配 4KB 空间。
     */
    public TablePage(Page page) {
        this.page = page;
    }

    /**
     * 初始化一个全新的空页面。
     * <p>
     * 此方法会设置页面头部的基础字段，将空闲指针指向页面底部（4096），元组计数置为 0，
     * 并建立与前后兄弟页面的链表关系。调用后页面即处于可插入状态。
     * </p>
     * <p>
     * 注意：本方法<b>不会</b>清空数据区内容（新页的 {@code data} 数组在构造 {@link Page} 时已全为 0）。
     * 但若复用已淘汰的旧页，上层应确保已调用 {@link Page#resetMemory()} 清零。
     * </p>
     *
     * @param prevPageId 前一个兄弟页面的 ID，若为链表首部则传 {@code -1}。
     * @param nextPageId 后一个兄弟页面的 ID，若为链表尾部则传 {@code -1}。
     */
    public void init(int prevPageId, int nextPageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());

        // 写入前后页指针
        buf.position(OFFSET_PREV_PAGE_ID);
        buf.putInt(prevPageId);

        buf.position(OFFSET_NEXT_PAGE_ID);
        buf.putInt(nextPageId);

        // 空闲空间指针初始指向页面最底部（4096）
        buf.position(OFFSET_FREE_SPACE);
        buf.putInt(Constants.PAGE_SIZE);

        // 初始元组数量为 0
        buf.position(OFFSET_TUPLE_COUNT);
        buf.putInt(0);

        // LSN 和 Magic 字段暂时未使用，但为保持一致性，可显式置零
        buf.position(OFFSET_LSN);
        buf.putInt(0);

        buf.position(OFFSET_MAGIC);
        buf.putInt(0);

        // 注意：此时页面内容已改变，但调用者（如 BufferPoolManager）通常会在分配新页后立即标记脏页。
        // 本方法内不主动标记，由上层负责。
    }

    /**
     * 向页面中插入一行新的元组（Tuple）。
     * <p>
     * 此方法执行以下步骤：
     * <ol>
     *   <li>计算所需空间（元组长度 + 一个新槽位）。</li>
     *   <li>检查空闲空间是否足够，若不足则返回 {@code false}。</li>
     *   <li>将元组数据拷贝到空闲区域底部（从新的空闲指针位置开始）。</li>
     *   <li>在槽位数组尾部追加一条新槽位，记录元组的偏移和长度。</li>
     *   <li>更新空闲指针和元组计数。</li>
     *   <li>标记底层页为脏页。</li>
     * </ol>
     * 插入操作是 O(1) 的，且不涉及数据移动。
     * </p>
     *
     * <h3>空间计算公式详解</h3>
     * <pre>
     * 已用头部空间 = 固定头部(24) + 现有槽位数 * 8
     * 空闲空间总量 = 当前空闲指针 - 已用头部空间
     * 所需空间 = 元组长度 + 8（一个新槽位）
     * 若 空闲空间总量 ≥ 所需空间，则可插入。
     * 新数据写入偏移量 = 当前空闲指针 - 元组长度
     * </pre>
     *
     * @param tuple 待插入的元组对象，其内部数据字节数组不应为 {@code null}。
     * @return {@code true} 表示插入成功；{@code false} 表示页面空间不足，插入失败。
     */
    public boolean insertTuple(Tuple tuple) {
        int tupleSize = tuple.getLength();

        // 1. 获取当前页面状态
        int freeSpacePointer = getFreeSpacePointer();
        int tupleCount = getTupleCount();

        // 2. 精确空间计算
        // 已使用的头部区域（固定头部 + 现有槽位数组）
        int usedHeaderSpace = SIZE_PAGE_HEADER + tupleCount * SIZE_SLOT;
        // 中间剩余的空闲空间（单位：字节）
        int freeSpace = freeSpacePointer - usedHeaderSpace;

        // 插入需要：元组自身数据 + 为它新建的一个槽位（8字节）
        if (freeSpace < tupleSize + SIZE_SLOT) {
            // 空间不足，插入失败。上层应尝试分裂页面或分配新页。
            return false;
        }

        // 3. 计算新元组的物理写入起始位置（向低地址移动）
        int newFreeSpacePointer = freeSpacePointer - tupleSize;

        // 4. 将元组数据拷贝到页面底部的数据区
        //    System.arraycopy 是 Java 底层内存块拷贝，效率极高
        System.arraycopy(tuple.getData(), 0,
                page.getData(), newFreeSpacePointer,
                tupleSize);

        // 5. 在槽位数组尾部追加新槽位
        int newSlotOffset = SIZE_PAGE_HEADER + tupleCount * SIZE_SLOT;
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(newSlotOffset);
        buf.putInt(newFreeSpacePointer); // 槽位记录1：元组起始偏移
        buf.putInt(tupleSize);           // 槽位记录2：元组长度

        // 6. 更新页面头部统计信息
        setFreeSpacePointer(newFreeSpacePointer);
        setTupleCount(tupleCount + 1);

        // 7. ⚠️ 关键：标记脏页，确保持久化
        page.setDirty(true);

        return true;
    }

    /**
     * 根据槽位号（Slot ID）读取一行元组。
     * <p>
     * 槽位号即元组在槽位数组中的逻辑索引，从 0 开始。此方法提供 O(1) 的直接访问。
     * 若槽位对应的元组已被逻辑删除（偏移量和长度均为 0），则返回 {@code null}。
     * </p>
     *
     * <h3>逻辑删除的透明性</h3>
     * <p>
     * 上层调用者（如扫描器）通过此方法获取元组，无需关心槽位是否已被删除。
     * 本方法自动过滤已删除元组，返回 {@code null}，由调用者决定是否跳过。
     * </p>
     *
     * @param slotId 槽位号，合法范围为 {@code 0} 到 {@code getTupleCount() - 1}。
     * @return 读取到的元组对象；若槽位号越界或元组已被删除，则返回 {@code null}。
     */
    public Tuple getTuple(int slotId) {
        int tupleCount = getTupleCount();
        // 越界检查
        if (slotId < 0 || slotId >= tupleCount) {
            return null;
        }

        // 1. 定位槽位在字节数组中的偏移
        int slotOffset = SIZE_PAGE_HEADER + slotId * SIZE_SLOT;

        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(slotOffset);

        // 2. 读取槽位内容：偏移量 + 长度
        int tupleOffset = buf.getInt();
        int tupleSize = buf.getInt();

        // 3. 逻辑删除检查：若偏移量或长度为 0，表示元组已被删除
        if (tupleOffset == 0 || tupleSize == 0) {
            return null;
        }

        // 4. 从数据区拷贝出元组字节内容
        byte[] tupleData = new byte[tupleSize];
        System.arraycopy(page.getData(), tupleOffset, tupleData, 0, tupleSize);

        return new Tuple(tupleData);
    }

    /**
     * 逻辑删除指定槽位号的元组。
     * <p>
     * 此方法不释放物理空间，仅将槽位中的偏移量和长度字段均置为 0，
     * 并标记底层页为脏页。被删除的空间不会立即回收，需等待后续的空间整理操作。
     * </p>
     * <p>
     * 重复删除同一槽位是幂等的：若槽位已被删除（偏移量为 0），则直接返回 {@code false}。
     * </p>
     *
     * @param slotId 待删除元组的槽位号。
     * @return {@code true} 表示删除成功；{@code false} 表示槽位越界或元组此前已被删除。
     */
    public boolean deleteTuple(int slotId) {
        int tupleCount = getTupleCount();
        if (slotId < 0 || slotId >= tupleCount) {
            return false;
        }

        int slotOffset = SIZE_PAGE_HEADER + slotId * SIZE_SLOT;
        ByteBuffer buf = ByteBuffer.wrap(page.getData());

        // 读取当前偏移量，判断是否已被删除
        buf.position(slotOffset);
        int tupleOffset = buf.getInt();

        if (tupleOffset == 0) {
            // 已经逻辑删除，幂等返回 false
            return false;
        }

        // 执行逻辑删除：将槽位的偏移和长度均置为 0
        buf.position(slotOffset);
        buf.putInt(0);
        buf.putInt(0);

        // 标记脏页
        page.setDirty(true);
        return true;
    }

    // ==================== 辅助 Getter / Setter ====================

    /**
     * 获取当前页面中存储的元组总数（包含已逻辑删除的槽位）。
     * <p>
     * 此值等于槽位数组的长度。若要获取有效（存活）元组数，需遍历槽位数组统计偏移量非 0 的槽位数量。
     * </p>
     *
     * @return 槽位总数。
     */
    public int getTupleCount() {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        return buf.getInt(OFFSET_TUPLE_COUNT);
    }

    /**
     * 设置元组总数（内部使用）。
     *
     * @param count 新的槽位数量。
     */
    private void setTupleCount(int count) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_TUPLE_COUNT);
        buf.putInt(count);
    }

    /**
     * 获取当前空闲空间指针的值。
     * <p>
     * 该指针指向数据区中下一个可用字节的起始偏移量（即当前已使用数据区的最低地址）。
     * 新元组将从此偏移量减去自身长度后的位置开始写入。
     * </p>
     *
     * @return 空闲空间起始偏移量。
     */
    public int getFreeSpacePointer() {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        return buf.getInt(OFFSET_FREE_SPACE);
    }

    /**
     * 更新空闲空间指针（内部使用）。
     *
     * @param freeSpacePointer 新的空闲空间起始偏移量。
     */
    private void setFreeSpacePointer(int freeSpacePointer) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_FREE_SPACE);
        buf.putInt(freeSpacePointer);
    }

    /**
     * 获取前一个兄弟页面的 ID。
     *
     * @return 前一个页面的 ID，若当前为首部则返回 -1。
     */
    public int getPrevPageId() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_PREV_PAGE_ID);
    }

    /**
     * 获取后一个兄弟页面的 ID。
     *
     * @return 后一个页面的 ID，若当前为尾部则返回 -1。
     */
    public int getNextPageId() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_NEXT_PAGE_ID);
    }

    /**
     * 设置后一个兄弟页面的 ID。
     * <p>
     * 通常在页面分裂或链表重组时调用。调用后需标记脏页。
     * </p>
     *
     * @param nextPageId 后一个页面的 ID。
     */
    public void setNextPageId(int nextPageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_NEXT_PAGE_ID);
        buf.putInt(nextPageId);
        page.setDirty(true);
    }

    /**
     * 获取底层物理页对象。
     * <p>
     * 通常用于传递给缓冲池管理器进行 unpin 操作。
     * </p>
     *
     * @return 底层 Page 对象。
     */
    public Page getPage() {
        return page;
    }
}