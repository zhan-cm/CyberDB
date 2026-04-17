package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.storage.Page;

import java.nio.ByteBuffer;

/**
 * B+ 树叶子节点（Leaf Node）。
 *
 * <h2>核心定位</h2>
 * 叶子节点是 B+ 树中真正存储<b>用户数据（键值对）</b>的节点，位于树的最底层。
 * 与内部节点（{@link BPlusTreeInternalPage}）不同，叶子节点不包含指向子节点的指针，
 * 而是直接存储索引键（Key）和对应的数据值（Value，例如行记录的物理位置）。
 *
 * <h2>主要职责</h2>
 * <ul>
 *   <li><b>数据存储</b>：以有序数组的形式存储键值对，支持按 Key 顺序遍历。</li>
 *   <li><b>有序插入</b>：在保持键有序的前提下插入新键值对，必要时移动后续元素以腾出空间。</li>
 *   <li><b>水平链表</b>：通过 {@code NextPageId} 字段将所有叶子节点串联成单向链表，
 *       以支持高效的范围查询（Range Scan）。</li>
 *   <li><b>分裂准备</b>：提供插入后容量检测（通过父类的 {@code isFull()}），
 *       为上层 {@link BPlusTree} 的节点分裂逻辑提供支持。</li>
 * </ul>
 *
 * <h2>在磁盘上的物理布局</h2>
 * 一个叶子节点占用完整的 4KB 磁盘页（{@link Page#getData()}），其字节布局如下：
 * <pre>
 * ┌──────────────────────────────┬────────────────────┬───────────────────────────────────────┐
 * │      父类 Header (24 字节)     │  NextPageId (4字节) │           KV 对数组（连续存储）           │
 * ├──────────────────────────────┼────────────────────┼───────────────────────────────────────┤
 * │ pageType, pageId, parentId,  │ 指向下一个叶子节点的  │ [key0][value0][key1][value1] ... [keyN][valueN] │
 * │ currentSize, maxSize, ...    │    页面 ID          │                                       │
 * └──────────────────────────────┴────────────────────┴───────────────────────────────────────┘
 * </pre>
 * <ul>
 *   <li><b>父类 Header（24 字节）</b>：继承自 {@link BPlusTreePage}，包含页面类型、ID、父子关系、
 *       当前大小、最大容量等元数据。具体字段布局见父类文档。</li>
 *   <li><b>NextPageId（4 字节）</b>：叶子节点特有的字段，存储逻辑上下一个叶子节点的页面 ID。
 *       若当前节点为链表尾部，则值为 {@code -1}。此设计使得范围查询仅需单向遍历叶子层，
 *       无需回溯内部节点，极大提升区间扫描效率。</li>
 *   <li><b>KV 对数组</b>：从偏移量 28 开始，连续存储 {@code currentSize} 个键值对。
 *       每个键值对由 4 字节的 Key（int）和 4 字节的 Value（int）组成，合计 8 字节。
 *       Key 在数组中保持严格升序排列，以保证二分查找和范围扫描的正确性。</li>
 * </ul>
 *
 * <h2>字段大小说明</h2>
 * 所有字段大小均为硬编码常量，修改需同时调整偏移计算逻辑。
 * <ul>
 *   <li>{@link #SIZE_LEAF_PAGE_HEADER}：28 字节（父类 24 + NextPageId 4）。</li>
 *   <li>{@link #SIZE_KEY}：4 字节（int）。</li>
 *   <li>{@link #SIZE_VALUE}：4 字节（int）。</li>
 *   <li>{@link #SIZE_KV_PAIR}：8 字节（Key + Value）。</li>
 * </ul>
 *
 * @author Zhan
 * @version 1.0
 * @see BPlusTreePage
 * @see BPlusTreeInternalPage
 * @see Page
 */
public class BPlusTreeLeafPage extends BPlusTreePage {

    /**
     * NextPageId 字段相对于页面数据起始位置（偏移量 0）的字节偏移。
     * <p>
     * 由于父类 {@link BPlusTreePage} 的头部固定占用 24 字节，
     * 因此 NextPageId 紧接其后，偏移量为 24。
     * </p>
     * <p>
     * 该字段存储下一个叶子节点的页面 ID（32 位整数），用于构建叶子层单向链表。
     * 若当前节点为链表末尾，则值为 {@code -1}（或 {@link com.zhan.cyberdb.common.Constants#INVALID_PAGE_ID}）。
     * </p>
     */
    private static final int OFFSET_NEXT_PAGE_ID = 24;

    /**
     * 叶子节点专用头部（Leaf Page Header）的总字节大小。
     * <p>
     * 计算公式：父类头部 24 字节 + NextPageId 字段 4 字节 = 28 字节。
     * 此常量用于计算第一个键值对在字节数组中的起始偏移量。
     * </p>
     */
    private static final int SIZE_LEAF_PAGE_HEADER = 28;

    /**
     * 单个键（Key）的字节大小。
     * <p>
     * 在 CyberDB 的简化实现中，键固定为 32 位有符号整数（int），占用 4 字节。
     * </p>
     */
    private static final int SIZE_KEY = 4;

    /**
     * 单个值（Value）的字节大小。
     * <p>
     * 值同样固定为 32 位整数（int），占用 4 字节。在实际应用中，Value 通常存储
     * 行记录的物理地址（如页号 + 槽位号），此处简化为直接存储整数。
     * </p>
     */
    private static final int SIZE_VALUE = 4;

    /**
     * 一个完整键值对（Key-Value Pair）的字节大小。
     * <p>
     * 等于 {@link #SIZE_KEY} + {@link #SIZE_VALUE} = 8 字节。
     * 叶子节点中所有键值对紧密排列，无填充字节。
     * </p>
     */
    private static final int SIZE_KV_PAIR = SIZE_KEY + SIZE_VALUE;

    /**
     * 构造一个叶子节点视图，包装一个底层磁盘页。
     * <p>
     * 此构造方法不执行任何初始化逻辑，仅将传入的 {@link Page} 对象保存到父类的 {@code page} 字段中。
     * 若该页面是全新分配的，调用者必须在构造后立即调用 {@link #init(int, int, int)} 进行初始化。
     * 若该页面是从磁盘读取的已有节点，则其字节数据已包含有效头部和 KV 数组，可直接调用读取方法。
     * </p>
     *
     * @param page 底层物理页对象，其 {@code data} 字段必须已分配 4KB 空间。不能为 {@code null}。
     */
    public BPlusTreeLeafPage(Page page) {
        super(page);
    }

    /**
     * 初始化一个全新的叶子节点（通常在分配新页面后调用）。
     * <p>
     * 该方法会向页面的头部写入节点类型、自身 ID、父节点 ID、当前大小（0）、最大容量，
     * 并将 {@code NextPageId} 初始化为 {@code -1}（表示暂无后继节点）。
     * 此后，KV 数组区域的内容为未初始化的垃圾数据，后续将通过 {@link #insert(int, int)} 进行填充。
     * </p>
     *
     * @param pageId   当前节点的页面 ID。该 ID 应由 {@link com.zhan.cyberdb.buffer.BufferPoolManager#allocatePage()} 分配。
     * @param parentId 父节点的页面 ID。若当前节点为根节点（树只有一层），则传 {@code -1}。
     * @param maxSize  节点最大容量（即最多可容纳的键值对数量）。此值决定了节点何时触发分裂。
     */
    public void init(int pageId, int parentId, int maxSize) {
        // 设置父类继承的通用元数据
        setPageType(PageType.LEAF);       // 标记为叶子节点
        setPageId(pageId);                // 记录自身页面 ID
        setParentPageId(parentId);        // 记录父节点 ID
        setCurrentSize(0);                // 初始时无任何键值对
        setMaxSize(maxSize);              // 设定容量上限

        // 初始化叶子节点特有的 NextPageId 字段为 -1，表示链表尾部
        setNextPageId(-1);
    }

    // ==================== Next Page ID 的 Getter / Setter ====================

    /**
     * 获取下一个叶子节点的页面 ID。
     * <p>
     * 此方法用于支持范围查询时的叶子节点间跳转。若返回 {@code -1}，表示当前节点是链表中的最后一个节点。
     * </p>
     *
     * @return 下一个叶子节点的页面 ID；若无后继节点则返回 {@code -1}。
     */
    public int getNextPageId() {
        // 直接从字节数组的固定偏移量读取 int 值
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_NEXT_PAGE_ID);
    }

    /**
     * 设置下一个叶子节点的页面 ID。
     * <p>
     * 此方法通常在两种场景下调用：
     * <ul>
     *   <li><b>节点分裂</b>：当叶子节点分裂时，需要更新原节点和新节点的链表指针，
     *       以维持叶子层的有序链表结构。</li>
     *   <li><b>初始插入</b>：新建第一个叶子节点时，通常保持为 -1。</li>
     * </ul>
     * 修改此字段后，页面即变为脏页（Dirty），上层需通过
     * {@link com.zhan.cyberdb.buffer.BufferPoolManager#unpinPage(int, boolean)} 标记并最终写回磁盘。
     * </p>
     *
     * @param nextPageId 下一个叶子节点的页面 ID。若设为 {@code -1} 则表示链表结束。
     */
    public void setNextPageId(int nextPageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_NEXT_PAGE_ID); // 定位到 NextPageId 字段偏移
        buf.putInt(nextPageId);            // 写入 4 字节整数值
        // 注意：本方法不主动设置脏页标记，由上层调用者负责。
    }

    // ==================== 底层字节读写封装（Key / Value） ====================

    /**
     * 读取指定索引位置的键（Key）。
     * <p>
     * 叶子节点中的键值对按 Key 升序排列，索引 0 为最小键，索引 {@code getCurrentSize() - 1} 为最大键。
     * </p>
     *
     * @param index 键值对的逻辑索引。合法范围为 {@code 0} 到 {@code getCurrentSize() - 1}。
     * @return 存储在该位置的键（32 位整数）。
     */
    public int getKeyAt(int index) {
        // 计算偏移量：叶子节点头部 + 第 index 个 KV 对的起始位置
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR;
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }

    /**
     * 读取指定索引位置的值（Value）。
     *
     * @param index 键值对的逻辑索引。合法范围为 {@code 0} 到 {@code getCurrentSize() - 1}。
     * @return 存储在该位置的值（32 位整数）。
     */
    public int getValueAt(int index) {
        // 偏移量 = 头部 + KV 对起始 + 跳过 Key 的 4 字节
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }

    /**
     * 向指定索引位置写入键（Key）。
     * <p>
     * 此操作用于插入新键值对或更新现有键（通常 B+ 树不会更新键，而是删除再插入）。
     * 调用后页面即变为脏页。
     * </p>
     *
     * @param index 要写入的键索引。
     * @param key   要写入的键值。
     */
    private void setKeyAt(int index, int key) {
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR;
        ByteBuffer.wrap(page.getData()).position(offset).putInt(key);
    }

    /**
     * 向指定索引位置写入值（Value）。
     * <p>
     * 用于插入新键值对或更新现有键对应的值（Update）。
     * </p>
     *
     * @param index 要写入的值索引。
     * @param value 要写入的值。
     */
    private void setValueAt(int index, int value) {
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        ByteBuffer.wrap(page.getData()).position(offset).putInt(value);
    }

    // ==================== 🌟 核心算法：有序插入 ====================

    /**
     * 🌟 核心算法：在叶子节点中插入一个键值对，并<b>维持键的升序排列</b>。
     * <p>
     * 此方法是 B+ 树写操作在叶子层的最终落地点。它保证插入后节点内所有键仍保持严格升序，
     * 为后续的范围查询和二分查找提供基础。
     * </p>
     *
     * <h3>算法步骤</h3>
     * <ol>
     *   <li><b>定位插入点</b>：通过线性扫描（可优化为二分查找）找到第一个大于等于待插入键的位置。
     *       若存在相同键，本实现将其视为新插入位置（不覆盖），实际生产环境需根据唯一性约束处理。</li>
     *   <li><b>腾挪空间</b>：若插入点不是数组末尾，则将该位置及其后的所有键值对整体向后移动 8 字节（一个 KV 对大小），
     *       为新数据腾出空位。此操作使用 {@link System#arraycopy(Object, int, Object, int, int)} 以保证高性能。</li>
     *   <li><b>写入数据</b>：将新的键和值分别写入腾出的空位。</li>
     *   <li><b>更新元数据</b>：将节点的 {@code currentSize} 加 1，并显式标记页面为脏页（{@code page.setDirty(true)}）。</li>
     * </ol>
     *
     * <h3>时间复杂度</h3>
     * <ul>
     *   <li>查找插入点：O(N)，N 为当前节点大小。若采用二分查找可降至 O(log N)。</li>
     *   <li>数据移动：O(N)，最坏情况下需要移动 N 个元素。</li>
     * </ul>
     * 由于 B+ 树节点大小通常较小（几百），线性扫描与移动的开销在内存操作中可接受。
     *
     * <h3>注意事项</h3>
     * <ul>
     *   <li>调用本方法前，调用者应确保节点未满（可通过 {@link #isFull()} 检查），
     *       否则插入将导致数组越界或数据损坏。节点满后的插入应由上层 {@link BPlusTree} 触发分裂逻辑。</li>
     *   <li>本方法不处理重复键的特殊逻辑（如覆盖或忽略），假设上层已做处理。</li>
     * </ul>
     *
     * @param key   要插入的索引键。
     * @param value 关联的数据值。
     */
    public void insert(int key, int value) {
        // 获取当前节点中已存储的键值对数量
        int currentSize = getCurrentSize();

        // ---------- 1. 寻找插入位置 ----------
        // 使用线性扫描定位第一个大于等于待插入 key 的位置
        int targetIndex = 0;
        while (targetIndex < currentSize && getKeyAt(targetIndex) < key) {
            targetIndex++;
        }
        // 循环结束时，targetIndex 即为新键值对应插入的位置

        // ---------- 2. 空间腾挪：将 [targetIndex, currentSize-1] 的元素整体后移一个位置 ----------
        if (targetIndex < currentSize) {
            // 计算源数据起始偏移（当前 targetIndex 位置的 KV 对）
            int srcOffset = SIZE_LEAF_PAGE_HEADER + targetIndex * SIZE_KV_PAIR;
            // 计算目标起始偏移（targetIndex + 1 位置的 KV 对）
            int destOffset = SIZE_LEAF_PAGE_HEADER + (targetIndex + 1) * SIZE_KV_PAIR;
            // 计算需要移动的字节总数
            int lengthToMove = (currentSize - targetIndex) * SIZE_KV_PAIR;

            // 使用高效的系统级内存拷贝，将后续元素整体向后平移
            // 注意：System.arraycopy 能正确处理源与目标区域重叠的情况
            System.arraycopy(page.getData(), srcOffset,
                    page.getData(), destOffset,
                    lengthToMove);
        }

        // ---------- 3. 将新键值对写入腾出的空位 ----------
        setKeyAt(targetIndex, key);
        setValueAt(targetIndex, value);

        // ---------- 4. 更新节点大小并标记脏页 ----------
        increaseSize(1);                // 父类方法，将 currentSize 加 1 并写回头部
        page.setDirty(true);            // 显式标记页面已被修改，确保缓冲池淘汰时写回磁盘
    }
}