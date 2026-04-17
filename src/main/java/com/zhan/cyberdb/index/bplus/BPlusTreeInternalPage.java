package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.storage.Page;

import java.nio.ByteBuffer;

/**
 * B+ 树内部节点（Internal Node / Index Node / Non-leaf Node）。
 *
 * <h2>核心定位</h2>
 * 内部节点是 B+ 树中除叶子节点以外的所有节点，其唯一作用是<b>存储路由信息</b>，
 * 引导查询从根节点逐层向下定位到目标叶子节点。
 * <p>
 * <b>内部节点不存储任何实际的业务数据（Value）</b>，仅存储：
 * <ul>
 *   <li><b>路牌键（Separator Key）</b>：用于区分不同子节点键值范围的界限值。</li>
 *   <li><b>子节点页面 ID（Child Page ID）</b>：指向下一层节点（可能是内部节点或叶子节点）的物理页面标识。</li>
 * </ul>
 * 这种设计使得 B+ 树的内部节点可以非常紧凑，从而在单个磁盘页内存储更多的分叉指针，降低树的高度。
 * </p>
 *
 * <h2>在磁盘上的物理布局</h2>
 * 一个内部节点的 4KB {@link Page#getData()} 字节数组布局如下：
 * <pre>
 * ┌──────────────────────────────┬─────────────────────────────────────────────────────────┐
 * │      父类 Header (24 字节)     │               KV 对数组（连续存储）                         │
 * ├──────────────────────────────┼─────────────────────────────────────────────────────────┤
 * │ pageType, pageId, parentId,  │ [value0][ key1 ][value1][ key2 ][value2] ... [ keyN ][valueN] │
 * │ currentSize, maxSize, ...    │   ↑        ↑        ↑        ↑        ↑          ↑        ↑   │
 * │                              │  index=0  index=1  index=1  index=2  index=2    index=N  index=N│
 * │                              │  (只有value) (key)   (value)  (key)   (value)    (key)   (value)│
 * └──────────────────────────────┴─────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>特殊约定：索引 0 位置</h3>
 * 在内部节点中，索引 0（index = 0）的位置<b>仅存储一个子节点页面 ID（value）</b>，
 * 没有对应的路牌键（key）。这个位置被称为“最左侧指针”（Leftmost Pointer）。
 * <p>
 * 原因：对于 B+ 树内部节点，若有 N 个子节点，则只需要 N-1 个分隔键。第 0 个子节点覆盖所有“小于第一个分隔键”的键值范围，
 * 因此不需要额外的键来描述其上限。例如，若节点包含子节点 A、B、C，以及分隔键 K1 和 K2，则结构为：
 * <pre>
 * [A] < K1 ≤ [B] < K2 ≤ [C]
 * 存储为: values[0]=A, keys[1]=K1, values[1]=B, keys[2]=K2, values[2]=C
 * </pre>
 * 这一约定简化了路由算法的实现，并节省了存储空间。
 * </p>
 *
 * <h2>字段说明</h2>
 * 每个 KV 对由 4 字节的键（int）和 4 字节的值（int，即页面 ID）组成，合计 8 字节。
 * 所有读写操作均通过 {@link ByteBuffer} 直接操作底层字节数组，避免创建大量临时对象，
 * 这是数据库系统追求高性能的标准做法。
 *
 * @author Zhan
 * @version 1.0
 * @see BPlusTreePage
 * @see BPlusTreeLeafPage
 * @see Page
 */
public class BPlusTreeInternalPage extends BPlusTreePage {

    /**
     * 单个键（Key）的字节大小。
     * <p>
     * 在 CyberDB 的简化实现中，键固定为 32 位有符号整数（int），占用 4 字节。
     * 这是为了教学清晰和实现简洁。实际工业级数据库（如 MySQL、PostgreSQL）
     * 通常支持变长键（如 VARCHAR、多列复合键），此时内部节点的布局会更加复杂。
     * </p>
     */
    private static final int SIZE_KEY = 4;

    /**
     * 单个值（Value，即子节点页面 ID）的字节大小。
     * <p>
     * 页面 ID 为 32 位整数（int），占用 4 字节。在单表不超过 2³² 页（约 16TB，以 4KB 页计算）
     * 的场景下足够使用。若未来需要支持更大规模，可扩展为 8 字节（long）。
     * </p>
     */
    private static final int SIZE_VALUE = 4;

    /**
     * 一个完整 KV 对（Key-Value Pair）的字节大小。
     * <p>
     * 等于 {@link #SIZE_KEY} + {@link #SIZE_VALUE} = 8 字节。
     * 注意：索引 0 的位置仅使用 4 字节（只有 Value），但为简化寻址计算，
     * 我们仍为其预留 8 字节的空间，其中前 4 字节的 Key 区域内容无定义（可能为 0 或垃圾数据）。
     * </p>
     */
    private static final int SIZE_KV_PAIR = SIZE_KEY + SIZE_VALUE;

    /**
     * 构造一个内部节点视图，包装一个底层磁盘页。
     * <p>
     * 此构造方法不执行任何初始化逻辑，仅将传入的 {@link Page} 对象保存到父类的 {@code page} 字段中。
     * 若该页面是全新分配的，调用者必须在构造后立即调用 {@link #init(int, int, int)} 进行初始化。
     * 若该页面是从磁盘读取的已有节点，则其字节数据已包含有效头部和 KV 数组，可直接调用读取方法。
     * </p>
     *
     * @param page 底层物理页对象，其 {@code data} 字段必须已分配 4KB 空间。不能为 {@code null}。
     */
    public BPlusTreeInternalPage(Page page) {
        super(page);
    }

    /**
     * 初始化一个全新的内部节点（通常在分配新页面后调用）。
     * <p>
     * 该方法会向页面的头部写入节点类型、自身 ID、父节点 ID、当前大小（0）和最大容量。
     * 此后，KV 数组区域的内容为未初始化的垃圾数据，后续将通过 {@link #setKeyAt(int, int)}
     * 和 {@link #setValueAt(int, int)} 进行填充。
     * </p>
     *
     * @param pageId   当前节点的页面 ID。该 ID 应由 {@link com.zhan.cyberdb.buffer.BufferPoolManager#allocatePage()} 分配。
     * @param parentId 父节点的页面 ID。若当前节点为根节点，则传 {@code -1}。
     * @param maxSize  节点最大容量（即最多可容纳的 KV 对数量）。此值决定了节点何时触发分裂。
     *                 注意：内部节点的实际子节点数最多为 {@code maxSize}，对应的键数量最多为 {@code maxSize - 1}。
     */
    public void init(int pageId, int parentId, int maxSize) {
        setPageType(PageType.INTERNAL);  // 标记节点类型：内部节点
        setPageId(pageId);               // 设置自己的 pageId
        setParentPageId(parentId);       // 设置父节点 pageId
        setCurrentSize(0);               // 初始 KV 数量 = 0
        setMaxSize(maxSize);             // 设置节点最大容量
    }

    // ==================== 底层字节读写封装 ====================
    // 所有读写直接操作 Page 的 byte 数组，避免创建临时对象，是数据库追求极限性能的标准实践。

    /**
     * 读取指定索引位置的路牌键（Key）。
     * <p>
     * 注意：按照 B+ 树内部节点的约定，索引 0 的键是无效的（未定义），
     * 调用者不应依赖该位置的返回值。本方法不会对索引进行范围检查，由调用者保证索引合法。
     * </p>
     *
     * @param index 键在 KV 数组中的逻辑索引。合法范围为 {@code 1} 到 {@code getCurrentSize() - 1}。
     * @return 存储在该位置的路牌键（32 位整数）。
     */
    public int getKeyAt(int index) {
        // 计算在 byte[] 中的偏移：头部偏移 + 第几个 KV * KV 大小
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR;
        // 使用 ByteBuffer 包装字节数组，以绝对偏移量读取一个 int
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }

    /**
     * 读取指定索引位置的子节点页面 ID（Value）。
     * <p>
     * 索引 0 处存储的是最左侧子节点的页面 ID，这是有效的。
     * </p>
     *
     * @param index 值在 KV 数组中的逻辑索引。合法范围为 {@code 0} 到 {@code getCurrentSize() - 1}。
     * @return 子节点的页面 ID（32 位整数）。
     */
    public int getValueAt(int index) {
        // 偏移 = 头部 + KV 位置 + 跳过 key(4 字节)
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }

    /**
     * 向指定索引位置写入路牌键（Key）。
     * <p>
     * 此操作会直接修改底层字节数组，调用后页面即变为脏页（Dirty）。
     * 上层调用者（如 {@link BPlusTree}）负责在适当时候通过
     * {@link com.zhan.cyberdb.buffer.BufferPoolManager#unpinPage(int, boolean)} 标记脏页并写回磁盘。
     * </p>
     *
     * @param index 要写入的键索引。合法范围为 {@code 1} 到 {@code getMaxSize() - 1}。
     * @param key   要写入的路牌键值。
     */
    public void setKeyAt(int index, int key) {
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR;
        // 使用 position(offset) 定位，然后写入 int
        ByteBuffer.wrap(page.getData()).position(offset).putInt(key);
    }

    /**
     * 向指定索引位置写入子节点页面 ID（Value）。
     * <p>
     * 与 {@link #setKeyAt(int, int)} 类似，修改后页面变为脏页。
     * </p>
     *
     * @param index 要写入的值索引。合法范围为 {@code 0} 到 {@code getMaxSize() - 1}。
     * @param value 子节点的页面 ID。
     */
    public void setValueAt(int index, int value) {
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        ByteBuffer.wrap(page.getData()).position(offset).putInt(value);
    }

    // ==================== 🌟 核心：查找路由（B+ 树搜索必经之路） ====================

    /**
     * 🌟 核心算法：内部节点路由导航（Lookup / Search）。
     * <p>
     * 这是 B+ 树查询过程中最关键的方法之一。给定一个目标键 {@code key}，
     * 本方法根据内部节点存储的路牌键，确定下一步应该走向哪个子节点，
     * 并返回该子节点的页面 ID。
     * </p>
     *
     * <h3>算法原理</h3>
     * 内部节点可视为一系列有序的分隔键与子节点指针的组合：
     * <pre>
     * 子节点0 (value0)  <  key1  ≤  子节点1 (value1)  <  key2  ≤  子节点2 (value2) ... < keyN ≤ 子节点N (valueN)
     * </pre>
     * 注意第一个子节点（value0）没有对应的左边界键，它覆盖所有小于 key1 的键值。
     * <p>
     * 查找逻辑因此分为两步：
     * <ol>
     *   <li>从索引 1 开始遍历所有路牌键 {@code key_i}。</li>
     *   <li>若目标键 {@code key} <b>严格小于</b>当前路牌键 {@code key_i}，
     *       则说明目标键应位于左侧子节点 {@code value_{i-1}} 所指向的子树中，
     *       立即返回 {@code value_{i-1}}。</li>
     *   <li>若遍历完所有路牌键仍未找到小于关系，说明目标键大于等于最后一个路牌键 {@code key_N}，
     *       此时应走向最右侧子节点 {@code value_N}，返回之。</li>
     * </ol>
     *
     * <h3>边界情况</h3>
     * <ul>
     *   <li>若内部节点当前只有两个子节点（即刚分裂后），则只有 key1 和 value0、value1。
     *       查找时若 key < key1，返回 value0；否则返回 value1。</li>
     *   <li>若内部节点为空（{@code getCurrentSize() == 0}），本方法行为未定义。
     *       正常 B+ 树操作不会产生空内部节点，因此调用方需保证节点非空。</li>
     * </ul>
     *
     * <h3>时间复杂度</h3>
     * 线性扫描 O(N)，其中 N 为节点的当前大小。当节点度数较大（如几百）时，
     * 可改用二分查找优化为 O(log N)，但本教程为清晰起见采用线性扫描。
     *
     * @param key 要查找的目标键值。
     * @return 下一步应访问的子节点的页面 ID。
     */
    public int lookup(int key) {
        // 当前节点中存储的 KV 对数量（等于子节点数量）
        int currentSize = getCurrentSize();

        // ==================== B+ 树内部节点核心约定 ====================
        // index = 0：只存子节点 pageId，key 无效（第一个岔路口）
        // index = 1 ~ N：存【路牌 key】+【岔路口 pageId】
        // 路牌 key 的含义：所有小于它的数据，走左边岔路
        // =================================================================

        // 从 index = 1 开始比较（index = 0 的 key 没有定义，不能使用）
        for (int i = 1; i < currentSize; i++) {
            // 规则：目标 key < 当前路牌 key → 走左边的岔路
            // 注意：使用严格小于 (<) 而非小于等于 (<=)
            // 这是因为对于等于路牌键的情况，B+ 树约定走向右侧子节点（或叶子节点），
            // 以保证所有等于路牌键的数据都存储在右侧子树中。
            if (key < getKeyAt(i)) {
                // 返回左边岔路口的子节点 pageId
                return getValueAt(i - 1);
            }
        }

        // 如果目标 key 大于或等于所有路牌 key，则走向最右侧的子节点
        // 此时 currentSize - 1 即为最后一个有效 value 的索引
        return getValueAt(currentSize - 1);
    }
}