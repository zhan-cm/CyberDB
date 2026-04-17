package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.buffer.BufferPoolManager;
import com.zhan.cyberdb.storage.Page;

/**
 * B+ 树索引管理器（总指挥官）。
 *
 * <h2>核心定位</h2>
 * 本类是 CyberDB 数据库引擎中 B+ 树索引的顶层入口，负责协调 {@link BufferPoolManager}（缓冲池）
 * 与具体的 B+ 树节点类（{@link BPlusTreePage}、{@link BPlusTreeInternalPage}、{@link BPlusTreeLeafPage}），
 * 对外提供统一的键值对增删改查接口。
 *
 * <h2>主要职责</h2>
 * <ul>
 *   <li><b>树结构管理</b>：维护根节点的 {@code pageId}，处理空树初始化及树的高度增长（根分裂）。</li>
 *   <li><b>查找（Search）</b>：实现点查 {@link #getValue(int)}，通过 {@link #findLeafPage(int)} 定位目标叶子节点。</li>
 *   <li><b>插入（Insert）</b>：实现 {@link #insert(int, int)}，包括空树初始化、叶子节点插入、
 *       叶子分裂（{@link #splitLeafPage(BPlusTreeLeafPage)}）以及向上挂载分裂产生的新兄弟节点。</li>
 *   <li><b>缓冲池交互</b>：通过 {@link BufferPoolManager} 获取页面、标记脏页、归还页面，
 *       确保内存使用受控且修改能持久化。</li>
 * </ul>
 *
 * <h2>与底层节点类的关系</h2>
 * 本类不直接操作页面的二进制数据，而是通过 {@link BPlusTreeInternalPage} 和 {@link BPlusTreeLeafPage}
 * 两个包装类来解读和修改 {@link Page} 对象中的内容。这种分层设计隔离了物理存储细节与逻辑树操作。
 *
 * <h2>线程安全性</h2>
 * 当前实现<b>未内置并发控制</b>。对于多线程并发访问同一棵 B+ 树，调用方（例如事务管理器）
 * 必须在外部施加适当的锁机制（如表锁、页锁或闩锁）以保证一致性。
 *
 * <h2>关于 {@code maxDegree} 的说明</h2>
 * 本类中的 {@code maxDegree} 表示每个节点最多能容纳的键值对数量（对于叶子节点）或儿子指针数量（对于内部节点）。
 * 为了在教学演示中更容易触发分裂，可以将其设置为较小的值（如 3 或 4）。实际生产环境中通常为几百。
 *
 * @author Zhan
 * @version 1.0
 * @see BufferPoolManager
 * @see BPlusTreePage
 * @see BPlusTreeInternalPage
 * @see BPlusTreeLeafPage
 */
public class BPlusTree {

    /**
     * 缓冲池管理器（车间主任）。
     * <p>
     * 负责所有页面在内存与磁盘之间的调度。本类中所有对 {@link Page} 对象的获取与释放均通过此实例完成。
     * 它提供了：
     * <ul>
     *   <li>{@link BufferPoolManager#fetchPage(int)}：根据页面 ID 获取内存中的页面副本（可能触发磁盘 I/O）。</li>
     *   <li>{@link BufferPoolManager#unpinPage(int, boolean)}：释放页面引用，若页面被修改则标记为脏页。</li>
     *   <li>{@link BufferPoolManager#allocatePage()}：分配一个新的、唯一的页面 ID。</li>
     * </ul>
     * 在 B+ 树操作中，正确配对 {@code fetchPage} 与 {@code unpinPage} 调用是防止内存泄漏和脏页丢失的关键。
     * </p>
     */
    private BufferPoolManager bufferPoolManager;

    /**
     * 树根节点的页面 ID。
     * <p>
     * 当 B+ 树为空时（即尚未插入任何数据），此值为 {@code -1}（等价于 {@link com.zhan.cyberdb.common.Constants#INVALID_PAGE_ID}）。
     * 一旦插入第一条数据，便会创建一个新的叶子节点同时作为根节点，并将此字段更新为该节点的页面 ID。
     * </p>
     * <p>
     * 后续所有查找和插入操作的起点均依赖于此字段。根节点分裂时此字段会被更新为新根的页面 ID。
     * </p>
     */
    private int rootPageId;

    /**
     * 节点的最大度数（扇出因子，Order / Degree）。
     * <p>
     * 决定了一个 B+ 树节点最多可以容纳多少个元素（键值对或儿子指针）。具体语义为：
     * <ul>
     *   <li><b>叶子节点</b>：最多可存储 {@code maxDegree} 个键值对。</li>
     *   <li><b>内部节点</b>：最多可存储 {@code maxDegree} 个儿子指针（其中第 0 个指针为最左侧指针，
     *       不伴随 Key，其余指针均伴随一个 Key 作为分界）。因此内部节点的键数量最多为 {@code maxDegree - 1}。</li>
     * </ul>
     * 此值在构造时指定，运行期不可变。较小的值（如 3 或 4）便于测试时快速触发节点分裂逻辑。
     * </p>
     */
    private int maxDegree;

    /**
     * 构造一棵 B+ 树索引。
     * <p>
     * 初始时树为空（{@code rootPageId = -1}），随着后续调用 {@link #insert(int, int)} 会逐步构建树结构。
     * </p>
     *
     * @param bufferPoolManager 缓冲池管理器实例，用于页面获取与释放。不能为 {@code null}。
     * @param maxDegree         节点最大度数（容量）。必须为正整数，通常建议 ≥ 3。
     */
    public BPlusTree(BufferPoolManager bufferPoolManager, int maxDegree) {
        this.bufferPoolManager = bufferPoolManager;
        this.rootPageId = -1;          // 初始化为空树
        this.maxDegree = maxDegree;
    }

    /**
     * 核心引擎 1：顺藤摸瓜 —— 从根节点出发，逐层向下定位到包含指定键的叶子节点。
     * <p>
     * 这是 B+ 树所有查询与插入操作的基础路径导航方法。它利用内部节点中存储的“路牌键”
     * 来决定每一步应该走向哪个子节点，最终抵达必定包含目标键的叶子节点（但不保证键一定存在）。
     * </p>
     *
     * <h3>执行流程</h3>
     * <ol>
     *   <li>若树为空（{@code rootPageId == -1}），直接返回 {@code null}。</li>
     *   <li>从根节点开始，通过 {@link BufferPoolManager#fetchPage(int)} 获取当前节点的内存副本。</li>
     *   <li>读取节点元数据判断其类型：
     *       <ul>
     *           <li>若为<b>内部节点</b>（{@code PageType.INTERNAL}），则调用
     *               {@link BPlusTreeInternalPage#lookup(int)} 根据键值查找下一步应访问的子节点页面 ID。</li>
     *           <li>若为<b>叶子节点</b>（{@code PageType.LEAF}），则停止循环，返回该页面。</li>
     *       </ul>
     *   </li>
     *   <li>在从内部节点获取到子页面 ID 后，<b>必须立即归还当前页面</b>（{@code unpinPage}），
     *       然后获取子页面继续下一轮循环。</li>
     * </ol>
     *
     * <h3>内存管理约定</h3>
     * 调用此方法后，返回的叶子节点 {@link Page} 对象处于“已钉住”（pin count ≥ 1）状态。
     * <b>调用者有责任在使用完毕后调用 {@link BufferPoolManager#unpinPage(int, boolean)} 将其释放。</b>
     * 本方法内部遍历过程中经过的内部节点均已被正确释放。
     *
     * @param key 要查找的目标键值。
     * @return 包含该键的叶子节点对应的 {@link Page} 对象（已 pin）；若树为空则返回 {@code null}。
     */
    private Page findLeafPage(int key) {
        // 空树检查
        if (rootPageId == -1) {
            return null; // 树是空的，啥也没有
        }

        // 1. 从根节点开始
        int currPageId = rootPageId;
        Page currPage = bufferPoolManager.fetchPage(currPageId);

        // 2. 读取页面头部信息以判断页面类型（借用 InternalPage 的解析能力，两者头部结构相同）
        BPlusTreePage treePage = new BPlusTreeInternalPage(currPage);

        // 3. 只要当前页面不是叶子节点，就一直向下查找
        while (treePage.getPageType() != BPlusTreePage.PageType.LEAF) {
            // 既然不是叶子，那么一定是内部节点，包装成内部节点以调用 lookup
            BPlusTreeInternalPage internalPage = new BPlusTreeInternalPage(currPage);

            // 核心动作：根据键值查找应进入的子节点页面 ID
            int childPageId = internalPage.lookup(key);

            // 当前内部节点已使用完毕，立即归还（未修改，脏标记为 false）
            bufferPoolManager.unpinPage(currPageId, false);

            // 目标转为子节点，继续下一轮循环
            currPageId = childPageId;
            currPage = bufferPoolManager.fetchPage(currPageId);
            treePage = new BPlusTreeInternalPage(currPage);
        }

        // 4. 循环结束，currPage 必定为叶子节点，直接返回
        return currPage;
    }

    /**
     * 对外开放的点查 API（Point Query）。
     * <p>
     * 对应 SQL 语句：{@code SELECT value FROM table WHERE key = ?}。
     * 该方法会定位到目标叶子节点，并在其键值对数组中线性搜索精确匹配的键。
     * </p>
     *
     * <h3>返回值</h3>
     * <ul>
     *   <li>若找到匹配的键，返回关联的 {@code value}。</li>
     *   <li>若树为空，或键不存在，返回 {@code null}。</li>
     * </ul>
     *
     * <h3>内存管理</h3>
     * 无论查找成功与否，最终都会通过 {@code unpinPage} 释放叶子节点，并传递 {@code false} 表示未修改页面。
     *
     * @param key 要查询的键。
     * @return 对应的值；若不存在则返回 {@code null}。
     */
    public Integer getValue(int key) {
        // 1. 定位到包含该 key 的叶子节点
        Page leafRawPage = findLeafPage(key);
        if (leafRawPage == null) {
            return null; // 空树
        }

        // 2. 将原始 Page 包装为叶子节点视图
        BPlusTreeLeafPage leafPage = new BPlusTreeLeafPage(leafRawPage);
        Integer result = null;

        // 3. 遍历叶子节点中的所有键值对，寻找精确匹配
        int size = leafPage.getCurrentSize();
        for (int i = 0; i < size; i++) {
            if (leafPage.getKeyAt(i) == key) {
                result = leafPage.getValueAt(i); // 命中！
                break;
            }
        }

        // 4. 释放叶子节点（未修改，isDirty = false）
        bufferPoolManager.unpinPage(leafRawPage.getPageId(), false);

        return result;
    }

    /**
     * 核心引擎 2：向 B+ 树中插入一条新的键值对。
     * <p>
     * 这是整个数据库写操作的绝对核心入口。所有索引数据的持久化均从此处开始。
     * 该方法处理两种主要场景：
     * </p>
     * <ol>
     *   <li><b>空树插入</b>：创建第一个页面，同时作为根节点和叶子节点，直接插入数据。</li>
     *   <li><b>非空树插入</b>：
     *       <ul>
     *           <li>通过 {@link #findLeafPage(int)} 定位目标叶子节点。</li>
     *           <li>将键值对插入叶子节点（内部涉及槽位移动）。</li>
     *           <li>若插入后叶子节点已满（{@code isFull()}），触发叶子分裂（{@link #splitLeafPage(BPlusTreeLeafPage)}）。</li>
     *           <li>将分裂产生的新兄弟节点挂载到父节点（{@link #insertIntoParent(BPlusTreePage, int, BPlusTreePage)}）。</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * <h3>脏页标记</h3>
     * 任何被修改的页面（包括新分配的页面、发生插入或分裂的页面）在调用 {@code unpinPage} 时
     * 都必须传递 {@code true} 作为 {@code isDirty} 参数，以确保缓冲池在淘汰该页时将其写回磁盘。
     *
     * @param key   要插入的索引键（例如用户 ID）。
     * @param value 关联的数据值（例如行记录在堆表中的槽位号）。
     */
    public void insert(int key, int value) {
        // ==========================================
        // 场景 A：空树 —— 创建根节点（同时也是叶子节点）
        // ==========================================
        if (rootPageId == -1) {
            // 1. 分配一个新页面 ID，并立即 fetch 获取其内存副本
            Page rootRawPage = bufferPoolManager.fetchPage(bufferPoolManager.allocatePage());

            // 2. 包装为叶子节点视图（因为第一页既是根也是叶子）
            BPlusTreeLeafPage rootLeafPage = new BPlusTreeLeafPage(rootRawPage);

            // 3. 初始化该页面的元数据
            //    参数: 自身 pageId, 父节点 pageId(-1 表示根节点), 最大容量
            rootLeafPage.init(rootRawPage.getPageId(), -1, maxDegree);

            // 4. 将第一条数据插入叶子节点
            rootLeafPage.insert(key, value);

            // 5. 更新总指挥官的记录，将根页面 ID 指向这个新页面
            this.rootPageId = rootRawPage.getPageId();

            // 6. 释放页面并标记为脏页（因为数据已改变，必须写回磁盘）
            bufferPoolManager.unpinPage(rootRawPage.getPageId(), true);
            return;
        }

        // ==========================================
        // 场景 B：非空树 —— 找到目标叶子并插入
        // ==========================================

        // 1. 导航到应插入的叶子节点
        Page leafRawPage = findLeafPage(key);
        BPlusTreeLeafPage leafPage = new BPlusTreeLeafPage(leafRawPage);

        // 2. 将键值对插入叶子（内部处理槽位移动）
        leafPage.insert(key, value);

        // 3. 分裂检测：插入后若叶子节点已满，必须进行分裂以维护 B+ 树性质
        if (leafPage.isFull()) {
            // 执行叶子分裂，返回新分裂出的右兄弟节点
            BPlusTreeLeafPage newSiblingLeaf = splitLeafPage(leafPage);

            // 提拔操作（Promote）：将新兄弟节点的第一个键作为“路牌”插入到父节点
            int promoteKey = newSiblingLeaf.getKeyAt(0);

            // 将 (左节点, 路牌键, 右节点) 挂载到父节点中
            insertIntoParent(leafPage, promoteKey, newSiblingLeaf);

            // 新兄弟节点的引用释放（它已被修改，标记脏页）
            bufferPoolManager.unpinPage(newSiblingLeaf.getPageId(), true);
        }

        // 4. 无论是否分裂，原叶子节点必定已被修改（插入了新数据），释放并标记脏页
        bufferPoolManager.unpinPage(leafRawPage.getPageId(), true);
    }

    /**
     * 终极大招：叶子节点分裂术（Leaf Node Split）。
     * <p>
     * 当一个叶子节点达到容量上限（{@code isFull() == true}）时，必须将其一分为二，
     * 以保证后续插入有空间，并维持 B+ 树的平衡性。
     * </p>
     *
     * <h3>分裂步骤</h3>
     * <ol>
     *   <li>分配一个新页面，初始化其元数据（父节点 ID 与左节点相同，容量相同）。</li>
     *   <li>计算分割点（通常取元素数量的中间位置）。</li>
     *   <li>将老节点中从分割点开始的后半部分键值对逐个复制到新节点中。</li>
     *   <li>将老节点的元素数量截断至分割点（逻辑删除后半部分）。</li>
     *   <li><b>重连双向链表</b>：更新新老节点的 {@code nextPageId} 指针，维持叶子节点间的顺序链。</li>
     * </ol>
     *
     * <h3>返回值</h3>
     * 返回新创建的右兄弟叶子节点（{@link BPlusTreeLeafPage}），调用者需负责将其插入到父节点中。
     *
     * <h3>内存管理</h3>
     * 本方法内部分配的新页面在返回时仍处于“已 pin”状态，<b>调用者有责任最终将其 unpin</b>。
     *
     * @param oldLeaf 已满的老叶子节点（左节点）。
     * @return 分裂后产生的新叶子节点（右节点），已初始化并填充数据，未释放。
     */
    private BPlusTreeLeafPage splitLeafPage(BPlusTreeLeafPage oldLeaf) {
        // 1. 分配新页面，用作右兄弟
        Page newRawPage = bufferPoolManager.fetchPage(bufferPoolManager.allocatePage());
        BPlusTreeLeafPage newLeaf = new BPlusTreeLeafPage(newRawPage);

        // 2. 初始化新兄弟的元数据（父亲、容量与老节点一致）
        newLeaf.init(
                newRawPage.getPageId(),
                oldLeaf.getParentPageId(),
                oldLeaf.getMaxSize());

        // 3. 计算分割点（取中间位置，后半部分迁移）
        int splitIndex = oldLeaf.getCurrentSize() / 2;
        int moveCount = oldLeaf.getCurrentSize() - splitIndex;

        // 4. 将老节点后半部分元素逐个拷贝到新节点
        for (int i = 0; i < moveCount; i++) {
            int keyToMove = oldLeaf.getKeyAt(splitIndex + i);
            int valueToMove = oldLeaf.getValueAt(splitIndex + i);
            newLeaf.insert(keyToMove, valueToMove); // 存入新家（此时新节点不会满，因为只迁移一半）
        }

        // 5. 截断老节点：直接将大小设为 splitIndex，后半部分数据逻辑删除
        oldLeaf.setCurrentSize(splitIndex);

        // 6. 🔗 重连双向链表（维持叶子节点间的水平顺序）
        //    原链表： [老叶子] ---> [原 next]
        //    现链表： [老叶子] ---> [新兄弟] ---> [原 next]
        newLeaf.setNextPageId(oldLeaf.getNextPageId()); // 新兄弟的 next 指向老叶子原来的 next
        oldLeaf.setNextPageId(newLeaf.getPageId());     // 老叶子的 next 指向新兄弟

        // 7. 返回新兄弟节点（已 pin，待上层处理）
        return newLeaf;
    }

    /**
     * 向上提拔：将分裂产生的新兄弟节点挂载到父节点中。
     * <p>
     * 当一个节点分裂后，会产生一个“路牌键”（通常是新右兄弟的第一个键）和左右两个子节点。
     * 父节点需要将这个三元组（左子节点 ID，路牌键，右子节点 ID）插入到自身内部。
     * </p>
     *
     * <h3>两种核心场景</h3>
     * <ol>
     *   <li><b>左节点原本是根节点</b>（树长高的情况）：
     *       <ul>
     *           <li>分配一个新的内部节点作为新根。</li>
     *           <li>将左右子节点挂载到新根下，并设置相应的路牌键。</li>
     *           <li>更新左右子节点的父指针指向新根。</li>
     *           <li>更新本类的 {@code rootPageId} 为新根的页面 ID。</li>
     *       </ul>
     *   </li>
     *   <li><b>左节点有普通父节点</b>（可能触发父节点的递归分裂）：
     *       <ul>
     *           <li>在当前教程实现中，暂未实现父节点满时的级联分裂逻辑，会抛出异常提示。</li>
     *           <li>完整实现需获取父节点，执行插入，若父节点满则分裂父节点，并递归向上。</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * <h3>内存管理</h3>
     * 本方法内部若创建新根，会分配新页面并最终在方法内释放（标记脏页）。调用者传入的左右节点仍由调用者负责最终释放。
     *
     * @param leftNode  分裂后的左节点（原节点，可能为叶子或内部节点，此处为叶子）。
     * @param key       提拔到父节点的路牌键（右兄弟节点的第一个键）。
     * @param rightNode 分裂后新产生的右兄弟节点。
     */
    private void insertIntoParent(BPlusTreePage leftNode, int key, BPlusTreePage rightNode) {
        // ==========================================
        // 史诗级场景：树根分裂 —— 树的高度增加一层
        // ==========================================
        if (leftNode.isRootPage()) {
            // 1. 分配一个新页面，作为新的根节点（内部节点）
            Page newRootRawPage = bufferPoolManager.fetchPage(bufferPoolManager.allocatePage());
            BPlusTreeInternalPage newRoot = new BPlusTreeInternalPage(newRootRawPage);

            // 2. 初始化新根：自身 ID，无父节点（-1），最大容量
            newRoot.init(newRootRawPage.getPageId(), -1, maxDegree);

            // 3. 设置内部节点的第 0 个儿子指针（最左侧指针，不伴随 Key）
            //    在内部节点中，values[0] 存储最左侧子节点的 pageId，没有对应的 key
            newRoot.setValueAt(0, leftNode.getPageId());

            // 4. 设置第 1 个槽位：路牌键 + 右儿子指针
            //    key 放在 keys[1]，右儿子 pageId 放在 values[1]
            newRoot.setKeyAt(1, key);
            newRoot.setValueAt(1, rightNode.getPageId());

            // 5. 更新新根的当前大小（此时有两个儿子）
            newRoot.setCurrentSize(2);

            // 6. 伦理重建：更新左右儿子节点的父指针，指向新根
            leftNode.setParentPageId(newRoot.getPageId());
            rightNode.setParentPageId(newRoot.getPageId());

            // 7. 更新总指挥官记录的根页面 ID
            this.rootPageId = newRoot.getPageId();

            // 8. 释放新根页面并标记为脏页（内容已修改）
            bufferPoolManager.unpinPage(newRoot.getPageId(), true);
            return;
        }

        // ==========================================
        // 场景 2：普通父节点插入（可能触发父节点分裂的级联反应）
        // ==========================================
        // 实际完整的 B+ 树实现中，这里需要：
        //   1. 获取父节点（leftNode.getParentPageId()）
        //   2. 将 (key, rightNode.pageId) 插入父节点
        //   3. 若父节点满，则分裂父节点，并递归调用 insertIntoParent
        // 由于代码量巨大，本教程版本仅实现根分裂，此处抛出异常提示进阶学习
        throw new RuntimeException("树太高了！触发了内部节点的级联分裂！由于代码量巨大，本教程先通关 Root 分裂！");
    }
}