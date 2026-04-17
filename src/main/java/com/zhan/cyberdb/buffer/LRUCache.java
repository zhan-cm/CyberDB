package com.zhan.cyberdb.buffer;

import java.util.HashMap;
import java.util.Map;

/**
 * <h1>LRU（最近最少使用）淘汰策略管理器（Replacer）</h1>
 *
 * <h2>职责定位</h2>
 * 本类仅负责维护页面淘汰的优先级顺序，并决定在缓冲池空间不足时哪个页面应该被踢出。
 * <b>它不实际存储页面的数据内容</b>，页面的实际数据由上层 {@code BufferPoolManager} 管理。
 *
 * <h2>核心数据结构</h2>
 * <ul>
 *   <li><b>双向链表</b>：维护页面访问/解钉的顺序。链表头部是最近访问/解钉的页面，链表尾部是最久未被访问的页面（即淘汰候选者）。</li>
 *   <li><b>哈希表 {@code cache}</b>：用于 O(1) 时间复杂度快速定位链表节点，键为 {@code pageId}，值为对应的链表节点。</li>
 * </ul>
 *
 * <h2>与 BufferPoolManager 的协作流程</h2>
 * <ol>
 *   <li>当一个页面被线程使用（引用计数增加）时，BufferPoolManager 调用 {@link #pin(int)}，将该页面从 LRU 链表中移除，
 *       表示它正在被钉住（pinned），<b>绝不能被淘汰</b>。</li>
 *   <li>当线程释放页面（引用计数减少至零）时，BufferPoolManager 调用 {@link #unpin(int)}，将该页面加入 LRU 链表，
 *       表示它现在处于空闲状态，可以被考虑淘汰。</li>
 *   <li>当 BufferPoolManager 需要腾出空间加载新页面时，调用 {@link #victimize()}，从链表尾部取出最近最少使用的页面 ID 作为牺牲品。</li>
 * </ol>
 *
 * <h2>线程安全性说明</h2>
 * 本类<b>不是线程安全</b>的。并发控制需由调用方（如 BufferPoolManager）通过外部锁机制保证。
 *
 * @author Zhan
 * @version 1.0
 */
public class LRUCache {

    /**
     * 双向链表节点类。
     * <p>
     * 每个节点代表一个当前未被钉住（pinned）、处于淘汰候选队列中的页面。
     * 节点本身不保存页面数据，仅保存页面的唯一标识符 {@code pageId}。
     * </p>
     */
    class DLinkedNode {
        /** 页面的唯一标识符，与 BufferPool 中管理的页面 ID 一致。 */
        int pageId;
        /** 前驱节点引用，用于支持链表双向遍历与 O(1) 删除操作。 */
        DLinkedNode prev;
        /** 后继节点引用。 */
        DLinkedNode next;

        /**
         * 构造一个代表指定页面的链表节点。
         *
         * @param pageId 页面 ID，必须为非负整数，由上层 BufferPool 分配
         */
        public DLinkedNode(int pageId) {
            this.pageId = pageId;
        }
    }

    /**
     * 哈希表，用于根据页面 ID 快速定位到其在双向链表中的节点。
     * <p>
     * 键：页面 ID ({@code Integer})<br>
     * 值：对应的链表节点 ({@code DLinkedNode})
     * </p>
     * <p>
     * 当执行 {@code pin()} 或 {@code victimize()} 时，通过此映射可以在 O(1) 时间内找到并操作节点，
     * 无需遍历链表。
     * </p>
     */
    private Map<Integer, DLinkedNode> cache = new HashMap<>();

    /**
     * 双向链表的虚拟头节点（哨兵节点）。
     * <p>
     * {@code head} 不存储实际页面数据（其 {@code pageId} 固定为 -1），
     * 它的存在使得对链表头部（第一个有效节点）的插入/删除操作无需进行空指针判断，
     * 简化了边界条件处理。
     * </p>
     */
    private DLinkedNode head;

    /**
     * 双向链表的虚拟尾节点（哨兵节点）。
     * <p>
     * {@code tail} 不存储实际页面数据（其 {@code pageId} 固定为 -1），
     * 它的存在使得对链表尾部（最后一个有效节点）的插入/删除操作无需进行空指针判断。
     * </p>
     */
    private DLinkedNode tail;

    /**
     * LRU 淘汰候选队列的最大容量。
     * <p>
     * 该容量表示最多可以有多少个空闲页面同时存在于候选队列中等待淘汰。
     * 注意：此容量与 BufferPoolManager 的总容量是不同的概念。BufferPoolManager 的总容量包含被钉住（pinned）
     * 的页面和候选队列中的页面，而本类的容量仅针对候选队列部分。
     * </p>
     * <p>
     * 当候选队列满时，{@link #unpin(int)} 不会直接添加新页面，而是由上层 BufferPoolManager
     * 先调用 {@link #victimize()} 淘汰一个页面以腾出空间。
     * </p>
     */
    private int capacity;

    /**
     * 构造一个具有指定容量的 LRU 淘汰管理器。
     * <p>
     * 初始化时会创建虚拟头节点和虚拟尾节点，并将它们首尾相连形成一个空链表。
     * 初始时哈希表为空，表示没有任何页面处于淘汰候选状态。
     * </p>
     *
     * @param capacity 淘汰候选队列的最大容量，必须为正整数。
     *                 它决定了最多能缓存多少个“可淘汰”页面的 ID 顺序。
     */
    public LRUCache(int capacity) {
        this.capacity = capacity;
        // 创建哨兵节点，它们的 pageId 使用 -1 表示无效/虚拟节点
        head = new DLinkedNode(-1);
        tail = new DLinkedNode(-1);
        // 初始时链表为空：head <-> tail
        head.next = tail;
        tail.prev = head;
    }

    /**
     * 将指定页面解除钉住（unpin），并将其加入淘汰候选队列。
     * <p>
     * 此方法对应 BufferPoolManager 中一个页面的引用计数从 1 降为 0 时的操作。
     * 一旦页面被 unpin，意味着当前没有活跃线程正在使用它，因此它成为可以被淘汰的候选者。
     * 页面会被插入到链表头部，表示它是“最近刚被使用/释放”的。
     * </p>
     *
     * <h3>重复 unpin 的处理</h3>
     * 如果该 {@code pageId} 已经存在于候选队列中（即 {@code cache.containsKey(pageId)} 为真），
     * 本方法将直接返回而不做任何操作。这是因为：
     * <ul>
     *   <li>一个页面不可能同时被 pin 两次，因此正常情况下不会连续 unpin 同一个页面而不经过 pin。</li>
     *   <li>若出现重复调用，可能是上层逻辑错误或并发控制不当，本方法采取幂等处理，避免链表结构被破坏。</li>
     * </ul>
     *
     * <h3>容量限制</h3>
     * 注意：本方法<b>不检查容量限制</b>。当候选队列已满时，直接调用本方法会导致队列大小超过
     * {@code capacity}。这是有意为之：容量检查与淘汰触发应由上层 BufferPoolManager 在调用本方法之前完成，
     * 即 BufferPoolManager 应保证在调用 {@code unpin()} 之前，候选队列有足够空间，否则应先调用
     * {@link #victimize()} 腾出空间。
     *
     * @param pageId 要解除钉住的页面 ID，必须为有效的页面标识符
     */
    public void unpin(int pageId) {
        // 如果页面已经在候选队列中，无需重复添加。
        // 这种情况理论上不应发生，但为了健壮性做幂等保护。
        if (cache.containsKey(pageId)) {
            return; // 已经在里面了
        }
        // 创建新节点并添加到链表头部，表示最近被使用/释放
        DLinkedNode node = new DLinkedNode(pageId);
        addToHead(node);
        // 在哈希表中建立映射，以便后续快速查找
        cache.put(pageId, node);
    }

    /**
     * 钉住（pin）指定页面，将其从淘汰候选队列中移除。
     * <p>
     * 当 BufferPoolManager 中的某个空闲页面被线程再次访问时，调用此方法以保护该页面不被淘汰。
     * 页面会从 LRU 链表中删除，对应的哈希表条目也被移除。之后即便调用 {@link #victimize()}
     * 也绝不会选中此页面。
     * </p>
     *
     * <h3>幂等性</h3>
     * 如果指定的 {@code pageId} 当前并不在候选队列中（例如它已经被 pin 住，或者从未被加入队列），
     * 则本方法静默返回，不做任何操作。这是正常的，因为一个被频繁使用的页面可能会经历多次 pin/unpin，
     * 而在 unpin 之前它可能不在队列中。
     *
     * @param pageId 要钉住的页面 ID
     */
    public void pin(int pageId) {
        DLinkedNode node = cache.get(pageId);
        // 仅当页面确实存在于候选队列中时才执行移除操作
        if (node != null) {
            // 从双向链表中摘除节点
            removeNode(node);
            // 从哈希表中移除记录
            cache.remove(pageId);
        }
        // 如果 node == null，表示该页面当前未被缓存为淘汰候选（可能已被 pin 或从未 unpin），无需处理。
    }

    /**
     * 选出一个牺牲者页面，即最近最少使用（LRU）的候选页面。
     * <p>
     * 该方法会从链表尾部（最久未被访问/释放的页面）取出一个页面 ID，
     * 将其从链表中删除并返回。该页面随后将由 BufferPoolManager 执行实际的淘汰（写回磁盘、释放内存等）。
     * </p>
     *
     * <h3>返回值</h3>
     * <ul>
     *   <li>若候选队列非空，返回被淘汰的页面 ID。</li>
     *   <li>若候选队列为空（即所有页面当前都被 pin 住，没有可淘汰的页面），返回 {@code -1}。</li>
     * </ul>
     *
     * <h3>注意</h3>
     * 调用此方法后，返回的页面 ID 将不再受本管理器追踪。上层调用者必须保证在淘汰完成后，
     * 该页面 ID 对应的资源被正确释放。
     *
     * @return 被淘汰的页面 ID；若候选队列为空则返回 {@code -1}
     */
    public int victimize() {
        // 如果哈希表为空，说明没有任何可淘汰页面
        if (cache.isEmpty()) {
            return -1; // 队列里没有可以淘汰的页面（所有页面都在被 pin 着）
        }
        // 链表尾部前一个节点即为最久未使用的实际节点（因为 tail 是哨兵）
        DLinkedNode lastNode = tail.prev;
        // 从双向链表中移除该节点
        removeNode(lastNode);
        // 从哈希表中删除对应的映射
        cache.remove(lastNode.pageId);
        // 返回被淘汰页面的 ID，供上层 BufferPoolManager 处理
        return lastNode.pageId;
    }

    /**
     * 获取当前淘汰候选队列中的页面数量。
     * <p>
     * 该数值等于 {@code cache.size()}，即哈希表中键值对的数量，也等于双向链表中有效节点的个数。
     * </p>
     *
     * @return 当前可被淘汰的页面个数
     */
    public int size() {
        return cache.size();
    }

    /**
     * 将指定节点添加到双向链表的头部（虚拟头节点之后）。
     * <p>
     * 此操作表示该节点对应的页面是“最近被访问/释放”的，应具有最低的淘汰优先级。
     * 时间复杂度 O(1)。
     * </p>
     *
     * <h3>指针调整步骤</h3>
     * <ol>
     *   <li>新节点的 prev 指向 head。</li>
     *   <li>新节点的 next 指向原来的第一个有效节点（head.next）。</li>
     *   <li>将原第一个有效节点的 prev 指向新节点。</li>
     *   <li>将 head 的 next 指向新节点。</li>
     * </ol>
     * <p>
     * 顺序必须正确，尤其是先处理新节点与原后续节点的连接，再修改 head 的 next，
     * 否则会丢失对原链表的引用。
     * </p>
     *
     * @param node 要添加到头部的节点，不能为 {@code null}
     */
    private void addToHead(DLinkedNode node) {
        // 建立新节点与前驱（head）和后继（原 head.next）的连接
        node.prev = head;
        node.next = head.next;
        // 修改原 head.next 的前驱指针指向新节点
        head.next.prev = node;
        // 修改 head 的后继指针指向新节点
        head.next = node;
    }

    /**
     * 从双向链表中移除指定节点。
     * <p>
     * 此操作不释放节点对象本身，仅将其从链表结构中剥离，使链表前后节点直接相连。
     * 时间复杂度 O(1)。
     * </p>
     *
     * <h3>前提条件</h3>
     * 节点必须当前存在于链表中，即其 {@code prev} 和 {@code next} 指针有效且非空。
     * 本方法不进行防御性检查，调用方需保证节点确实在链表中。
     *
     * @param node 要移除的节点
     */
    private void removeNode(DLinkedNode node) {
        // 将 node 的前驱节点的 next 指针跳过 node，直接指向 node 的后继节点
        node.prev.next = node.next;
        // 将 node 的后继节点的 prev 指针跳过 node，直接指向 node 的前驱节点
        node.next.prev = node.prev;
        // 可选：将 node 的 prev/next 置空，帮助垃圾回收。但这里省略以保持简洁。
    }
}