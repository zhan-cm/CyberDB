package com.zhan.cyberdb.buffer;

import java.util.HashMap;
import java.util.Map;

/**
 * 缓冲池的 LRU 页面替换算法实现
 */
public class LRUCache {

    //定义双向链表的节点
    class DLinkedNode {
        int pageId; //页面ID
        // 实际开发中这里还会存具体的 Page 数据，目前我们先存 ID
        DLinkedNode prev;
        DLinkedNode next;

        public DLinkedNode(int pageId) {this.pageId = pageId;}
        public DLinkedNode() {}
    }

    private Map<Integer,DLinkedNode> cache = new HashMap<>();
    private int size;
    private int capacity;
    private DLinkedNode head;
    private DLinkedNode tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        // 使用伪头部和伪尾部节点，方便插入和删除操作
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        tail.prev = head;
    }

    /**
     * 请求访问一个页面

     * 如果缓存没命中（Map 里拿出来是 null）：
     *      判断 size 是不是到了 capacity。
     *      如果到了，调用积木4 removeTail() 拿到被踢掉的节点，然后把它的 pageId 从 cache (Map) 里 remove 掉。并且 size 减 1。
     *      创建一个新的 DLinkedNode。
     *      把新节点放进 cache (Map) 里。
     *      调用积木1 addToHead() 把新节点放到链表头部。
     *      size 加 1。
     * 如果缓存命中（拿出来的节点不是 null）：
     *      调用积木3 moveToHead() 就完事了！
     */
    public void accessPage(int pageId) {
        DLinkedNode node = cache.get(pageId);
        if (node == null) {
            // TODO: 1. 缓存未命中（Cache Miss）的情况
            // 如果缓存已满（size == capacity），需要先淘汰最近最少使用的页面
            // 然后创建新节点，放入缓存（Map），并添加到链表头部
            if(size == capacity) {
                DLinkedNode oldTail = removeTail();
                cache.remove(oldTail.pageId);
                size--;
            }
            DLinkedNode newNode = new DLinkedNode(pageId);
            cache.put(pageId, newNode);
            addToHead(newNode);
            size++;
        } else {
            // TODO: 2. 缓存命中（Cache Hit）的情况
            // 将该节点移动到链表头部，代表它最近刚被使用过
            moveToHead(node);
        }
    }

    // --- 可以在下面封装一些辅助方法，比如添加到头部 (addToHead)、删除节点 (removeNode) 等 ---
    // ==========================================
    // 以下是私有辅助方法：专门用来操作双向链表
    // ==========================================

    /**
     * 积木 1：把一个节点添加到队伍的最前面（虚拟 head 的后面）
     * 为什么：无论是新来的，还是刚被访问的，都要放到最前面代表“最新”
     */
    private void addToHead(DLinkedNode node) {
        // 1. 新节点先跟前后的兄弟拉上手
        node.prev = head;
        node.next = head.next;
        // 2. 原本排在第一的兄弟，把左手交给新节点
        head.next.prev = node;
        // 3. 虚拟头部把右手交给新节点
        head.next = node;
    }

    /**
     * 积木 2：把一个节点从队伍中抽离出来
     * 为什么：当节点被移动或者被淘汰时，需要让它松开左右手
     */
    private void removeNode(DLinkedNode node) {
        // 让该节点的前面一个人，直接牵该节点的后面一个人
        node.prev.next = node.next;
        // 让该节点的后面一个人，直接牵该节点的前面一个人
        node.next.prev = node.prev;
        // 节点此时已经游离于链表之外了
    }

    /**
     * 积木 3：把一个已存在的节点移到最前面
     * 为什么：缓存命中的时候，需要更新它的新鲜度
     */
    private void moveToHead(DLinkedNode node) {
        removeNode(node); // 先从原位置抽离
        addToHead(node);  // 再插到最前面
    }

    /**
     * 积木 4：移除队伍最末尾的那个节点（虚拟 tail 的前面那个）
     * 为什么：缓存满了，要淘汰最老的数据
     */
    private DLinkedNode removeTail() {
        DLinkedNode realTail = tail.prev; // 找到真正的最后一个节点
        removeNode(realTail);             // 把它从链表里抽离
        return realTail;                  // 返回给调用者，因为 HashMap 也需要把它删掉
    }
}
