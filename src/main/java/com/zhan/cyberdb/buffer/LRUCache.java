package com.zhan.cyberdb.buffer;

import java.util.HashMap;
import java.util.Map;

/**
 * LRU 替换策略器 (Replacer)
 * 只负责决定谁该被踢出，不实际保存 Page 数据。
 */
public class LRUCache {

    class DLinkedNode {
        int pageId;
        DLinkedNode prev, next;
        public DLinkedNode(int pageId) { this.pageId = pageId; }
    }

    private Map<Integer, DLinkedNode> cache = new HashMap<>();
    private DLinkedNode head, tail;
    private int capacity;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        head = new DLinkedNode(-1);
        tail = new DLinkedNode(-1);
        head.next = tail;
        tail.prev = head;
    }

    // 将页面放入 LRU 候选队列（代表它目前没人用，可以被淘汰）unpin () = 松开页面 → 加入淘汰候选队列
    public void unpin(int pageId) {
        if (cache.containsKey(pageId)) {
            return; // 已经在里面了
        }
        // 如果候选队列满了，不能直接加，由上层 BufferPool 去调用 victimize 腾空间
        DLinkedNode node = new DLinkedNode(pageId);
        addToHead(node);
        cache.put(pageId, node);
    }

    // 页面正在被线程使用（钉住），从淘汰候选队列中移除，绝对不能淘汰它  pin () = 钉住页面 → 不能被淘汰
    public void pin(int pageId) {
        DLinkedNode node = cache.get(pageId);
        if (node != null) {
            removeNode(node);
            cache.remove(pageId);
        }
    }

    // 选出一个牺牲者（踢掉最近最少使用的）
    public int victimize() {
        if (cache.isEmpty()) {
            return -1; // 队列里没有可以淘汰的页面（所有页面都在被 pin 着）
        }
        DLinkedNode lastNode = tail.prev;
        removeNode(lastNode);
        cache.remove(lastNode.pageId);
        return lastNode.pageId;
    }

    public int size() { return cache.size(); }

    private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }
}