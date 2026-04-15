package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.storage.Page;
import java.nio.ByteBuffer;

/**
 * B+树 叶子节点
 * 结构: [24字节 父类Header] + [4字节 NextPageId] + [一系列紧凑的 Key-Value 键值对]
 */
public class BPlusTreeLeafPage extends BPlusTreePage {

    // 叶子节点专属的 Header：指向下一个叶子节点的 PageId
    private static final int OFFSET_NEXT_PAGE_ID = 24;
    
    // 叶子节点 Header 的总大小 (24 + 4 = 28)
    private static final int SIZE_LEAF_PAGE_HEADER = 28;
    
    // 我们假设 Key 和 Value 都是 int 类型 (各占 4 字节，一对占 8 字节)
    private static final int SIZE_KEY = 4;
    private static final int SIZE_VALUE = 4;
    private static final int SIZE_KV_PAIR = SIZE_KEY + SIZE_VALUE;

    public BPlusTreeLeafPage(Page page) {
        super(page);
    }

    /**
     * 初始化叶子节点
     */
    public void init(int pageId, int parentId, int maxSize) {
        setPageType(PageType.LEAF);
        setPageId(pageId);
        setParentPageId(parentId);
        setCurrentSize(0);
        setMaxSize(maxSize);
        setNextPageId(-1); // 初始没有下一个节点
    }

    // --- Next Page ID 的 Getter / Setter ---
    public int getNextPageId() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_NEXT_PAGE_ID);
    }

    public void setNextPageId(int nextPageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_NEXT_PAGE_ID);
        buf.putInt(nextPageId);
    }

    // --- 读取 Key 和 Value 的底层方法 ---
    public int getKeyAt(int index) {
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR;
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }

    public int getValueAt(int index) {
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }
    
    private void setKeyAt(int index, int key) {
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR;
        ByteBuffer.wrap(page.getData()).position(offset).putInt(key);
    }

    private void setValueAt(int index, int value) {
        int offset = SIZE_LEAF_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        ByteBuffer.wrap(page.getData()).position(offset).putInt(value);
    }

    /**
     * 🌟 核心算法：在叶子节点中插入一个键值对，并【保持从小到大排序】
     */
    public void insert(int key, int value) {
        // 获取当前节点里已经存了多少个键值对
        int currentSize = getCurrentSize();
        
        // 1. 寻找插入位置 (简单起见，我们用顺序查找。大厂里这里会用二分查找优化)
        int targetIndex = 0;//最终要插入的下标位置（从 0 开始数）
        while (targetIndex < currentSize && getKeyAt(targetIndex) < key) {//如果当前位置的 key 比 我要插入的 key 小 那就说明：我还应该继续往后找

            targetIndex++;
        }

        // 2. 空间腾挪术：把 targetIndex 后面的所有元素，统统往后平移一位！
        // 因为底层是 byte 数组，我们直接计算平移的字节范围
        if (targetIndex < currentSize) {
            int srcOffset = SIZE_LEAF_PAGE_HEADER + targetIndex * SIZE_KV_PAIR;
            int destOffset = SIZE_LEAF_PAGE_HEADER + (targetIndex + 1) * SIZE_KV_PAIR;
            int lengthToMove = (currentSize - targetIndex) * SIZE_KV_PAIR;
            
            // 使用 Java 底层极其高效的 C++ 级内存拷贝
            System.arraycopy(page.getData(), srcOffset, page.getData(), destOffset, lengthToMove);
        }

        // 3. 把新来的键值对，稳稳地安插在腾出来的 targetIndex 位置上
        setKeyAt(targetIndex, key);
        setValueAt(targetIndex, value);

        // 4. 更新节点大小
        increaseSize(1);
        page.setDirty(true); // 别忘了告诉主任，页面被弄脏了！
    }
}