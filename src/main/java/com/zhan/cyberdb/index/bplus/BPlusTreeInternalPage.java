package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.storage.Page;
import java.nio.ByteBuffer;

/**
 * B+ 树 【内部节点】（也叫索引节点、非叶子节点）
 * 作用：只存【路牌key】和【子节点pageId】，不存真实业务数据
 * 结构：[24字节 父类Header] + [连续KV键值对]
 * 特殊约定：index=0 的key无效，只存value（子节点pageId）
 */
public class BPlusTreeInternalPage extends BPlusTreePage {

    // 一个key占4字节（int类型）
    private static final int SIZE_KEY = 4;
    // 一个value（子节点pageId）占4字节（int类型）
    private static final int SIZE_VALUE = 4;
    // 一组KV对总大小：8字节
    private static final int SIZE_KV_PAIR = SIZE_KEY + SIZE_VALUE;

    /**
     * 构造方法：把磁盘页Page包装成B+树内部节点
     * @param page 数据库底层页数据
     */
    public BPlusTreeInternalPage(Page page) {
        super(page);
    }

    /**
     * 初始化内部节点（新建节点时调用）
     * @param pageId 当前节点的页面ID
     * @param parentId 父节点页面ID
     * @param maxSize 节点最多能存多少个KV对
     */
    public void init(int pageId, int parentId, int maxSize) {
        setPageType(PageType.INTERNAL);  // 标记节点类型：内部节点
        setPageId(pageId);               // 设置自己的pageId
        setParentPageId(parentId);       // 设置父节点pageId
        setCurrentSize(0);               // 初始KV数量=0
        setMaxSize(maxSize);             // 设置节点最大容量
    }

    // ==================== 底层字节读写封装 ====================
    // 所有读写直接操作page的byte数组，无对象开销，数据库标准实现

    /**
     * 读取第index个位置的key（路牌值）
     * @param index 索引下标
     * @return 路牌key
     */
    public int getKeyAt(int index) {
        // 计算在byte[]中的偏移：头部偏移 + 第几个KV * KV大小
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR;
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }

    /**
     * 读取第index个位置的value（子节点pageId）
     * @param index 索引下标
     * @return 子节点pageId
     */
    public int getValueAt(int index) {
        // 偏移 = 头部 + KV位置 + 跳过key(4字节)
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        return ByteBuffer.wrap(page.getData()).getInt(offset);
    }

    /**
     * 设置第index个位置的key（路牌）
     */
    public void setKeyAt(int index, int key) {
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR;
        ByteBuffer.wrap(page.getData()).position(offset).putInt(key);
    }

    /**
     * 设置第index个位置的value（子节点pageId）
     */
    public void setValueAt(int index, int value) {
        int offset = SIZE_BPLUS_PAGE_HEADER + index * SIZE_KV_PAIR + SIZE_KEY;
        ByteBuffer.wrap(page.getData()).position(offset).putInt(value);
    }

    // ==================== 🌟 核心：查找路由（B+树搜索必经之路） ====================

    /**
     * 🌟 核心算法：内部节点路由导航
     * 作用：根据要查找的key，判断下一步应该走向哪个子节点
     * 内部节点 = 路牌 + 岔路口，告诉搜索往哪走
     *
     * @param key 要查找的目标key
     * @return 下一个要访问的子节点 pageId
     */
    public int lookup(int key) {
        // 当前节点存了多少个KV对
        int currentSize = getCurrentSize();

        // ==================== B+树内部节点核心约定 ====================
        // index=0：只存子节点pageId，key无效（第一个岔路口）
        // index=1~n：存【路牌key】+【岔路口pageId】
        // 路牌key的含义：所有小于它的数据，走左边岔路
        // =================================================================

        // 从index=1开始比较（index=0的key没用）
        for (int i = 1; i < currentSize; i++) {
            // 规则：目标key < 当前路牌key → 走左边的岔路
            if (key < getKeyAt(i)) {
                // 返回左边岔路口的子节点pageId
                return getValueAt(i - 1);
            }
        }

        // 如果比所有路牌都大 → 走最右边的岔路
        return getValueAt(currentSize - 1);
    }
}