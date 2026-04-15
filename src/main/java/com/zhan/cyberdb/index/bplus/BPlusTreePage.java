package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.storage.Page;
import java.nio.ByteBuffer;

/**
 * B+树节点的通用基类 (内部节点和叶子节点都要继承它)
 * 它主要负责管理 B+ 树节点通用的 Header 头部信息。
 */
public abstract class BPlusTreePage {

    // 依然是在操作底层那 4KB 的物理 Page
    protected Page page;

    // --- B+ Tree Page Header 结构 (固定占用 24 字节) ---
    // 0~3 字节: 页面类型 (1 代表内部节点, 2 代表叶子节点)
    protected static final int OFFSET_PAGE_TYPE = 0;
    // 4~7 字节: 这个页面自己的 Page ID
    protected static final int OFFSET_PAGE_ID = 4;
    // 8~11 字节: 它的父节点的 Page ID (如果是根节点，父节点就是 -1)
    protected static final int OFFSET_PARENT_PAGE_ID = 8;
    // 12~15 字节: 当前节点已经存了多少个元素 (Size)
    protected static final int OFFSET_CURRENT_SIZE = 12;
    // 16~19 字节: 这个节点最多能存多少个元素 (Max Size)，一旦超过就要触发分裂！
    protected static final int OFFSET_MAX_SIZE = 16;
    // 20~23 字节: 预留空间对齐
    
    // Header 的总大小，子类在写数据时必须跳过这前 24 个字节！
    protected static final int SIZE_BPLUS_PAGE_HEADER = 24;

    // 枚举：页面类型
    public enum PageType {
        INVALID, INTERNAL, LEAF
    }

    public BPlusTreePage(Page page) {
        this.page = page;
    }

    // =========================================================
    // 以下是用 ByteBuffer 读写 4KB 数组中那 24 字节 Header 的方法
    // =========================================================

    public PageType getPageType() {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        int type = buf.getInt(OFFSET_PAGE_TYPE);
        if (type == 1) return PageType.INTERNAL;
        if (type == 2) return PageType.LEAF;
        return PageType.INVALID;
    }

    public void setPageType(PageType pageType) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_PAGE_TYPE);
        if (pageType == PageType.INTERNAL) buf.putInt(1);
        else if (pageType == PageType.LEAF) buf.putInt(2);
        else buf.putInt(0);
    }

    public int getPageId() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_PAGE_ID);
    }

    public void setPageId(int pageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_PAGE_ID);
        buf.putInt(pageId);
    }

    public int getParentPageId() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_PARENT_PAGE_ID);
    }

    public void setParentPageId(int parentPageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_PARENT_PAGE_ID);
        buf.putInt(parentPageId);
    }

    public int getCurrentSize() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_CURRENT_SIZE);
    }

    public void setCurrentSize(int currentSize) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_CURRENT_SIZE);
        buf.putInt(currentSize);
    }

    // 辅助方法：增加元素数量
    public void increaseSize(int amount) {
        setCurrentSize(getCurrentSize() + amount);
    }

    public int getMaxSize() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_MAX_SIZE);
    }

    public void setMaxSize(int maxSize) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_MAX_SIZE);
        buf.putInt(maxSize);
    }

    // =========================================================
    // B+ 树极其核心的判断逻辑
    // =========================================================

    /**
     * 判断当前节点是否已经装满了？
     * 如果装满了，在下一次插入时，就必须把它劈成两半（Split）！
     */
    public boolean isFull() {
        return getCurrentSize() >= getMaxSize();
    }

    /**
     * 判断当前节点是不是树的根节点？
     * 约定：根节点没有爹（父节点 ID 为 -1）
     */
    public boolean isRootPage() {
        return getParentPageId() == -1;
    }
}