package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.storage.Page;

import java.nio.ByteBuffer;

/**
 * B+ 树节点的抽象基类（Abstract Base Page）。
 * <p>
 * 内部节点（{@link BPlusTreeInternalPage}）和叶子节点（{@link BPlusTreeLeafPage}）均继承自此抽象类。
 * 它统一管理 B+ 树节点在磁盘页面上通用的 24 字节固定头部（Header），并提供对头部字段的类型安全读写方法。
 * </p>
 *
 * <h2>设计目的</h2>
 * <ul>
 *   <li><b>代码复用</b>：将内部节点和叶子节点共享的元数据（如页面类型、父节点指针、当前大小、最大容量）
 *       抽取到基类中，避免重复实现。</li>
 *   <li><b>物理布局规范</b>：明确定义了节点在 {@link Page#getData()} 字节数组中的固定头部布局，
 *       子类只需关注头部之后的自有数据区域。</li>
 *   <li><b>统一接口</b>：提供了 {@link #isFull()} 和 {@link #isRootPage()} 等核心判断方法，
 *       供上层 {@link BPlusTree} 调度分裂和树结构调整时使用。</li>
 * </ul>
 *
 * <h2>B+ 树节点通用头部布局（24 字节）</h2>
 * <pre>
 * ┌────────────────┬─────────────────┬───────────────────┬──────────────────┬───────────────┬────────────┐
 * │  PageType      │   PageId        │   ParentPageId    │   CurrentSize    │   MaxSize     │  Reserved  │
 * │  (4 bytes)     │   (4 bytes)     │   (4 bytes)       │   (4 bytes)      │   (4 bytes)   │  (4 bytes) │
 * │  偏移 0~3      │   偏移 4~7      │   偏移 8~11       │   偏移 12~15     │   偏移 16~19  │  偏移 20~23│
 * └────────────────┴─────────────────┴───────────────────┴──────────────────┴───────────────┴────────────┘
 * </pre>
 * <ul>
 *   <li><b>PageType</b>：标识节点类型。1 表示内部节点（{@link PageType#INTERNAL}），2 表示叶子节点（{@link PageType#LEAF}）。</li>
 *   <li><b>PageId</b>：当前节点自身的页面 ID，与 {@link Page#getPageId()} 保持一致。</li>
 *   <li><b>ParentPageId</b>：父节点的页面 ID。若当前节点为整棵树的根节点，则该字段值为 {@code -1}。</li>
 *   <li><b>CurrentSize</b>：节点当前存储的元素数量。对于叶子节点，指键值对的数量；对于内部节点，指子节点指针的数量。</li>
 *   <li><b>MaxSize</b>：节点允许存储的最大元素数量。一旦 {@code CurrentSize} 达到此值，节点即被视为“已满”，
 *       后续插入将触发节点分裂（Split）。</li>
 *   <li><b>Reserved</b>：4 字节保留字段，暂未使用，可用于未来扩展或内存对齐。</li>
 * </ul>
 * <p>
 * 所有字段均以<b>大端序（Big-Endian）</b>或<b>本地字节序</b>存储，具体取决于 {@link ByteBuffer} 的默认行为。
 * 由于 Java 的 {@code ByteBuffer} 默认采用大端序，且所有读写均在同一 JVM 内进行，跨平台兼容性有保障。
 * </p>
 *
 * <h2>子类实现指南</h2>
 * 子类在操作 {@link Page#getData()} 字节数组时，必须跳过前 {@link #SIZE_BPLUS_PAGE_HEADER}（24）个字节，
 * 从偏移量 24 开始存储自有数据。例如：
 * <ul>
 *   <li>{@link BPlusTreeLeafPage} 在偏移 24 处存储 4 字节的 {@code NextPageId}，随后存储键值对数组。</li>
 *   <li>{@link BPlusTreeInternalPage} 在偏移 24 处直接存储键值对数组。</li>
 * </ul>
 *
 * @author Zhan
 * @version 1.0
 * @see BPlusTreeInternalPage
 * @see BPlusTreeLeafPage
 * @see Page
 */
public abstract class BPlusTreePage {

    /**
     * 底层物理页对象。
     * <p>
     * 该对象封装了 4KB 的磁盘页面数据（{@code byte[] data}），并提供了页面 ID、脏标记、引用计数等元数据。
     * 本类及子类的所有读写操作最终都通过操作 {@code page.getData()} 字节数组完成。
     * 此字段由构造函数注入，子类可直接访问。
     * </p>
     */
    protected Page page;

    // =========================================================
    // B+ Tree Page Header 结构偏移量定义（固定占用 24 字节）
    // =========================================================

    /**
     * 页面类型字段在字节数组中的起始偏移量（0）。
     * <p>
     * 存储一个 4 字节整数，其值对应 {@link PageType} 枚举：
     * <ul>
     *   <li>0：无效/未初始化（{@link PageType#INVALID}）</li>
     *   <li>1：内部节点（{@link PageType#INTERNAL}）</li>
     *   <li>2：叶子节点（{@link PageType#LEAF}）</li>
     * </ul>
     * </p>
     */
    protected static final int OFFSET_PAGE_TYPE = 0;

    /**
     * 当前页面 ID 字段在字节数组中的起始偏移量（4）。
     * <p>
     * 存储一个 4 字节整数，表示该节点自身的页面标识符。此值应与 {@link Page#getPageId()} 返回的值一致。
     * </p>
     */
    protected static final int OFFSET_PAGE_ID = 4;

    /**
     * 父节点页面 ID 字段在字节数组中的起始偏移量（8）。
     * <p>
     * 存储一个 4 字节整数，表示父节点的页面标识符。若当前节点为根节点，则存储 {@code -1}。
     * 该字段在节点分裂、合并、树结构调整时会被更新。
     * </p>
     */
    protected static final int OFFSET_PARENT_PAGE_ID = 8;

    /**
     * 当前元素数量字段在字节数组中的起始偏移量（12）。
     * <p>
     * 存储一个 4 字节整数，表示节点当前实际存储的元素个数。语义取决于节点类型：
     * <ul>
     *   <li>叶子节点：已存储的键值对数量。</li>
     *   <li>内部节点：已存储的子节点指针数量（即 {@code values} 数组的有效长度）。</li>
     * </ul>
     * 该值在插入和删除时动态更新，且永远不超过 {@code MaxSize}。
     * </p>
     */
    protected static final int OFFSET_CURRENT_SIZE = 12;

    /**
     * 最大容量字段在字节数组中的起始偏移量（16）。
     * <p>
     * 存储一个 4 字节整数，表示节点允许存储的最大元素数量。该值在节点初始化时设定，
     * 并在节点生命周期内保持不变。当 {@code CurrentSize >= MaxSize} 时，节点被视为“已满”，
     * 需通过分裂操作维持 B+ 树性质。
     * </p>
     */
    protected static final int OFFSET_MAX_SIZE = 16;

    // 偏移 20~23 为保留字段，当前未使用，可用于未来扩展或维持 8 字节对齐。

    /**
     * B+ 树页面固定头部的总字节大小（24 字节）。
     * <p>
     * 子类在计算自有数据区域的偏移量时，必须以此常量为基础进行累加。
     * 例如，叶子节点在偏移 {@code SIZE_BPLUS_PAGE_HEADER} 处存储 {@code NextPageId}。
     * </p>
     */
    protected static final int SIZE_BPLUS_PAGE_HEADER = 24;

    /**
     * 页面类型枚举。
     * <p>
     * 用于标识当前 {@link BPlusTreePage} 的具体子类型，决定其数据布局和可用操作。
     * </p>
     */
    public enum PageType {
        /** 无效类型，通常表示页面尚未初始化。 */
        INVALID,
        /** 内部节点（索引节点），仅存储路由键和子节点指针。 */
        INTERNAL,
        /** 叶子节点，存储实际键值对和链表指针。 */
        LEAF
    }

    /**
     * 构造一个 B+ 树节点视图，绑定到底层物理页。
     * <p>
     * 此构造函数不对页面数据做任何修改，仅保存引用。子类需自行提供初始化方法（如 {@code init}）
     * 来设置头部字段的初始值。
     * </p>
     *
     * @param page 底层物理页对象，其 {@code data} 字段必须为已分配的 4KB 字节数组。不能为 {@code null}。
     */
    public BPlusTreePage(Page page) {
        this.page = page;
    }

    // =========================================================
    // 以下是用 ByteBuffer 读写 4KB 数组中那 24 字节 Header 的方法
    // =========================================================

    /**
     * 获取当前页面的类型。
     *
     * @return 页面类型枚举值：{@link PageType#INTERNAL}、{@link PageType#LEAF} 或 {@link PageType#INVALID}。
     */
    public PageType getPageType() {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        int type = buf.getInt(OFFSET_PAGE_TYPE);
        if (type == 1) return PageType.INTERNAL;
        if (type == 2) return PageType.LEAF;
        return PageType.INVALID;
    }

    /**
     * 设置当前页面的类型。
     * <p>
     * 通常在节点初始化时调用，一旦设置不应再更改。调用此方法后页面即变为脏页，
     * 上层调用者需负责标记脏页（通过 {@link Page#setDirty(boolean)}）。
     * </p>
     *
     * @param pageType 要设置的页面类型枚举值。
     */
    public void setPageType(PageType pageType) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_PAGE_TYPE);
        if (pageType == PageType.INTERNAL) buf.putInt(1);
        else if (pageType == PageType.LEAF) buf.putInt(2);
        else buf.putInt(0);
    }

    /**
     * 获取当前页面的 ID。
     *
     * @return 当前页面的唯一标识符（32 位整数）。
     */
    public int getPageId() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_PAGE_ID);
    }

    /**
     * 设置当前页面的 ID。
     * <p>
     * 通常在节点初始化时由 {@code init} 方法调用。后续不应再修改。
     * </p>
     *
     * @param pageId 页面的唯一标识符，由 {@link com.zhan.cyberdb.buffer.BufferPoolManager#allocatePage()} 分配。
     */
    public void setPageId(int pageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_PAGE_ID);
        buf.putInt(pageId);
    }

    /**
     * 获取父节点的页面 ID。
     *
     * @return 父节点的页面 ID；若当前节点为根节点，则返回 {@code -1}。
     */
    public int getParentPageId() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_PARENT_PAGE_ID);
    }

    /**
     * 设置父节点的页面 ID。
     * <p>
     * 在节点分裂、合并或树根提升时，需要调用此方法更新子节点的父指针。
     * </p>
     *
     * @param parentPageId 父节点的页面 ID。若当前节点成为新根，则设为 {@code -1}。
     */
    public void setParentPageId(int parentPageId) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_PARENT_PAGE_ID);
        buf.putInt(parentPageId);
    }

    /**
     * 获取节点当前存储的元素数量。
     * <p>
     * 对于叶子节点，指键值对的数量；对于内部节点，指子节点指针的数量。
     * </p>
     *
     * @return 当前元素数量。
     */
    public int getCurrentSize() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_CURRENT_SIZE);
    }

    /**
     * 设置节点当前存储的元素数量。
     * <p>
     * 插入或删除元素后必须更新此值。通常通过 {@link #increaseSize(int)} 进行增量修改更为便捷。
     * </p>
     *
     * @param currentSize 新的元素数量。必须为非负整数，且不超过 {@link #getMaxSize()}。
     */
    public void setCurrentSize(int currentSize) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_CURRENT_SIZE);
        buf.putInt(currentSize);
    }

    /**
     * 增加节点当前的元素数量。
     * <p>
     * 这是 {@link #setCurrentSize(int)} 的便捷封装，适用于插入操作。
     * </p>
     *
     * @param amount 要增加的数量，通常为 1（插入）或分裂时迁移的元素个数。
     */
    public void increaseSize(int amount) {
        setCurrentSize(getCurrentSize() + amount);
    }

    /**
     * 获取节点允许的最大元素数量（容量上限）。
     *
     * @return 最大容量。
     */
    public int getMaxSize() {
        return ByteBuffer.wrap(page.getData()).getInt(OFFSET_MAX_SIZE);
    }

    /**
     * 设置节点允许的最大元素数量。
     * <p>
     * 通常在节点初始化时设定，并在节点生命周期内保持不变。
     * </p>
     *
     * @param maxSize 最大容量，必须为正整数。
     */
    public void setMaxSize(int maxSize) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_MAX_SIZE);
        buf.putInt(maxSize);
    }

    // =========================================================
    // B+ 树极其核心的判断逻辑
    // =========================================================

    /**
     * 判断当前节点是否已满。
     * <p>
     * 当节点内元素数量达到或超过其最大容量时，节点被视为“已满”。
     * 在插入新元素前，上层逻辑必须调用此方法检查；若返回 {@code true}，
     * 则需触发节点分裂（Split）以维持 B+ 树的结构性质。
     * </p>
     *
     * @return {@code true} 如果当前元素数量 ≥ 最大容量；否则 {@code false}。
     */
    public boolean isFull() {
        return getCurrentSize() >= getMaxSize();
    }

    /**
     * 判断当前节点是否为整棵 B+ 树的根节点。
     * <p>
     * 约定：根节点没有父节点，因此其父页面 ID 字段的值为 {@code -1}。
     * 此判断用于决定树结构调整的边界条件，例如根节点分裂时需要创建新的根。
     * </p>
     *
     * @return {@code true} 如果父页面 ID 为 {@code -1}；否则 {@code false}。
     */
    public boolean isRootPage() {
        return getParentPageId() == -1;
    }
}