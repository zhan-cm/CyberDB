package com.zhan.cyberdb.storage;

import com.zhan.cyberdb.common.Constants;
import java.nio.ByteBuffer;

/**
 * 带有分槽页(Slotted Page)结构的表数据页
 * 它不实际拥有数据，它只是一个"工具类"，里面包裹着一个原始的 Page 对象，
 * 用 ByteBuffer 帮助我们在 Page 的 4096 字节里精确地画格子、写数据。
 */
public class TablePage {

    // 我们正在操作的原始物理页面
    private Page page;

    // --- Page Header 的结构定义 (固定占用 24 个字节) ---
    // 0~3 字节: 这个页的前一个页的 PageID (用于链表，我们先留着)
    // 数据页是链表串起来的，知道我前面是谁
    private static final int OFFSET_PREV_PAGE_ID = 0;
    // 4~7 字节: 这个页的后一个页的 PageID
    // 知道我后面是谁
    private static final int OFFSET_NEXT_PAGE_ID = 4;
    // 8~11 字节: 剩余的空闲空间开始的偏移量 (Free Space Pointer)，初始指向 4096（最底部）
    // 最重要：新数据从这里开始写
    private static final int OFFSET_FREE_SPACE = 8;
    // 12~15 字节: 当前页面里已经存了多少条 Tuple
    // 这一页存了多少行
    private static final int OFFSET_TUPLE_COUNT = 12;
    // 16~19 字节: 预留给事务可见性的标记 (暂不使用)
    // 事务、崩溃恢复用
    private static final int OFFSET_LSN = 16;
    // 20~23 字节: 魔法数字或校验和 (验证页面没有损坏)
    // 防止页损坏
    private static final int OFFSET_MAGIC = 20;

    // Header 的总大小
    private static final int SIZE_PAGE_HEADER = 24;

    // --- Slot (槽位) 的结构定义 (每个槽位占用 8 个字节) ---
    // 槽位里存两个东西：这条数据在页面里的起始位置 (Offset)，和它的长度 (Size)
    private static final int SIZE_SLOT = 8;

    public TablePage(Page page) {
        this.page = page;
    }

    /**
     * 核心动作 1：初始化一个全新的空页面
     * 就像是给一张白纸画上表格的表头
     */
    public void init(int prevPageId, int nextPageId) {
        // 使用 ByteBuffer 来包装我们 Page 里面的字节数组
        ByteBuffer buf = ByteBuffer.wrap(page.getData());

        /*
        buf.position(偏移位置);  // 第一步：移动“笔尖”到指定位置
        buf.putInt(值);         // 第二步：在这个位置写入 4 字节 int
         */

        // 在指定的偏移量位置，写入对应的初始值
        buf.position(OFFSET_PREV_PAGE_ID);
        buf.putInt(prevPageId);
        
        buf.position(OFFSET_NEXT_PAGE_ID);
        buf.putInt(nextPageId);
        
        // 初始状态下，没有任何数据，空闲空间的指针指向页面的最底部 (4096)
        buf.position(OFFSET_FREE_SPACE);
        buf.putInt(Constants.PAGE_SIZE);
        
        // 初始状态下，有 0 条数据
        buf.position(OFFSET_TUPLE_COUNT);
        buf.putInt(0);
    }

    /**
     * 核心动作 2：向页面中插入一行新的数据 (Tuple)
     * @param tuple 要插入的数据
     * @return 插入成功返回 true，如果空间不足返回 false
     */
    public boolean insertTuple(Tuple tuple) {
        int tupleSize = tuple.getLength();

        // 1. 拿到当前页面的状态
        int freeSpacePointer = getFreeSpacePointer();
        int tupleCount = getTupleCount();

        // 2. 空间检查 (极其严谨)
        // 顶部已经用掉的空间 = Header的大小(24) + 目前已有的槽位总大小 (每个槽位8字节)
        int usedHeaderSpace = SIZE_PAGE_HEADER + tupleCount * SIZE_SLOT;
        // 中间剩下的空白区域
        int freeSpace = freeSpacePointer - usedHeaderSpace;

        // 我们需要的空间 = 数据本身的大小 + 为这条数据新建一个槽位的大小(8字节)
        if (freeSpace < tupleSize + SIZE_SLOT) {
            return false; // 悲剧：页面满了，中间的空闲区域被挤没了，塞不下了！
        }

        // 3. 计算真实写入的起始位置 (这就是你刚才答对的公式！)
        int newFreeSpacePointer = freeSpacePointer - tupleSize;

        // 4. 拷贝真实数据到底部
        // System.arraycopy 是 Java 底层极快的数据块复制方法
        System.arraycopy(tuple.getData(), 0, page.getData(), newFreeSpacePointer, tupleSize);

        // 5. 更新槽位目录 (Slot Array)
        // 新槽位应该写在旧槽位的紧下方
        int newSlotOffset = SIZE_PAGE_HEADER + tupleCount * SIZE_SLOT;
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(newSlotOffset);
        buf.putInt(newFreeSpacePointer); // 槽位记录 1：数据的真实起始位置
        buf.putInt(tupleSize);           // 槽位记录 2：数据的长度

        // 6. 更新 Header 的统计信息
        setFreeSpacePointer(newFreeSpacePointer);
        setTupleCount(tupleCount + 1);

        // 🌟 极其重要：只要修改了数据，必须给 Page 打上脏页标记！
        // 这样车间主任(BufferPoolManager)在踢掉它时，才知道要把它写回磁盘。
        page.setDirty(true);

        return true;
    }

    /**
     * 核心动作 3：根据槽位号（Slot ID），极速读取一行数据
     * @param slotId 槽位号（比如 0 代表第一条插入的数据）
     */
    public Tuple getTuple(int slotId) {
        int tupleCount = getTupleCount();
        if (slotId < 0 || slotId >= tupleCount) {
            return null; // 非法请求：槽位越界
        }

        // 1. 算出我们要找的槽位，在数组里的准确位置
        int slotOffset = SIZE_PAGE_HEADER + slotId * SIZE_SLOT;

        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(slotOffset);

        // 2. 从槽位中读出"偏移量"和"长度"
        int tupleOffset = buf.getInt();
        int tupleSize = buf.getInt();

        // 3. 检查这块数据是不是已经被删除了（逻辑删除的标记）
        if (tupleOffset == 0 || tupleSize == 0) {
            return null; // 数据已死，有事烧纸
        }

        // 4. 精准打击：去真实数据的物理位置，把字节数组拷贝出来
        byte[] tupleData = new byte[tupleSize];
        System.arraycopy(page.getData(), tupleOffset, tupleData, 0, tupleSize);

        return new Tuple(tupleData);
    }

    /**
     * 核心动作 4：逻辑删除一行数据
     */
    public boolean deleteTuple(int slotId) {
        int tupleCount = getTupleCount();
        if (slotId < 0 || slotId >= tupleCount) {
            return false;
        }

        int slotOffset = SIZE_PAGE_HEADER + slotId * SIZE_SLOT;
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(slotOffset);

        int tupleOffset = buf.getInt();

        if (tupleOffset == 0) {
            return false; // 早就被删过了，别鞭尸了
        }

        // 🌟 核心：逻辑删除。回到这个槽位，把偏移量和长度都抹零。
        buf.position(slotOffset);
        buf.putInt(0);
        buf.putInt(0);

        // 打上脏页标记，等待车间主任把它写回磁盘保存修改
        page.setDirty(true);
        return true;
    }

    // --- 以下是读取 Header 中各个数值的辅助方法 ---

    //获取 / 设置 数据行数

    public int getTupleCount() {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        return buf.getInt(OFFSET_TUPLE_COUNT);
    }

    private void setTupleCount(int count) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_TUPLE_COUNT);
        buf.putInt(count);
    }

    //获取 / 设置 空闲空间指针

    public int getFreeSpacePointer() {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        return buf.getInt(OFFSET_FREE_SPACE);
    }

    private void setFreeSpacePointer(int freeSpacePointer) {
        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(OFFSET_FREE_SPACE);
        buf.putInt(freeSpacePointer);
    }
}