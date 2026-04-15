package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.buffer.BufferPoolManager;
import com.zhan.cyberdb.storage.Page;

/**
 * B+ 树总指挥官
 * 负责协调 BufferPoolManager 和各个节点，完成查找、插入、删除操作。
 */
public class BPlusTree {

    // 车间主任，负责帮我们把 PageId 变成内存里的 Page 对象
    private BufferPoolManager bufferPoolManager;
    
    // 树根的 PageId。初始为空树时，设为 -1
    private int rootPageId;
    
    // 每个节点最多能装几个元素（我们为了测试能快速触发分裂，可以设小一点，比如 3 或 4）
    private int maxDegree;

    public BPlusTree(BufferPoolManager bufferPoolManager, int maxDegree) {
        this.bufferPoolManager = bufferPoolManager;
        this.rootPageId = -1;
        this.maxDegree = maxDegree;
    }

    /**
     * 🌟 核心引擎 1：顺藤摸瓜，从根节点一路找到包含该 Key 的【叶子节点】
     * @param key 要找的目标值
     * @return 目标叶子节点的 Page 对象
     */
    private Page findLeafPage(int key) {
        if (rootPageId == -1) {
            return null; // 树是空的，啥也没有
        }

        // 1. 先向车间主任申请拿到根节点
        int currPageId = rootPageId;
        Page currPage = bufferPoolManager.fetchPage(currPageId);
        
        // 2. 把物理 Page 包装成 B+树通用的 Page
        BPlusTreePage treePage = new BPlusTreeInternalPage(currPage); // 借用 internal 的壳子读 Header
        
        // 3. 只要当前页面不是叶子节点，就一直往下找！
        while (treePage.getPageType() != BPlusTreePage.PageType.LEAF) {
            // 既然不是叶子，那它肯定是内部节点，包装成内部节点
            BPlusTreeInternalPage internalPage = new BPlusTreeInternalPage(currPage);
            
            // 核心动作：看路牌！问问这个节点，我该去哪个儿子那里找？
            int childPageId = internalPage.lookup(key);
            
            // 看完路牌了，当前页面用完了，立刻还给车间主任（极其重要，否则内存泄漏！）
            bufferPoolManager.unpinPage(currPageId, false);
            
            // 目标变成儿子节点，继续下一轮循环
            currPageId = childPageId;
            currPage = bufferPoolManager.fetchPage(currPageId);
            treePage = new BPlusTreeInternalPage(currPage); 
        }

        // 4. 跳出循环时，currPage 一定是一个叶子节点！把它交出去。
        return currPage;
    }

    /**
     * 对外开放的 API：点查 (Point Query)
     * 相当于 SQL: SELECT value FROM table WHERE key = ?
     */
    public Integer getValue(int key) {
        // 1. 找到这片叶子
        Page leafRawPage = findLeafPage(key);
        if (leafRawPage == null) {
            return null;
        }

        // 2. 包装成叶子节点
        BPlusTreeLeafPage leafPage = new BPlusTreeLeafPage(leafRawPage);
        Integer result = null;

        // 3. 在叶子节点的键值对数组里，精确寻找目标 Key
        int size = leafPage.getCurrentSize();
        for (int i = 0; i < size; i++) {
            if (leafPage.getKeyAt(i) == key) {
                result = leafPage.getValueAt(i); // 找到了！
                break;
            }
        }

        // 4. 用完立刻归还（没修改，所以 isDirty = false）
        bufferPoolManager.unpinPage(leafRawPage.getPageId(), false);

        return result;
    }

    /**
     * 🌟 核心引擎 2：向 B+ 树中插入一条新的键值对数据
     * 这是整个数据库写操作的绝对核心入口！
     * * @param key   要插入的索引键 (比如用户 ID: 9527)
     * @param value 要插入的真实数据 (比如数据所在的槽位号)
     */
    public void insert(int key, int value) {
        // ==========================================
        // 场景 A：这是一棵空树，我们迎来了开天辟地的第一条数据！
        // ==========================================
        if (rootPageId == -1) {
            // 1. 向车间主任 (BufferPoolManager) 申请一张全新的白纸 (Page)
            Page rootRawPage = bufferPoolManager.fetchPage(bufferPoolManager.allocatePage());

            // 2. 既然是第一页，那它既是根节点，也是叶子节点 (存放真实数据)
            BPlusTreeLeafPage rootLeafPage = new BPlusTreeLeafPage(rootRawPage);

            // 3. 初始化这个全新的页面
            // 参数1: 自己的ID, 参数2: 父亲的ID(-1代表没爹，因为我是根), 参数3: 最大容量(比如设为4，为了测试容易触发分裂)
            rootLeafPage.init(rootRawPage.getPageId(), -1, maxDegree);

            // 4. 把我们的第一条数据稳稳地插进去
            rootLeafPage.insert(key, value);

            // 5. 极其重要：更新总指挥官的记录，记住新的树根是哪一页
            this.rootPageId = rootRawPage.getPageId();

            // 6. 用完了，通知车间主任回收控制权，并高亮标记 "我弄脏了(isDirty=true)！记得帮我写回磁盘！"
            bufferPoolManager.unpinPage(rootRawPage.getPageId(), true);
            return;
        }

        // ==========================================
        // 场景 B：树已经存在了，我们需要顺藤摸瓜，把数据插入到正确的叶子里
        // ==========================================

        // 1. 调用我们上一关写的找叶子方法，精准空降到目标叶子节点
        Page leafRawPage = findLeafPage(key);
        BPlusTreeLeafPage leafPage = new BPlusTreeLeafPage(leafRawPage);

        // 2. 直接把数据塞进叶子里 (我们在 BPlusTreeLeafPage 里已经写过这个平移数组的方法了)
        leafPage.insert(key, value);

        // 3. 💥 危机检测：检查塞入新数据后，这片叶子是不是被撑爆了？
        // 比如 maxDegree 是 4，现在塞进去了 4 个甚至 5 个元素，违反了 B+ 树的铁律！
        if (leafPage.isFull()) {
            // 触发终极大招：叶子节点分裂！将一个臃肿的节点劈成左右两个完美的节点。
            // 并且会返回新分裂出来的那一半。
            BPlusTreeLeafPage newSiblingLeaf = splitLeafPage(leafPage);

            // 提拔操作 (Promote)：新兄弟的最左边那个 Key，要被提拔到父节点去当"高空路牌"
            int promoteKey = newSiblingLeaf.getKeyAt(0);

            // 把这对新诞生的兄弟，挂载到它们的父亲身上 (这个方法我们下一关写)
            insertIntoParent(leafPage, promoteKey, newSiblingLeaf);

            // 新分裂出来的兄弟用完了，归还给主任，并标记为脏页
            bufferPoolManager.unpinPage(newSiblingLeaf.getPageId(), true);
        }

        // 4. 无论有没有触发分裂，老叶子节点肯定是被修改过了 (插入了新数据)，归还并标记为脏页
        bufferPoolManager.unpinPage(leafRawPage.getPageId(), true);
    }

    /**
     * ⚔️ 终极大招：叶子节点分裂术 (Split)
     * 当一个叶子节点(Old Node)装满时，申请一个新节点(New Node)，把老节点后半部分的数据搬过去。
     * 并重新牵好双向链表的线！
     * * @param oldLeaf 装满数据的那个老叶子节点
     * @return 分裂出来的新叶子节点
     */
    private BPlusTreeLeafPage splitLeafPage(BPlusTreeLeafPage oldLeaf) {
        // 1. 向车间主任紧急申请一张新白纸，用来当老节点的"右兄弟"
        Page newRawPage = bufferPoolManager.fetchPage(bufferPoolManager.allocatePage());
        BPlusTreeLeafPage newLeaf = new BPlusTreeLeafPage(newRawPage);

        // 2. 初始化新兄弟
        // 它的父亲和老叶子是一样的；最大容量也一样
        newLeaf.init(
                newRawPage.getPageId(),
                oldLeaf.getParentPageId(),
                oldLeaf.getMaxSize());

        // 3. 计算切割线 (分水岭)
        // 比如老节点装了 4 个元素，一半就是 2。我们要把 index >= 2 的元素搬到新家里去。
        int splitIndex = oldLeaf.getCurrentSize() / 2;
        int moveCount = oldLeaf.getCurrentSize() - splitIndex;

        // 4. 开始疯狂搬家 (将后半部分元素逐个拷贝到新节点)
        for (int i = 0; i < moveCount; i++) {
            int keyToMove = oldLeaf.getKeyAt(splitIndex + i);
            int valueToMove = oldLeaf.getValueAt(splitIndex + i);
            newLeaf.insert(keyToMove, valueToMove); // 存入新家
        }

        // 5. 老节点减肥：把它的 Size 直接砍掉一半。
        // (注意：底层的物理字节其实没删，但只要 Size 变小了，后面的脏数据就会被认为是无效的，下次插入直接覆盖)
        oldLeaf.setCurrentSize(splitIndex);

        // 6. 🔗 极其精妙的双向链表重连 (Iron Chain Connection)
        // 设想原本是：[老叶子] ---> [右边某个未知节点]
        // 现在变成了：[老叶子] ---> [新兄弟] ---> [右边某个未知节点]

        // a. 新兄弟的 Next 指针，指向老叶子原来的 Next
        newLeaf.setNextPageId(oldLeaf.getNextPageId());
        // b. 老叶子的 Next 指针，指向新兄弟的 PageId
        oldLeaf.setNextPageId(newLeaf.getPageId());

        // 7. 返回新造出来的兄弟，以便后续把它汇报给父节点
        return newLeaf;
    }

    /**
     * ⬆️ 向上提拔：把分裂产生的新兄弟，挂载到父节点上
     */
    private void insertIntoParent(BPlusTreePage leftNode, int key, BPlusTreePage rightNode) {
        // 🌟 史诗级场景：如果刚刚分裂的左节点是【树根】
        // 这意味着原来的单层平房装不下了，我们要盖二楼了！树长高了！
        if (leftNode.isRootPage()) {
            // 1. 向主任申请一个新页面，来当它们俩的“新爹”（全新的内部节点）
            Page newRootRawPage = bufferPoolManager.fetchPage(bufferPoolManager.allocatePage());
            BPlusTreeInternalPage newRoot = new BPlusTreeInternalPage(newRootRawPage);

            // 2. 初始化新树根 (它没有爹，所以 parentId 是 -1)
            newRoot.init(newRootRawPage.getPageId(), -1, maxDegree);

            // 3. 极其特殊的内部节点第 0 位：只存左儿子的 PageId (不存 Key)
            newRoot.setValueAt(0, leftNode.getPageId());

            // 4. 第 1 位：存路牌 Key，以及右儿子的 PageId
            newRoot.setKeyAt(1, key);
            newRoot.setValueAt(1, rightNode.getPageId());

            // 5. 更新新树根的 Size 为 2 (它现在有两个儿子了)
            newRoot.setCurrentSize(2);

            // 6. 伦理重建：更新左右儿子的“认父”证明
            leftNode.setParentPageId(newRoot.getPageId());
            rightNode.setParentPageId(newRoot.getPageId());

            // 7. 更新总指挥官的印章：宣告新的树根诞生！
            this.rootPageId = newRoot.getPageId();

            // 8. 归还并标记脏页，让苦力写回磁盘
            bufferPoolManager.unpinPage(newRoot.getPageId(), true);
            return;
        }

        // 场景 2：普通节点的上浮（如果爹也满了，需要发生恐怖的"递归级联分裂"）
        // 在几十万行代码的 MySQL InnoDB 源码里，这里是最复杂的地方。
        // 为了你能顺利拿到今天的阶段性胜利，我们先捕获这个场景。
        throw new RuntimeException("树太高了！触发了内部节点的级联分裂！由于代码量巨大，本教程先通关 Root 分裂！");
    }
}