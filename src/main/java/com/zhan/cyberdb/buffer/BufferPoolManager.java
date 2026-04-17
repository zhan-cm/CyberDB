package com.zhan.cyberdb.buffer;

import com.zhan.cyberdb.common.Constants;
import com.zhan.cyberdb.storage.DiskManager;
import com.zhan.cyberdb.storage.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 缓冲池管理器（Buffer Pool Manager）。
 *
 * <h2>核心定位</h2>
 * 本类是存储引擎的“交通枢纽”，负责在内存与磁盘之间调度页面数据，并提供统一的页面访问接口。
 * 上层模块（如 B+ 树、索引管理器）只与 {@code BufferPoolManager} 交互，无需感知磁盘 I/O 细节。
 *
 * <h2>主要职责</h2>
 * <ul>
 *   <li><b>页面缓存</b>：在内存中维护一个固定大小的页面数组（{@link #pages}），缓存热点页面以减少磁盘访问。</li>
 *   <li><b>页面定位</b>：通过页表（{@link #pageTable}）快速判断某个 {@code pageId} 是否已缓存在内存中，
 *       以及缓存在哪个槽位（帧）。</li>
 *   <li><b>淘汰策略</b>：当缓冲池满且需要加载新页面时，委托给 {@link LRUCache} 选出最近最少使用的页面，
 *       并在必要时将脏页写回磁盘。</li>
 *   <li><b>并发控制基础</b>：通过 {@code pinCount} 引用计数机制，确保正在被使用的页面不会被误淘汰。</li>
 *   <li><b>新页分配</b>：代理 {@link DiskManager#allocatePage()} 为上层生成唯一的新页面 ID。</li>
 * </ul>
 *
 * <h2>与 LRUCache 的关系</h2>
 * {@code LRUCache} 仅维护淘汰候选队列的顺序，不实际持有页面数据。本类在以下时机调用其方法：
 * <ul>
 *   <li>页面被访问时：调用 {@link LRUCache#pin(int)} 从候选队列中移除。</li>
 *   <li>页面引用计数归零时：调用 {@link LRUCache#unpin(int)} 将其加入候选队列。</li>
 *   <li>缓冲池满且无空槽位时：调用 {@link LRUCache#victimize()} 获取待淘汰的 {@code pageId}。</li>
 * </ul>
 *
 * <h2>线程安全性说明</h2>
 * 当前实现<b>未内置并发控制</b>。若需在多线程环境下使用，调用方必须自行加锁（例如在 B+ 树或事务管理器层加锁）。
 *
 * @author Zhan
 * @version 1.0
 * @see LRUCache
 * @see DiskManager
 * @see Page
 */
public class BufferPoolManager {
    private static final Logger log = LoggerFactory.getLogger(BufferPoolManager.class);

    /**
     * 缓冲池的总槽位数（帧数）。
     * <p>
     * 决定了内存中最多能同时缓存多少个不同的页面。该值在构造时指定，运行期不可变。
     * 每个槽位对应一个 {@link Page} 对象。
     * </p>
     */
    private int poolSize;

    /**
     * 缓冲池本体：一个固定大小的 {@link Page} 数组。
     * <p>
     * 数组下标称为“帧标识（frameId / slot index）”。每个数组元素是一个 {@code Page} 对象，
     * 其中包含页面的实际数据内容（{@code byte[] data}）、页面 ID、脏标记、引用计数等元数据。
     * </p>
     * <p>
     * 数组大小在构造时确定，一旦分配不再动态扩容。因此必须配合淘汰策略使用。
     * </p>
     */
    private Page[] pages;

    /**
     * 页表（Page Table）：建立页面 ID 到帧索引的映射。
     * <p>
     * 键（Key）：{@code pageId} —— 页面的全局唯一标识符，由 {@link DiskManager} 分配。<br>
     * 值（Value）：{@code frameId} —— 该页面在 {@link #pages} 数组中的下标。
     * </p>
     * <p>
     * 通过此映射可以 O(1) 判断一个页面是否已缓存，并直接定位到对应的 {@code Page} 对象。
     * 当页面被淘汰或从磁盘换入时，此表会同步更新。
     * </p>
     */
    private Map<Integer, Integer> pageTable;

    /**
     * 磁盘管理器（苦力）。
     * <p>
     * 负责执行底层的物理 I/O 操作，包括：
     * <ul>
     *   <li>根据 {@code pageId} 将页面数据从磁盘文件读入 {@link Page} 对象。</li>
     *   <li>将 {@link Page} 对象中的脏数据写回磁盘文件。</li>
     *   <li>分配新的、唯一的 {@code pageId}。</li>
     * </ul>
     * 本类不直接操作文件，所有磁盘交互均委托给此对象。
     * </p>
     */
    private DiskManager diskManager;

    /**
     * LRU 淘汰策略器（参谋）。
     * <p>
     * 负责维护一个“淘汰候选队列”，记录当前未被任何线程钉住（pin count == 0）的页面，
     * 并按最近使用/释放的顺序排列。当缓冲池需要腾出空间时，由此对象决定牺牲哪个页面。
     * </p>
     * <p>
     * 注意：{@code LRUCache} 的容量与 {@link #poolSize} 相等，
     * 因为最坏情况下所有缓冲池槽位都可能存放着空闲页面。
     * </p>
     */
    private LRUCache replacer;

    /**
     * 构造一个缓冲池管理器。
     * <p>
     * 初始化时会：
     * <ol>
     *   <li>创建指定大小的 {@link Page} 数组，并用空的 {@code Page} 对象填充每个槽位。</li>
     *   <li>初始化空的页表。</li>
     *   <li>创建容量为 {@code poolSize} 的 {@link LRUCache} 淘汰器。</li>
     * </ol>
     * 初始状态下，所有槽位的 {@code pageId} 均为 {@link Constants#INVALID_PAGE_ID}，表示空闲。
     * </p>
     *
     * @param poolSize    缓冲池容量，即最多可同时缓存的页面数，必须为正整数。
     * @param diskManager 磁盘管理器实例，用于实际的 I/O 操作。不能为 {@code null}。
     */
    public BufferPoolManager(int poolSize, DiskManager diskManager) {
        this.poolSize = poolSize;
        this.diskManager = diskManager;
        // 分配固定大小的 Page 数组
        this.pages = new Page[poolSize];
        // 初始化页表（空）
        this.pageTable = new HashMap<>();
        // 初始化 LRU 淘汰器，容量与缓冲池容量一致
        this.replacer = new LRUCache(poolSize);

        // 初始化所有槽位：填充空 Page 对象，pageId 为 INVALID_PAGE_ID（表示未使用）
        for (int i = 0; i < poolSize; i++) {
            pages[i] = new Page();
        }
    }

    /**
     * 获取指定页面的数据（核心入口方法）。
     * <p>
     * 上层模块（如 B+ 树节点读取）调用此方法获取页面的内存副本。该方法会自动处理：
     * <ul>
     *   <li>缓存命中（Cache Hit）：直接返回内存中的页面，并增加引用计数。</li>
     *   <li>缓存未命中（Cache Miss）：
     *     <ol>
     *       <li>查找或腾出一个空闲槽位（优先空槽，若无则淘汰 LRU 页面）。</li>
     *       <li>若淘汰的页面是脏页，先将其写回磁盘。</li>
     *       <li>从磁盘读取目标页面数据到该槽位。</li>
     *       <li>更新页表与 LRU 状态。</li>
     *     </ol>
     *   </li>
     * </ul>
     *
     * <h3>引用计数管理</h3>
     * 调用此方法后，返回的 {@code Page} 对象的 {@code pinCount} 会增加 1，
     * 同时该页面会从 LRU 淘汰候选队列中移除（{@link LRUCache#pin(int)}）。
     * <b>调用者在用完页面后必须调用 {@link #unpinPage(int, boolean)} 释放。</b>
     *
     * <h3>极端情况</h3>
     * 若缓冲池满且所有页面均被钉住（pinCount > 0），则无法腾出空间，
     * 此时返回 {@code null} 并记录错误日志。上层需自行处理或重试。
     *
     * @param pageId 要获取的页面 ID，必须为非负整数，且已由 {@link DiskManager} 分配。
     * @return 包含页面数据的 {@link Page} 对象；若无法加载（如无空槽位且无法淘汰）则返回 {@code null}。
     */
    public Page fetchPage(int pageId) {
        // ---------- 1. 缓存命中处理 ----------
        // 检查页表，判断目标页面是否已在内存中
        if (pageTable.containsKey(pageId)) {
            // 获取该页面所在的槽位索引
            int frameId = pageTable.get(pageId);
            Page page = pages[frameId];

            // 增加引用计数：表示当前线程正在使用该页面，防止被淘汰
            page.setPinCount(page.getPinCount() + 1);

            // 通知 LRU：此页面已被“钉住”，从淘汰候选队列中移除
            replacer.pin(pageId);

            log.debug("Cache Hit: Fetching page {}", pageId);
            return page;
        }

        // ---------- 2. 缓存未命中：需要从磁盘加载 ----------
        // 目标槽位索引，-1 表示尚未找到
        int targetFrameId = -1;

        // a) 优先寻找空闲槽位（pageId 为 INVALID_PAGE_ID 表示从未被使用或已被清空）
        for (int i = 0; i < poolSize; i++) {
            if (pages[i].getPageId() == Constants.INVALID_PAGE_ID) {
                targetFrameId = i;
                break;
            }
        }

        // b) 若无空闲槽位，则必须淘汰一个页面以腾出空间
        if (targetFrameId == -1) {
            // 向 LRU 淘汰器索要一个牺牲品（最近最少使用的空闲页面）
            int victimPageId = replacer.victimize();

            // 若返回 -1，说明 LRU 候选队列为空，即所有页面都被 pin 住了
            if (victimPageId == -1) {
                log.error("缓冲池已满，且没有可以淘汰的页面！");
                return null; // 上层调用者需自行决定重试或报错
            }

            // 通过页表找到牺牲品所在的槽位索引
            targetFrameId = pageTable.get(victimPageId);
            Page victimPage = pages[targetFrameId];

            // ⚠️ 关键操作：如果被淘汰的页面是脏页（内容已被修改），必须先将数据写回磁盘！
            if (victimPage.isDirty()) {
                log.info("淘汰脏页 {}，正在写回磁盘...", victimPageId);
                diskManager.writePage(victimPageId, victimPage);
                // 注意：写回后脏标记应由 DiskManager 或本方法清除，这里由后续 resetMemory 统一处理
            }

            // 从页表中移除被淘汰页面的映射关系
            pageTable.remove(victimPageId);

            // 重置该槽位的 Page 对象，清空旧数据、脏标记、pinCount 等，使其变为“空闲”状态
            victimPage.resetMemory();
            // 注意：此时 victimPage.getPageId() 已被 resetMemory 置为 INVALID_PAGE_ID
        }

        // ---------- 3. 从磁盘读取目标页面到选定的槽位 ----------
        Page targetPage = pages[targetFrameId];
        // 委托磁盘管理器将 pageId 对应的数据填充到 targetPage 的 data 字段中
        diskManager.readPage(pageId, targetPage);

        // ---------- 4. 更新元数据 ----------
        // 在页表中建立 pageId → frameId 的映射
        pageTable.put(pageId, targetFrameId);
        // 设置初始引用计数为 1（当前调用者正在使用）
        targetPage.setPinCount(1);
        // 通知 LRU：此页面已被 pin，不可淘汰
        replacer.pin(pageId);

        log.debug("Cache Miss: Fetched page {} from disk into frame {}", pageId, targetFrameId);
        return targetPage;
    }

    /**
     * 释放页面的使用（配套方法，极其重要）。
     * <p>
     * 每当调用者完成对某个页面的读写操作后，必须调用此方法通知缓冲池管理器。
     * 此方法会：
     * <ul>
     *   <li>减少该页面的引用计数（{@code pinCount}）。</li>
     *   <li>若调用者修改了页面内容，则设置脏标记（{@code isDirty}）。</li>
     *   <li>当引用计数降至 0 时，将页面重新加入 LRU 淘汰候选队列，允许其被淘汰。</li>
     * </ul>
     *
     * <h3>引用计数机制的重要性</h3>
     * 引用计数是防止页面在使用过程中被意外淘汰的唯一保障。
     * <b>未调用此方法（或未正确配对调用）会导致引用计数泄漏</b>，
     * 使得页面永久处于“被钉住”状态，最终造成缓冲池无法淘汰任何页面而僵死。
     *
     * <h3>参数说明</h3>
     * 调用者必须根据实际对页面的操作正确传递 {@code isDirty}：
     * <ul>
     *   <li>若对页面仅进行了读取操作，则 {@code isDirty = false}。</li>
     *   <li>若对页面进行了写入（修改了 {@link Page#getData()} 内容），则 {@code isDirty = true}。</li>
     * </ul>
     * 脏标记的设置是幂等的：即使多次设置也不会产生副作用。
     *
     * @param pageId  要释放的页面 ID。
     * @param isDirty 若页面内容已被修改，则传 {@code true}；否则传 {@code false}。
     */
    public void unpinPage(int pageId, boolean isDirty) {
        // 防御性检查：若页面已不在缓冲池中（可能已被淘汰），则静默返回
        if (!pageTable.containsKey(pageId)) {
            return;
        }

        // 通过页表找到对应的槽位和 Page 对象
        int frameId = pageTable.get(pageId);
        Page page = pages[frameId];

        // 如果本次使用修改了页面数据，打上脏页标记
        // 注意：若页面原本已是脏页，再次设置无妨
        if (isDirty) {
            page.setDirty(true);
        }

        // 减少引用计数（前提是当前计数大于 0，防止出现负值）
        if (page.getPinCount() > 0) {
            page.setPinCount(page.getPinCount() - 1);
        }

        // 当引用计数降为 0 时，表示没有任何线程在使用此页面，可以将其加入淘汰候选队列
        if (page.getPinCount() == 0) {
            replacer.unpin(pageId);
        }
    }

    /**
     * 分配一个新的页面 ID。
     * <p>
     * 当上层模块需要创建全新的数据页时（例如 B+ 树新增节点、分裂节点、堆表追加新页），
     * 调用此方法向磁盘管理器申请一个全局唯一的、从未使用过的 {@code pageId}。
     * </p>
     * <p>
     * 注意：此方法仅分配 ID，并不会立即在内存中创建对应的页面对象。
     * 调用者需随后通过 {@link #fetchPage(int)} 获取该页面的内存副本以进行初始化写入。
     * </p>
     *
     * @return 新分配的、唯一的页面 ID（通常为非负整数）。
     */
    public int allocatePage() {
        // 直接委托给磁盘管理器生成新 ID
        return diskManager.allocatePage();
    }
}