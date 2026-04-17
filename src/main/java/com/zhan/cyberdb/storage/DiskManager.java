package com.zhan.cyberdb.storage;

import com.zhan.cyberdb.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 磁盘管理器（Disk Manager）。
 * <p>
 * 负责数据库物理文件的底层 I/O 操作，是存储引擎与操作系统文件系统之间的桥梁。
 * 所有页面的读写、新页面的分配均由此类统一代理。
 * </p>
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li><b>页面读取</b>：根据页面 ID 将磁盘文件中的对应 4KB 数据块读入内存 {@link Page} 对象。</li>
 *   <li><b>页面写入</b>：将内存中被修改的 {@link Page} 对象写回磁盘文件的正确位置。</li>
 *   <li><b>页面分配</b>：生成全局唯一且递增的新页面 ID，用于创建新页。</li>
 *   <li><b>文件生命周期管理</b>：打开数据库文件（若不存在则创建），并在系统关闭时安全关闭文件句柄。</li>
 * </ul>
 *
 * <h2>与 BufferPoolManager 的协作</h2>
 * <p>
 * {@code DiskManager} 不关心缓存策略，只被动响应 {@link com.zhan.cyberdb.buffer.BufferPoolManager}
 * 发起的读写请求。缓冲池负责决定何时将脏页写回（淘汰或检查点），本类仅执行具体的 I/O 指令。
 * </p>
 *
 * <h2>线程安全性</h2>
 * <p>
 * {@link RandomAccessFile} 的读写操作本身是线程安全的（由 JVM 底层同步），
 * 但本类未额外施加并发控制。在多线程并发读写不同页面时，由上层缓冲池或事务管理器保证页面级的互斥。
 * {@link #nextPageId} 使用 {@link AtomicInteger} 确保分配页面 ID 的原子性。
 * </p>
 *
 * <h2>文件布局</h2>
 * <p>
 * 数据库文件（默认为 {@code cyber.db}）被划分为连续的 4KB 页面，页面 ID 从 0 开始线性递增。
 * 页面 ID 为 {@code N} 的页在文件中的物理偏移量为 {@code N * PAGE_SIZE} 字节。
 * 文件末尾可能包含未完全写满最后一个页面的情况，但逻辑上只按完整页面管理。
 * </p>
 *
 * @author Zhan
 * @version 1.0
 * @see Page
 * @see Constants#PAGE_SIZE
 * @see com.zhan.cyberdb.buffer.BufferPoolManager
 */
public class DiskManager {

    /**
     * SLF4J 日志记录器。
     * <p>
     * 用于输出 I/O 操作、文件状态、异常等关键信息，便于调试和监控数据库运行状况。
     * 在生产环境中可通过日志配置文件调整输出级别（如 DEBUG、INFO、ERROR）。
     * </p>
     */
    private static final Logger log = LoggerFactory.getLogger(DiskManager.class);

    /**
     * 随机访问文件对象，代表磁盘上的数据库物理文件。
     * <p>
     * 使用 {@link RandomAccessFile} 而非 {@link java.io.FileInputStream} 等流式 I/O，
     * 是因为数据库需要能够在文件的任意偏移位置进行随机读写（Random Access）。
     * 例如读取页面 9527 时，可以直接将文件指针定位到 {@code 9527 * 4096} 字节处。
     * </p>
     *
     * <h3>关于 "rws" 模式</h3>
     * <p>
     * 构造时传入的模式字符串 {@code "rws"} 具有至关重要的意义：
     * <ul>
     *   <li><b>r</b>：允许读取（read）。</li>
     *   <li><b>w</b>：允许写入（write）。</li>
     *   <li><b>s</b>：同步模式（synchronous）。每当调用 {@code write()} 方法后，
     *       底层会强制将数据立即刷新到物理存储设备（磁盘），而非仅停留在操作系统的页缓存中。
     *       这是数据库保证事务持久性（Durability）的底线配置——即使系统突然崩溃或断电，
     *       已提交的写操作也不会丢失。</li>
     * </ul>
     * 代价是每次写入都会引发磁盘同步，性能较普通写入有所下降。实际生产数据库（如 MySQL）
     * 通常会结合组提交（Group Commit）和异步刷盘策略来平衡性能与可靠性。
     * </p>
     */
    private RandomAccessFile dbFile;

    /**
     * 数据库文件的完整路径名。
     * <p>
     * 由构造时传入的目录路径和固定文件名 {@code "cyber.db"} 拼接而成。
     * 例如传入 {@code "/var/cyberdb/data"}，则完整路径为 {@code "/var/cyberdb/data/cyber.db"}。
     * </p>
     */
    private String fileName;

    /**
     * 下一个可用的页面 ID 生成器（原子整型）。
     * <p>
     * 数据库每次需要创建新页面时（例如 B+ 树分裂产生新节点、堆表追加新页），
     * 都通过调用 {@link #allocatePage()} 获取一个全局唯一且严格递增的页面 ID。
     * </p>
     * <p>
     * 使用 {@link AtomicInteger} 保证多线程并发分配时的线程安全，无需额外加锁。
     * 初始值由构造函数根据现有文件大小计算得出（文件中已有页面数），
     * 确保数据库重启后新分配的页面 ID 不会与已存在的页面冲突。
     * </p>
     */
    private AtomicInteger nextPageId;

    /**
     * 构造并初始化磁盘管理器。
     * <p>
     * 此方法会执行以下关键步骤：
     * <ol>
     *   <li>检查并创建数据库文件所在目录（若不存在）。</li>
     *   <li>以 {@code "rws"} 同步模式打开（或创建）数据库文件。</li>
     *   <li>计算当前文件大小，据此推断已存在的页面数量，初始化 {@code nextPageId}。</li>
     *   <li>记录启动日志，输出文件路径、大小及已有页面数。</li>
     * </ol>
     * 若任何步骤失败，将抛出运行时异常并终止初始化。
     * </p>
     *
     * @param dbFileDir 数据库文件存放的目录路径（例如 {@code "./data"} 或 {@code "/var/cyberdb"}）。
     *                  若目录不存在，将尝试递归创建。
     * @throws RuntimeException 如果目录创建失败或文件打开失败。
     */
    public DiskManager(String dbFileDir) {
        // 拼接完整文件路径
        this.fileName = dbFileDir + File.separator + "cyber.db";
        try {
            // 1. 确保父目录存在
            File dir = new File(dbFileDir);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                if (!created) {
                    throw new IOException("无法创建数据库目录: " + dbFileDir);
                }
                log.info("数据库目录不存在，已自动创建: {}", dir.getAbsolutePath());
            }

            // 2. 以 "rws" 模式打开文件（若文件不存在则自动创建）
            this.dbFile = new RandomAccessFile(fileName, "rws");

            // 3. 计算现有页面数量，初始化 nextPageId
            long fileSize = dbFile.length();
            // 页面数量 = 文件总字节数 / 每页大小（取整，余数忽略，因为文件大小应是页大小的整数倍）
            int numPages = (int) (fileSize / Constants.PAGE_SIZE);
            this.nextPageId = new AtomicInteger(numPages);

            log.info("DiskManager 启动成功，文件: {}, 大小: {} bytes, 已存在页数: {}",
                    fileName, fileSize, numPages);
        } catch (IOException e) {
            log.error("初始化 DiskManager 失败: {}", e.getMessage(), e);
            throw new RuntimeException("无法打开或创建数据库文件: " + fileName, e);
        }
    }

    /**
     * 从磁盘读取指定页面到内存 {@link Page} 对象中。
     * <p>
     * 此方法会被 {@link com.zhan.cyberdb.buffer.BufferPoolManager} 在缓存未命中时调用。
     * 它会将磁盘文件中对应该 {@code pageId} 的 4KB 数据块填充到传入的 {@code page} 对象的
     * {@code data} 字节数组中，并更新页面的元数据。
     * </p>
     *
     * <h3>边界情况处理：读取未写入过的页面</h3>
     * <p>
     * 当请求的 {@code pageId} 对应的文件偏移量大于等于当前文件大小时，表示这是一个尚未被写入过磁盘的
     * “新页面”（例如 B+ 树分裂刚分配的节点）。此时若强制调用 {@code readFully} 会触发
     * {@code EOFException}。为避免此异常，本方法会检测此情况，并直接用全 0 填充页面数据，
     * 模拟一个空白的初始页面。
     * </p>
     * <p>
     * 这种设计使得上层逻辑无需区分页面是否已物理存在，统一调用 {@code readPage} 即可。
     * </p>
     *
     * <h3>页面元数据设置</h3>
     * <ul>
     *   <li>将 {@code page} 的 {@code pageId} 设置为传入的 ID。</li>
     *   <li>将 {@code dirty} 标记重置为 {@code false}（刚读出或初始化的页面与磁盘内容一致）。</li>
     * </ul>
     *
     * @param pageId 要读取的页面 ID。必须为非负整数。
     * @param page   用于接收磁盘数据的内存 {@link Page} 对象。其内部 {@code data} 数组必须已分配 4KB 空间。
     * @throws RuntimeException 如果发生 I/O 错误（如磁盘损坏、文件被意外截断等）。
     */
    public void readPage(int pageId, Page page) {
        // 计算该页面在文件中的物理偏移量（页面 ID × 页面大小）
        long offset = (long) pageId * Constants.PAGE_SIZE;
        try {
            // 关键边界检查：要读取的位置是否超出当前文件长度？
            if (offset < dbFile.length()) {
                // 文件足够长：正常定位并读取 4KB 数据
                dbFile.seek(offset);
                dbFile.readFully(page.getData());
            } else {
                // 文件尚未延伸到该偏移量：说明请求的是一个从未写入过的“新页”
                // 直接用 0 填充页面数据，模拟一个全新的空白页
                java.util.Arrays.fill(page.getData(), (byte) 0);
                log.trace("Page {} 尚未写入磁盘，返回全零页", pageId);
            }

            // 设置页面的元数据
            page.setPageId(pageId);
            page.setDirty(false); // 刚读出/初始化的页面与磁盘一致，脏标记清除

            log.debug("Read page {} from disk", pageId);
        } catch (IOException e) {
            log.error("读取磁盘页面 {} 失败: {}", pageId, e.getMessage(), e);
            throw new RuntimeException("磁盘 I/O 读取异常，页面 ID: " + pageId, e);
        }
    }

    /**
     * 将内存中的页面数据写回磁盘。
     * <p>
     * 此方法通常在两种情况下被调用：
     * <ul>
     *   <li>缓冲池淘汰一个脏页时（通过 {@link com.zhan.cyberdb.buffer.BufferPoolManager}）。</li>
     *   <li>数据库执行检查点（Checkpoint）或关闭时，强制将所有脏页刷盘。</li>
     * </ul>
     * 写入完成后，会将该页面的脏标记清除（{@code page.setDirty(false)}），
     * 表示内存与磁盘内容再次一致。
     * </p>
     *
     * <h3>同步写入保证</h3>
     * 由于文件以 {@code "rws"} 模式打开，调用 {@code dbFile.write()} 后，
     * 操作系统会确保数据被写入物理存储介质后才返回。这提供了强持久性保证。
     *
     * @param pageId 要写入的页面 ID。用于计算文件偏移量，应与 {@code page.getPageId()} 一致。
     * @param page   包含待写入数据的内存页面对象。
     * @throws RuntimeException 如果发生 I/O 错误（如磁盘满、权限不足等）。
     */
    public void writePage(int pageId, Page page) {
        long offset = (long) pageId * Constants.PAGE_SIZE;
        try {
            // 移动文件指针到目标偏移量
            dbFile.seek(offset);
            // 将页面数据字节数组完整写入文件
            dbFile.write(page.getData());
            // 清除脏标记：内存与磁盘再次同步
            page.setDirty(false);

            log.debug("Write page {} to disk", pageId);
        } catch (IOException e) {
            log.error("写入磁盘页面 {} 失败: {}", pageId, e.getMessage(), e);
            throw new RuntimeException("磁盘 I/O 写入异常，页面 ID: " + pageId, e);
        }
    }

    /**
     * 分配一个新的页面 ID。
     * <p>
     * 数据库在需要创建新页面时（例如 B+ 树新增节点、堆表扩展）调用此方法，
     * 获取一个全局唯一且严格递增的页面标识符。该 ID 随后可用于 {@link #readPage(int, Page)}
     * 和 {@link #writePage(int, Page)} 等操作。
     * </p>
     * <p>
     * 本方法不实际在磁盘上分配空间或创建文件空洞。物理空间的分配将推迟到第一次
     * {@link #writePage(int, Page)} 调用时，由操作系统在文件末尾扩展。
     * 这种延迟分配（Lazy Allocation）策略减少了不必要的磁盘 I/O。
     * </p>
     *
     * @return 新分配的页面 ID。每次调用返回值递增 1。
     */
    public int allocatePage() {
        // 原子地获取当前值并自增，保证并发安全
        int newId = nextPageId.getAndIncrement();
        log.debug("Allocated new page ID: {}", newId);
        return newId;
    }

    /**
     * 获取当前数据库文件中已分配的页面总数。
     * <p>
     * 注意：此数值等于 {@code nextPageId} 的当前值，表示“下一个可用的页面 ID”，
     * 也即当前文件中已存在的最大页面 ID 加 1（假设页面 ID 从 0 开始连续分配）。
     * </p>
     *
     * @return 当前已分配的页面总数。
     */
    public int getNumPages() {
        return nextPageId.get();
    }

    /**
     * 关闭磁盘管理器，释放文件句柄。
     * <p>
     * 应在数据库系统正常关闭时调用。调用后，任何后续的读写操作都将失败。
     * 此方法会尝试关闭底层的 {@link RandomAccessFile}，并记录成功或异常日志。
     * </p>
     */
    public void shutDown() {
        try {
            if (dbFile != null) {
                dbFile.close();
                log.info("DiskManager 成功关闭，数据库文件已安全释放");
            }
        } catch (IOException e) {
            log.error("关闭 DiskManager 时发生异常", e);
        }
    }
}