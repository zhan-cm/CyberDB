package com.zhan.cyberdb.storage;

import com.zhan.cyberdb.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 磁盘管理器：负责物理硬盘文件的读写
 */
public class DiskManager {
    // 引入日志，方便我们在控制台看到酷炫的底层 I/O 操作
    private static final Logger log = LoggerFactory.getLogger(DiskManager.class);
    
    private RandomAccessFile dbFile;//随机访问文件类
    private String fileName;
    private AtomicInteger nextPageId; // 线程安全地生成新的 Page ID  并发安全的 ID 生成器

    public DiskManager(String dbFileDir) {
        this.fileName = dbFileDir + File.separator + "cyber.db";
        try {
            // 如果目录不存在，先创建目录
            File dir = new File(dbFileDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            
            // "rws" 极其重要！
            // r: 读, w: 写, s: 同步(sync)。
            // 每次写入后立即强制操作系统刷新到物理磁盘，这是数据库保证断电不丢数据的底线！
            this.dbFile = new RandomAccessFile(fileName, "rws");
            
            // 数据库重启时，计算文件里已经有多少页了
            long fileSize = dbFile.length();
            int numPages = (int) (fileSize / Constants.PAGE_SIZE);
            this.nextPageId = new AtomicInteger(numPages);
            
            log.info("DiskManager 启动成功，文件: {}, 大小: {} bytes, 已存在页数: {}", fileName, fileSize, numPages);
        } catch (IOException e) {
            log.error("初始化 DiskManager 失败: {}", e.getMessage());
            throw new RuntimeException("无法打开数据库文件", e);
        }
    }

    /**
     * 从磁盘读取一页数据到内存的 Page 对象中
     */
    public void readPage(int pageId, Page page) {
        // 计算这一页在文件中的物理偏移量
        long offset = (long) pageId * Constants.PAGE_SIZE;
        try {
            // 🌟 核心修复：检查我们要读的位置，有没有超出当前文件的总长度
            if (offset < dbFile.length()) {
                // 如果文件足够长，正常将文件指针移动到偏移量位置并读取
                dbFile.seek(offset);
                dbFile.readFully(page.getData());
            } else {
                // 🌟 如果超出了文件现有长度，说明我们在请求一个"未来的新页"
                // 直接用 0 填满这页的内存即可，不要去强读磁盘（防止 EOF 崩溃）
                java.util.Arrays.fill(page.getData(), (byte) 0);
            }

            // 3. 设置 Page 的元数据
            page.setPageId(pageId);
            page.setDirty(false); // 刚读出来，和磁盘一致（或者刚初始化），所以不脏

            log.debug("Read page {} from disk", pageId);
        } catch (IOException e) {
            log.error("读取磁盘页面 {} 失败: {}", pageId, e.getMessage());
            throw new RuntimeException("I/O 读异常", e);
        }
    }

    /**
     * 将内存中 Page 的数据写入到磁盘
     */
    public void writePage(int pageId, Page page) {
        long offset = (long) pageId * Constants.PAGE_SIZE;
        try {
            dbFile.seek(offset);
            dbFile.write(page.getData());
            page.setDirty(false); // 写入磁盘后，内存和磁盘又一致了，洗白了
            
            log.debug("Write page {} to disk", pageId);
        } catch (IOException e) {
            log.error("写入磁盘页面 {} 失败: {}", pageId, e.getMessage());
            throw new RuntimeException("I/O 写异常", e);
        }
    }

    /**
     * 向磁盘申请分配一个新的页面
     * @return 新页面的 ID
     */
    public int allocatePage() {
        // 每次调用，ID 安全地加 1
        return nextPageId.getAndIncrement();
    }

    /**
     * 关闭数据库（系统停机时调用）
     */
    public void shutDown() {
        try {
            dbFile.close();
            log.info("DiskManager 成功关闭");
        } catch (IOException e) {
            log.error("关闭 DiskManager 异常", e);
        }
    }
}