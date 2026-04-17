package com.zhan.cyberdb;

import com.zhan.cyberdb.buffer.BufferPoolManager;
import com.zhan.cyberdb.execution.Executor;
import com.zhan.cyberdb.execution.FilterExecutor;
import com.zhan.cyberdb.execution.SeqScanExecutor;
import com.zhan.cyberdb.storage.DiskManager;
import com.zhan.cyberdb.storage.Page;
import com.zhan.cyberdb.storage.TablePage;
import com.zhan.cyberdb.storage.Tuple;
import com.zhan.cyberdb.transaction.LockManager;
import com.zhan.cyberdb.transaction.LogManager;
import com.zhan.cyberdb.transaction.LogRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CyberDBFinalTest {

    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private LockManager lockManager;
    private LogManager logManager;

    @Before
    public void setUp() {
        System.out.println("========== 🟢 CyberDB 系统冷启动 ==========");
        diskManager = new DiskManager("E:\\cyberdb_data");
        bufferPoolManager = new BufferPoolManager(10, diskManager);
        lockManager = new LockManager();
        logManager = new LogManager();
        System.out.println("各大核心组件装载完毕：Disk, BufferPool, LockManager, WAL LogManager.");
    }

    @Test
    public void testFullACIDPipeline() {
        System.out.println("\n========== 🚀 阶段 1：模拟事务并发与数据写入 ==========");
        
        // 1. 获取一张数据页
        Page rawPage = bufferPoolManager.fetchPage(bufferPoolManager.allocatePage());
        TablePage tablePage = new TablePage(rawPage);
        tablePage.init(-1, -1);
        int targetRowId = 0; // 我们将操作第 0 行

        int txnId = 1001; // 模拟分配一个事务 ID

        try {
            // 2. 事务开始！第一步：保安队长处登记，获取该行的【写锁】 (隔离性 Isolation)
            lockManager.acquireExclusiveLock(targetRowId);

            // 3. 极其关键的 WAL 协议：在真实修改内存前，先写日志！ (持久性 Durability)
            String newData = "Account_A_Balance: 5000";
            LogRecord record = new LogRecord(txnId, "INSERT ROW " + targetRowId + " -> " + newData);
            logManager.appendLogRecord(record);

            // 4. 日志落盘后，放心大胆地修改内存中的数据页
            Tuple tuple = new Tuple(newData.getBytes(StandardCharsets.UTF_8));
            tablePage.insertTuple(tuple);
            bufferPoolManager.unpinPage(rawPage.getPageId(), true); // 标记脏页

            System.out.println("✅ 事务 " + txnId + " 执行成功，数据已写入 BufferPool！");

        } finally {
            // 5. 无论如何，事务结束时必须释放锁
            lockManager.releaseExclusiveLock(targetRowId);
        }

        System.out.println("\n========== 🔍 阶段 2：火山模型流水线查询 ==========");
        
        // 拼装执行器：扫描 -> 过滤
        Executor scan = new SeqScanExecutor(tablePage);
        Executor filter = new FilterExecutor(scan, t -> {
            String content = new String(t.getData(), StandardCharsets.UTF_8);
            return content.contains("Balance"); // 只查询包含 Balance 的行
        });

        filter.init();
        Tuple result;
        while ((result = filter.next()) != null) {
            System.out.println("🎯 执行引擎检索到数据: " + new String(result.getData(), StandardCharsets.UTF_8));
        }

        System.out.println("\n========== ⚡ 阶段 3：模拟天灾与重启恢复 ==========");
        
        // 模拟断电，内存数据丢失，重置各大管理器
        System.out.println("💥 咔嚓！机房停电，BufferPool 内存数据瞬间清空！");
        
        // 系统重启，调用 WAL 日志恢复机制
        logManager.recover();
    }

    @After
    public void tearDown() {
        diskManager.shutDown();
        System.out.println("\n========== 🔴 CyberDB 系统安全关闭 ==========");
    }
}