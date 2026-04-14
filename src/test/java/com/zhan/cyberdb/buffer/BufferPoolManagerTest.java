package com.zhan.cyberdb.buffer;

import com.zhan.cyberdb.storage.DiskManager;
import com.zhan.cyberdb.storage.Page;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BufferPoolManagerTest {

    private DiskManager diskManager;
    private BufferPoolManager bpm;

    @Before
    public void setUp() {
        // 1. 初始化磁盘苦力，放在测试目录下（你可以改成 E:\cyberdb_data）
        diskManager = new DiskManager("E:\\CyberDB\\cyberdb_data");
        // 2. 初始化车间主任，故意把内存池设置得很小：只能装 3 个页面！
        bpm = new BufferPoolManager(3, diskManager);
    }

    @Test
    public void testBufferPoolEvictionAndDirtyWrite() {
        System.out.println("========== 测试开始 ==========");
        
        // 步骤 1：向主任申请第 1 页。并在里面写上字母 'A' (ASCII码 65)
        Page page1 = bpm.fetchPage(1);
        assertNotNull(page1);
        page1.getData()[0] = 65; 
        // 极其重要：用完归还，并告诉主任“我弄脏了它！”
        bpm.unpinPage(1, true); 
        System.out.println("Page 1 (脏页) 已放入缓冲池。\n");

        // 步骤 2：把缓冲池塞满。依次申请第 2 页和第 3 页，看完直接还回去（没弄脏）
        Page page2 = bpm.fetchPage(2);
        bpm.unpinPage(2, false);
        
        Page page3 = bpm.fetchPage(3);
        bpm.unpinPage(3, false);
        
        System.out.println("当前内存池已满，顺序为：[3, 2, 1] (1最老，且是脏页)\n");

        // 步骤 3：高潮来了！申请第 4 页！
        // 内存满了，必须踢掉最老的 Page 1。因为它是脏的，注意看控制台会不会打印【写回磁盘】的日志！
        System.out.println(">>> 准备申请 Page 4，触发淘汰机制！");
        Page page4 = bpm.fetchPage(4);
        assertNotNull(page4);
        bpm.unpinPage(4, false);
        System.out.println("Page 4 申请成功！\n");

        // 步骤 4：验证数据没丢！再次申请 Page 1！
        // 因为 Page 1 刚才被踢了，所以这次必须从磁盘重新读。看控制台有没有【从磁盘读取】的日志！
        System.out.println(">>> 准备再次申请被踢掉的 Page 1！");
        Page page1Again = bpm.fetchPage(1);
        assertNotNull(page1Again);
        
        // 步骤 5：断言验证。如果能读出 65 ('A')，说明脏页落盘完美成功！
        assertEquals(65, page1Again.getData()[0]);
        System.out.println("牛逼！从磁盘重新读出的 Page 1 中，数据 'A' 完好无损！");
        
        bpm.unpinPage(1, false);
        System.out.println("========== 测试完美结束 ==========");
    }

    @After
    public void tearDown() {
        // 测试结束后关闭磁盘文件
        diskManager.shutDown();
    }
}