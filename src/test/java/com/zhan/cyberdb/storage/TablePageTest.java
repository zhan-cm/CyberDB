package com.zhan.cyberdb.storage;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TablePageTest {

    @Test
    public void testTablePageInsert() {
        System.out.println("========== 分槽页插入测试开始 ==========");
        
        // 1. 造一个干净的内存页
        Page rawPage = new Page();
        TablePage tablePage = new TablePage(rawPage);
        
        // 2. 初始化它 (前后没有链接的页，传 -1)
        tablePage.init(-1, -1);
        System.out.println("初始化完成。初始剩余空间指针: " + tablePage.getFreeSpacePointer());

        // 3. 造第一条数据
        String data1 = "Hello CyberDB!";
        Tuple tuple1 = new Tuple(data1.getBytes(StandardCharsets.UTF_8));
        
        // 4. 插入第一条数据
        boolean isSuccess1 = tablePage.insertTuple(tuple1);
        assertTrue(isSuccess1);
        
        System.out.println("\n插入 Tuple 1 成功！大小: " + tuple1.getLength() + " 字节");
        System.out.println("当前 Tuple 数量: " + tablePage.getTupleCount());
        System.out.println("当前剩余空间指针: " + tablePage.getFreeSpacePointer());
        
        // 验证你的公式算得对不对：4096 - 14 = 4082
        assertEquals(4096 - tuple1.getLength(), tablePage.getFreeSpacePointer());

        // 5. 暴力测试：造一个极其巨大的数据（4000字节），试图把它塞进去
        byte[] giantData = new byte[4050];
        Tuple giantTuple = new Tuple(giantData);
        
        System.out.println("\n试图插入一个 4000 字节的巨型 Tuple...");
        boolean isSuccess2 = tablePage.insertTuple(giantTuple);
        
        // 空间绝对不够（因为还有 Header 和槽位的开销），必须返回 false！
        assertFalse(isSuccess2);
        System.out.println("插入巨型 Tuple 失败（预期之内），页面保护机制生效！");
        
        System.out.println("========== 测试完美结束 ==========");
    }

    @Test
    public void testTablePageReadAndDelete() {
        System.out.println("========== 读取与删除测试开始 ==========");

        Page rawPage = new Page();
        TablePage tablePage = new TablePage(rawPage);
        tablePage.init(-1, -1);

        // 1. 插入三条数据
        tablePage.insertTuple(new Tuple("Data_Zero".getBytes(StandardCharsets.UTF_8))); // Slot 0
        tablePage.insertTuple(new Tuple("Data_One".getBytes(StandardCharsets.UTF_8)));  // Slot 1
        tablePage.insertTuple(new Tuple("Data_Two".getBytes(StandardCharsets.UTF_8)));  // Slot 2

        // 2. 精准读取 Slot 1 (中间那条)
        Tuple tuple1 = tablePage.getTuple(1);
        String resultStr = new String(tuple1.getData(), StandardCharsets.UTF_8);
        System.out.println("读取 Slot 1 成功，内容是: " + resultStr);
        assertEquals("Data_One", resultStr);

        // 3. 施展逻辑删除大法，删除 Slot 1
        boolean deleteSuccess = tablePage.deleteTuple(1);
        assertTrue(deleteSuccess);
        System.out.println("删除 Slot 1 成功！");

        // 4. 再次试图读取 Slot 1，应该返回 null（因为它在目录里已经被抹掉了）
        Tuple ghostTuple = tablePage.getTuple(1);
        assertTrue(ghostTuple == null);
        System.out.println("再次读取 Slot 1 结果为 null，符合逻辑删除预期！");

        // 5. 验证旁边的兄弟有没有受影响
        Tuple tuple0 = tablePage.getTuple(0);
        assertEquals("Data_Zero", new String(tuple0.getData(), StandardCharsets.UTF_8));
        System.out.println("Slot 0 的数据完好无损！");

        System.out.println("========== 测试完美结束 ==========");
    }
}