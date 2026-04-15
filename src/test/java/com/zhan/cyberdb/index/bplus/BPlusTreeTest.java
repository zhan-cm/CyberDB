package com.zhan.cyberdb.index.bplus;

import com.zhan.cyberdb.buffer.BufferPoolManager;
import com.zhan.cyberdb.storage.DiskManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BPlusTreeTest {

    private DiskManager diskManager;
    private BufferPoolManager bpm;
    private BPlusTree bPlusTree;

    @Before
    public void setUp() {
        // 1. 初始化磁盘苦力和车间主任 (给 10 个页面的缓存池)
        diskManager = new DiskManager("E:\\cyberdb_data");
        bpm = new BufferPoolManager(10, diskManager);
        
        // 2. 初始化 B+树指挥官！
        // 🌟 注意这里：maxDegree=3 意味着只要塞入 3 个元素，叶子就会爆炸分裂！
        bPlusTree = new BPlusTree(bpm, 3);
    }

    @Test
    public void testBPlusTreeInsertAndSearch() {
        System.out.println("========== B+ 树点燃引擎，测试开始！ ==========");
        
        // 1. 插入第一条数据 (建立平房)
        System.out.println(">>> 插入 Key: 10, Value: 100");
        bPlusTree.insert(10, 100);
        
        System.out.println(">>> 插入 Key: 20, Value: 200");
        bPlusTree.insert(20, 200);

        // 2. 插入第三条数据，触发史诗级裂变！(因为 maxDegree = 3)
        System.out.println(">>> 插入 Key: 30, Value: 300 (⚠️ 即将触发节点满载与树高增长！)");
        bPlusTree.insert(30, 300);

        // 3. 继续插入，验证分裂后的路由是否正确
        System.out.println(">>> 插入 Key: 15, Value: 150 (测试插入到左兄弟)");
        bPlusTree.insert(15, 150);
        
//        System.out.println(">>> 插入 Key: 40, Value: 400 (测试插入到右兄弟)");
//        bPlusTree.insert(40, 400);

        System.out.println("\n========== 数据写入完毕，开启光速点查！ ==========");

        // 4. 从树根一路查下来，看看能不能找到！
        Integer val10 = bPlusTree.getValue(10);
        System.out.println("搜索 Key 10，找到 Value: " + val10);
        assertEquals(Integer.valueOf(100), val10);

        Integer val30 = bPlusTree.getValue(30);
        System.out.println("搜索 Key 30，找到 Value: " + val30);
        assertEquals(Integer.valueOf(300), val30);

        Integer val15 = bPlusTree.getValue(15);
        System.out.println("搜索 Key 15，找到 Value: " + val15);
        assertEquals(Integer.valueOf(150), val15);

        // 5. 找一个不存在的 Key
        Integer val99 = bPlusTree.getValue(99);
        System.out.println("搜索不存在的 Key 99，结果为: " + val99);
        assertEquals(null, val99);

        System.out.println("========== 恭喜！B+ 树测试完美绿灯通关！ ==========");
    }

    @After
    public void tearDown() {
        diskManager.shutDown();
    }
}