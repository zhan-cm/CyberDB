package com.zhan.cyberdb.buffer;

import com.zhan.cyberdb.storage.DiskManager;
import com.zhan.cyberdb.storage.Page;

public class DiskTest {
    public static void main(String[] args) {
        // 1. 在你电脑的 D 盘或 E 盘建一个测试目录
        DiskManager diskManager = new DiskManager("E:\\CyberDB\\cyberdb_data");
        
        // 2. 分配一个新页面
        int newPageId = diskManager.allocatePage();
        Page page = new Page();
        page.setPageId(newPageId);
        
        // 3. 在这个页面的开头写一点数据（比如写入字母 'A'，ASCII 码是 65）
        page.getData()[0] = 65;
        
        // 4. 写入磁盘
        diskManager.writePage(newPageId, page);
        diskManager.shutDown();
        
        System.out.println("写入完成！请去 E:\\cyberdb_data 目录下看看有没有生成 cyber.db 文件！");
    }
}