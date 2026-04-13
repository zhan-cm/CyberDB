package com.zhan.cyberdb.storage;

import com.zhan.cyberdb.common.Constants;
import java.util.Arrays;

/**
 * 数据库内存中的基本存储单元：页 (Page)
 */
public class Page {
    
    // 1. 身份标识：我是第几页？
    private int pageId;       
    
    // 2. 真实数据：4KB 的字节数组，数据其实全躺在这里面
    private byte[] data;      
    
    // 3. 脏页标记 (Dirty Flag)
    // 极其重要！如果这个页面在内存里被修改了，它就变"脏"了。
    // 当 LRU 把这个页踢出内存时，如果它是脏的，我们就必须把它写回磁盘，否则数据就丢了！
    private boolean isDirty;  
    
    // 4. 引用计数 (Pin Count)
    // 极其重要！如果有线程正在读取或修改这个页面，pinCount 就会大于 0（被钉住了）。
    // 哪怕这个页在 LRU 的最尾部，只要 pinCount > 0，LRU 绝对不能把它踢出去！
    private int pinCount;     

    public Page() {
        this.pageId = Constants.INVALID_PAGE_ID;
        this.data = new byte[Constants.PAGE_SIZE];
        this.isDirty = false;
        this.pinCount = 0;
    }

    /**
     * 当页面被复用时，清空内存数据
     */
    public void resetMemory() {
        Arrays.fill(data, (byte) 0);
        isDirty = false;
        pinCount = 0;
        pageId = Constants.INVALID_PAGE_ID;
    }

    // --- 下面是标准的 Getters 和 Setters ---
    
    public int getPageId() { return pageId; }
    public void setPageId(int pageId) { this.pageId = pageId; }

    public byte[] getData() { return data; }

    public boolean isDirty() { return isDirty; }
    public void setDirty(boolean dirty) { isDirty = dirty; }

    public int getPinCount() { return pinCount; }
    public void setPinCount(int pinCount) { this.pinCount = pinCount; }
}