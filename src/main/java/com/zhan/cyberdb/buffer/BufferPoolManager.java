package com.zhan.cyberdb.buffer;

import com.zhan.cyberdb.common.Constants;
import com.zhan.cyberdb.storage.DiskManager;
import com.zhan.cyberdb.storage.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 缓冲池管理器：整个存储引擎的交通枢纽
 */
public class BufferPoolManager {
    private static final Logger log = LoggerFactory.getLogger(BufferPoolManager.class);

    private int poolSize;
    // 内存中实际存放 Page 的大数组（缓冲池本体）
    private Page[] pages; 
    
    // 页表：记录某个 pageId 放在 pages 数组的哪个索引(槽位)里
    private Map<Integer, Integer> pageTable; 
    
    // 我们刚才写的两个大将
    private DiskManager diskManager;
    private LRUCache replacer;

    public BufferPoolManager(int poolSize, DiskManager diskManager) {
        this.poolSize = poolSize;
        this.diskManager = diskManager;
        this.pages = new Page[poolSize];
        this.pageTable = new HashMap<>();
        this.replacer = new LRUCache(poolSize);

        // 初始化时，把数组里填满空的 Page 对象
        for (int i = 0; i < poolSize; i++) {
            pages[i] = new Page();
        }
    }

    /**
     * 核心方法：获取一页数据
     */
    public Page fetchPage(int pageId) {
        // 1. 如果页面已经在缓冲池里了 (Cache Hit)
        if (pageTable.containsKey(pageId)) {
            int frameId = pageTable.get(pageId); // 拿到在数组里的索引
            Page page = pages[frameId];
            page.setPinCount(page.getPinCount() + 1); // 有人用了，pin count + 1
            replacer.pin(pageId); // 告诉 LRU，这个页正被用着，别踢它
            log.debug("Cache Hit: Fetching page {}", pageId);
            return page;
        }

        // 2. 缓存未命中 (Cache Miss)，需要找个空槽位，或者踢掉一个旧的
        // 要的数据不在内存里 → 必须从磁盘加载进来。
        //但内存满了，要么找个空位置，要么踢掉一个不用的旧数据
        int targetFrameId = -1;

        // a) 先看数组里有没有完全没用过的空槽位（比如刚开机时）
        for (int i = 0; i < poolSize; i++) {
            if (pages[i].getPageId() == Constants.INVALID_PAGE_ID) {
                targetFrameId = i;
                break;
            }
        }

        // b) 如果没有空槽位了，找 LRU 踢人！
        if (targetFrameId == -1) {
            int victimPageId = replacer.victimize();
            if (victimPageId == -1) {
                // 惨烈的情况：内存满了，而且所有页面都在被别人疯狂读取（都被 pin 住了）
                log.error("缓冲池已满，且没有可以淘汰的页面！");
                return null; 
            }
            targetFrameId = pageTable.get(victimPageId);
            Page victimPage = pages[targetFrameId];

            // ⚠️ 关键动作：如果是脏页，必须立刻写回磁盘！
            if (victimPage.isDirty()) {
                log.info("淘汰脏页 {}，正在写回磁盘...", victimPageId);
                diskManager.writePage(victimPageId, victimPage);
            }
            
            // 洗白槽位，从页表中删除旧的记录
            pageTable.remove(victimPageId);
            victimPage.resetMemory(); 
        }

        // 3. 让苦力从磁盘把新页面读到刚才找到的 targetFrameId 槽位里
        Page targetPage = pages[targetFrameId];
        diskManager.readPage(pageId, targetPage);
        
        // 4. 更新各种状态
        pageTable.put(pageId, targetFrameId);
        targetPage.setPinCount(1); // 我现在要用了，pin 住
        replacer.pin(pageId);      // 告诉 LRU 不能踢

        log.debug("Cache Miss: Fetched page {} from disk into frame {}", pageId, targetFrameId);
        return targetPage;
    }

    /**
     * 核心配套方法：用完页面后，告诉主任我用完了 (极其重要)
     */
    public void unpinPage(int pageId, boolean isDirty) {
        if (!pageTable.containsKey(pageId)) return;
        
        int frameId = pageTable.get(pageId);
        Page page = pages[frameId];
        
        // 如果这次使用修改了数据，打上脏页标记
        if (isDirty) {
            page.setDirty(true);
        }
        
        if (page.getPinCount() > 0) {
            page.setPinCount(page.getPinCount() - 1);
        }
        
        // 如果没有人再用它了，放回 LRU 候选队列，随时准备被淘汰
        if (page.getPinCount() == 0) {
            replacer.unpin(pageId);
        }
    }

    /**
     * 核心配套方法：向系统申请一个全新的页面 ID
     * (当 B+ 树需要造新节点、或者分裂节点时调用)
     */
    public int allocatePage() {
        // 直接让底层的磁盘苦力去生成一个绝对不会重复的新 Page ID
        return diskManager.allocatePage();
    }
}