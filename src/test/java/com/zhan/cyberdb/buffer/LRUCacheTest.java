package com.zhan.cyberdb.buffer;

import org.junit.Assert;
import org.junit.Test;

public class LRUCacheTest {

    @Test
    public void testLRUBehavior() {
        // 创建一个容量只有 3 的缓存池
        LRUCache cache = new LRUCache(3);

        // 访问页面 1, 2, 3
        cache.accessPage(1);
        cache.accessPage(2);
        cache.accessPage(3);
        // 此时缓存里应该是 [3, 2, 1]，3在最前面（头部），1在最后面（尾部）

        // 再次访问页面 1 (缓存命中 Cache Hit)
        cache.accessPage(1);
        // 此时 1 被移动到了头部，缓存顺序变成 [1, 3, 2]

        // 访问一个新页面 4 (缓存未命中 Cache Miss，且容量已满)
        cache.accessPage(4);
        // 此时容量超载，必须踢掉最不常使用的尾部元素。
        // 因为上一步 1 被拉到了前面，所以现在的尾部是 2。2应该被踢掉。
        // 现在的缓存顺序应该是 [4, 1, 3]

        // 我们写一个简单的打印，或者后面教你写更严谨的断言。
        System.out.println("LRU Cache 逻辑运行完毕，未抛出异常！");

        // 思考题：如果你现在能在 LRUCache 里写一个 printCache() 方法打印出当前链表的 pageId，
        // 它的输出顺序应该是 4 -> 1 -> 3。
    }
}