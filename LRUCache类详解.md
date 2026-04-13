### 核心矛盾：为什么不用普通的数组或 List？
在数据库中，缓存池的操作极其频繁，每秒可能发生几万次。我们对它有两个极其苛刻的要求：
1.查找要快：给一个 pageId，必须瞬间（$O(1)$ 时间）知道它在不在内存里。
2.移动/删除要快：每次访问一个页面，都要把它移到最前面；缓存满了，要把最后面的删掉。这个动作也必须是瞬间（$O(1)$ 时间）完成。

如果你用普通的 ArrayList，找数据要从头遍历（慢），把尾部数据挪到头部需要让所有数据往后退一格（极慢）。

大厂的破局之道：哈希表（HashMap） + 双向链表（Double Linked List）。
- HashMap 是“目录”，负责瞬间找到数据的位置。
- 双向链表 是“队伍”，负责瞬间把人拉出队伍排到最前面。

### 逐个剖析变量（成员属性）我们看着代码，挨个解释为什么要定义它们。
##### 1. 为什么要设计 DLinkedNode（双向链表节点）？

```
class DLinkedNode {
int pageId;    
DLinkedNode prev; // 指向前一个节点
DLinkedNode next; // 指向后一个节点
}
```

为什么需要 prev 和 next？ 想象大家手拉手排成一排。
如果某个人（节点）要离开队伍插到最前面，他必须知道左手牵着谁（prev），右手牵着谁（next）。
这样他松手后，他左右两边的人才能立刻拉上手（这就完成了 $O(1)$ 的删除）。如果是单向链表，他就不知道前面的人是谁，还得从头找一遍。


##### 2.为什么要用 Map<Integer, DLinkedNode> cache？

```private Map<Integer, DLinkedNode> cache = new HashMap<>();```
**它的作用**：这是一个“花名册”。键（Key）是 pageId，值（Value）是这个节点在内存中的具体位置（对象的引用）。
**为什么**：当你要找 pageId = 5 时，直接 cache.get(5) 就能一把抓出这个节点，不用在链表里顺藤摸瓜地找。

###### 3.为什么要有 size 和 capacity？

```
private int size;
private int capacity;
```

**capacity（容量）**：数据库给缓冲池分配的固定大小（比如只能装 10 个 Page）。
**size（当前大小）**：现在装了几个。用来判断是不是满了，满了就要触发“踢人”逻辑。


##### 4.最精妙的设计：为什么要 head 和 tail（虚拟头尾节点）？

```
private DLinkedNode head, tail;

public LRUCache(int capacity) {
// ... 初始化代码 ...
head = new DLinkedNode();
tail = new DLinkedNode();
head.next = tail;
tail.prev = head;
}
```
**这是整个数据结构中最神来之笔的地方，叫“哨兵机制”。**

**为什么**：假设没有这两个虚拟节点，当队伍是空的时候，你要加第一个人，你需要写 if (当前没人) { 头=他; 尾=他 }。
当队伍里只有一个人你要删除他时，又要写 if (只剩一个) { 头=null; 尾=null }。
各种边界条件的 if-else 会把你逼疯，而且容易出 Bug。

**有了哨兵**：head 和 tail 是两个隐形人，永远站在队伍的最前和最后。队伍永远不会为空（最少也有他们俩）。
任何新来的节点，直接往 head 后面塞；任何要滚蛋的节点，直接从 tail 前面拔。代码逻辑极其清爽！
