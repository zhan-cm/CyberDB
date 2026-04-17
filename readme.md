# 🚀 CyberDB: A Relational Database Engine from Scratch

<div align="center">
  <img src="https://img.shields.io/badge/Java-17+-ED8B00?style=for-the-badge&logo=java&logoColor=white" alt="Java"/>
  <img src="https://img.shields.io/badge/JUnit-4.13-25A162?style=for-the-badge&logo=junit5&logoColor=white" alt="JUnit"/>
  <img src="https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge" alt="License"/>
  <img src="https://img.shields.io/badge/Status-Core_Completed-success?style=for-the-badge" alt="Status"/>
</div>

<br/>

> **CyberDB** 是一个完全从零手写的关系型数据库底层存储与执行引擎。
> 本项目摒弃了繁杂的第三方依赖，深入计算机物理边界，在纯 Java 环境下实现了从磁盘 I/O 到内存调度、从 B+ 树索引到火山模型执行器、再到 2PL 并发锁与 WAL 灾难恢复的全链路核心架构。

---

## 💡 核心架构与特性 (Core Architecture)

CyberDB 包含五个相互协同的核心子系统，完整模拟了现代工业级关系型数据库（如 MySQL InnoDB）的底层运转逻辑：

### 1. 💽 存储引擎 (Storage Engine)
* **分槽页架构 (Slotted Page Architecture)**：实现极高空间利用率的 4KB 数据页物理结构。
* **偏移量指针机制**：支持极速的内部键值对平移与无碎片的“逻辑删除 (Logical Deletion)”。
* **EOF 边界防弹衣**：智能处理磁盘物理边界，动态分配零值内存页。

### 2. 🧠 缓冲池调度器 (Buffer Pool Manager)
* **LRU 缓存淘汰算法**：精准管理内存帧 (Frames)，在有限的内存池中压榨出最高的 Cache Hit 命中率。
* **脏页标记与异步落盘**：通过 `isDirty` 标记感知数据变更，保证数据在内存与磁盘间的一致性穿梭。

### 3. ⚡ 索引引擎 (Index Engine)
* **B+ 树 (B+ Tree)**：手写实现关系型数据库检索灵魂，将时间复杂度降至 $\mathcal{O}(\log N)$。
* **动态裂变与生长**：支持叶子节点 (Leaf Node) 与内部节点 (Internal Node) 的动态 `Split`、键值上浮 `Promote`，以及树高的自适应生长。

### 4. 🌋 执行引擎 (Execution Engine)
* **火山模型 (Volcano / Iterator Model)**：基于 `next()` 方法的拉取式执行流。
* **流水线算子**：实现了高度解耦的顺序扫描 (`SeqScanExecutor`) 与条件过滤 (`FilterExecutor`)，完美模拟 `SELECT * WHERE ...` 语义。

### 5. 🛡️ 事务与恢复引擎 (Transactions & Recovery)
* **两阶段锁协议 (2PL)**：基于行级 (Row-level) 的共享锁 (S Lock) 与排他锁 (X Lock)，解决多线程并发下的丢失更新异常。
* **预写式日志 (WAL - Write-Ahead Logging)**：防范机房断电级灾难，实现基于重做 (Redo) 机制的极速持久化与数据恢复。

---

## 📂 核心项目结构 (Directory Structure)

```text
com.zhan.cyberdb
├── buffer/             # 缓冲池管理 (BufferPoolManager)
├── execution/          # 火山模型执行器 (Executor, SeqScan, Filter)
├── index/
│   └── bplus/          # B+ 树索引引擎 (BPlusTree, InternalPage, LeafPage)
├── storage/            # 底层存储与分槽页架构 (DiskManager, Page, TablePage, Tuple)
└── transaction/        # ACID 事务守护者 (LockManager, LogManager)