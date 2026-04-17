package com.zhan.cyberdb.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 全局锁管理器（Lock Manager / 保安队长）。
 * <p>
 * 负责管理整个数据库中所有数据行（Tuple/Record）的并发访问权限，实现基于行级锁（Row-Level Locking）
 * 的共享锁（Shared Lock，S锁）和排他锁（Exclusive Lock，X锁）机制。这是数据库事务隔离性和并发控制的
 * 核心组件，确保多个线程/事务在访问同一数据行时满足 ACID 中的隔离性（Isolation）。
 * </p>
 *
 * <h2>锁类型与兼容性矩阵</h2>
 * <table border="1">
 *   <caption>锁兼容性</caption>
 *   <tr><th>已持有锁 \ 请求锁</th><th>共享锁（S）</th><th>排他锁（X）</th></tr>
 *   <tr><td>无锁</td><td>✅ 允许</td><td>✅ 允许</td></tr>
 *   <tr><td>共享锁（S）</td><td>✅ 允许（读-读并发）</td><td>❌ 阻塞</td></tr>
 *   <tr><td>排他锁（X）</td><td>❌ 阻塞</td><td>❌ 阻塞</td></tr>
 * </table>
 * <ul>
 *   <li><b>共享锁（S锁）</b>：用于只读操作。多个事务可同时持有同一行的共享锁，互不阻塞。</li>
 *   <li><b>排他锁（X锁）</b>：用于写操作。一旦某事务持有排他锁，其他任何事务（无论读写）均被阻塞，直到锁释放。</li>
 * </ul>
 *
 * <h2>实现原理</h2>
 * <p>
 * 本类利用 Java 并发包中的 {@link ConcurrentHashMap} 维护一个全局锁表（Lock Table），
 * 键为数据行标识符（{@code recordId}），值为一个 {@link ReentrantReadWriteLock} 对象。
 * 每个数据行拥有独立的读写锁，使得不同行之间的锁操作完全并行，最大化并发度。
 * </p>
 * <p>
 * {@link ReentrantReadWriteLock} 内部实现了读锁共享、写锁独占的语义，并支持<b>可重入性</b>：
 * 同一线程在已持有读锁的情况下可再次申请读锁；已持有写锁的线程可再次申请写锁或降级为读锁。
 * 这种特性简化了事务内部嵌套操作（如存储过程）的锁管理。
 * </p>
 *
 * <h2>典型使用场景</h2>
 * <ol>
 *   <li><b>SELECT 语句</b>：在执行器读取某一行前，事务需调用 {@link #acquireSharedLock(int)} 获取读锁。</li>
 *   <li><b>UPDATE / DELETE 语句</b>：在修改或删除某一行前，事务需调用 {@link #acquireExclusiveLock(int)} 获取写锁。</li>
 *   <li><b>事务提交或回滚</b>：事务结束时，必须释放其持有的所有锁，通常由事务管理器遍历锁集合调用对应的 release 方法。</li>
 * </ol>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>死锁风险</b>：本类仅提供基础的锁获取/释放能力，不包含死锁检测或预防机制。上层事务管理器
 *       需通过锁超时、等待图分析或严格的两阶段锁（2PL）顺序来避免死锁。</li>
 *   <li><b>锁未释放</b>：未能正确释放锁（如异常导致未调用 release）将使该行永久阻塞，造成系统 hang 住。
 *       强烈建议在 finally 块中释放锁，或通过事务管理器统一管理。</li>
 *   <li><b>锁表膨胀</b>：本实现未提供锁对象的自动清理机制。即使某行不再被任何事务持有锁，
 *       对应的 {@code ReentrantReadWriteLock} 对象仍会残留在 {@code lockTable} 中。
 *       在长时间运行的系统中，可能需要定期清理无锁且无等待者的条目以控制内存占用。</li>
 *   <li><b>线程安全</b>：所有公开方法均为线程安全，可在多线程环境下并发调用。</li>
 * </ul>
 *
 * @author Zhan
 * @version 1.0
 * @see ReentrantReadWriteLock
 * @see ConcurrentHashMap
 */
public class LockManager {

    /**
     * 全局锁表（Lock Table）。
     * <p>
     * 存储数据行 ID 到其专属读写锁的映射关系。采用 {@link ConcurrentHashMap} 以确保：
     * <ul>
     *   <li>多个线程可安全地并发访问和修改此表，无需外部同步。</li>
     *   <li>{@link #putIfAbsent(Object, Object)} 操作具有原子性，避免重复创建锁对象。</li>
     * </ul>
     * 键：数据行的唯一标识符（例如堆表元组的物理位置编码：pageId + slotId）。<br>
     * 值：为该行分配的 {@link ReentrantReadWriteLock} 实例，负责实际控制读写并发。
     * </p>
     * <p>
     * <b>惰性初始化</b>：锁对象不会预先创建。当第一次有事务请求对某行加锁时，
     * 才通过 {@code putIfAbsent} 创建并放入表中，减少内存开销。
     * </p>
     */
    private final ConcurrentHashMap<Integer, ReentrantReadWriteLock> lockTable = new ConcurrentHashMap<>();

    /**
     * 申请共享锁（Shared Lock / S锁 / 读锁）。
     * <p>
     * 当事务需要读取某一行数据时（例如执行 {@code SELECT}），应调用此方法获取读锁。
     * 多个事务可同时持有同一行的共享锁，互不阻塞。但若有事务已持有排他锁，则当前线程将被阻塞，
     * 直到排他锁被释放。
     * </p>
     *
     * <h3>执行流程</h3>
     * <ol>
     *   <li>检查锁表中是否已存在该 {@code recordId} 的锁对象。
     *       若不存在，则通过 {@code putIfAbsent} 原子地创建一个新的 {@link ReentrantReadWriteLock}。</li>
     *   <li>获取对应的读写锁实例，调用其 {@code readLock().lock()} 方法。
     *       若当前锁状态允许读访问，则立即返回；否则线程进入阻塞等待队列。</li>
     *   <li>成功获取锁后，打印日志（便于调试和学习）。</li>
     * </ol>
     *
     * <h3>可重入性</h3>
     * <p>
     * 同一线程在已持有该行的共享锁或排他锁的情况下，再次调用本方法仍可成功获取共享锁（锁降级场景）。
     * 但注意：若线程仅持有共享锁，则无法升级为排他锁，必须释放共享锁后重新申请排他锁，否则可能死锁。
     * </p>
     *
     * <h3>阻塞与中断</h3>
     * <p>
     * 本方法使用 {@code lock()} 而非 {@code lockInterruptibly()}，因此对线程中断不响应。
     * 若需支持超时或可中断的锁获取，需在更高层封装或改用其他方法。
     * </p>
     *
     * @param recordId 要加锁的数据行唯一标识符。通常由页面 ID 和槽位号组合而成。
     * @see ReentrantReadWriteLock.ReadLock#lock()
     */
    public void acquireSharedLock(int recordId) {
        // 1. 若锁表中尚无此 recordId 的锁对象，则原子地放入一个新的读写锁
        lockTable.putIfAbsent(recordId, new ReentrantReadWriteLock());

        // 2. 获取读锁（可能阻塞，直到写锁持有者释放）
        lockTable.get(recordId).readLock().lock();

        System.out.println("🔒 [Thread-" + Thread.currentThread().getId() + "] 获取了数据 [" + recordId + "] 的【读锁(S)】");
    }

    /**
     * 申请排他锁（Exclusive Lock / X锁 / 写锁）。
     * <p>
     * 当事务需要修改或删除某一行数据时（例如执行 {@code UPDATE} 或 {@code DELETE}），
     * 应调用此方法获取写锁。排他锁是独占的：一旦某事务持有，其他任何事务（无论读写）均无法获取
     * 该行的任何锁，必须等待排他锁释放。
     * </p>
     *
     * <h3>执行流程</h3>
     * 与 {@link #acquireSharedLock(int)} 类似，区别在于调用 {@code writeLock().lock()}。
     * 若当前锁状态允许独占写访问（即无任何读锁或写锁持有者），则立即获取；否则阻塞。
     *
     * <h3>可重入性</h3>
     * <p>
     * 持有写锁的线程可以再次成功获取写锁（写锁重入），也可以获取读锁（锁降级）。
     * 但持有读锁的线程<b>不能</b>升级为写锁，否则极易导致死锁。
     * </p>
     *
     * @param recordId 要加锁的数据行唯一标识符。
     * @see ReentrantReadWriteLock.WriteLock#lock()
     */
    public void acquireExclusiveLock(int recordId) {
        lockTable.putIfAbsent(recordId, new ReentrantReadWriteLock());

        // 申请写锁（绝对独占，阻塞所有后续读写操作）
        lockTable.get(recordId).writeLock().lock();

        System.out.println("🔐 [Thread-" + Thread.currentThread().getId() + "] 获取了数据 [" + recordId + "] 的【写锁(X)】");
    }

    /**
     * 释放共享锁（Shared Lock / S锁 / 读锁）。
     * <p>
     * 当事务完成对某行数据的读取操作后，应调用此方法释放读锁，以便其他等待写锁的事务能够继续执行。
     * 释放锁后，若该行的读写锁对象内部计数器归零（即没有任何线程持有该锁），则等待队列中的下一个
     * 线程将被唤醒。
     * </p>
     *
     * <h3>配对原则</h3>
     * <p>
     * 每次成功的 {@link #acquireSharedLock(int)} 调用都必须与一次 {@code releaseSharedLock}
     * 严格配对，否则会导致锁泄漏。建议使用 try-finally 模式：
     * <pre>{@code
     * lockManager.acquireSharedLock(recordId);
     * try {
     *     // 读取数据
     * } finally {
     *     lockManager.releaseSharedLock(recordId);
     * }
     * }</pre>
     * </p>
     *
     * <h3>幂等性</h3>
     * <p>
     * 若指定的 {@code recordId} 在锁表中不存在（通常意味着从未被加锁，或锁对象已被 GC），
     * 本方法静默返回，不抛出异常。但在正确使用下，不应出现此情况。
     * </p>
     *
     * @param recordId 要释放锁的数据行唯一标识符。
     * @throws IllegalMonitorStateException 若当前线程并未持有该行的共享锁（由底层读写锁抛出）。
     */
    public void releaseSharedLock(int recordId) {
        ReentrantReadWriteLock lock = lockTable.get(recordId);
        if (lock != null) {
            lock.readLock().unlock();
            System.out.println("🔓 [Thread-" + Thread.currentThread().getId() + "] 释放了数据 [" + recordId + "] 的【读锁(S)】");
        }
    }

    /**
     * 释放排他锁（Exclusive Lock / X锁 / 写锁）。
     * <p>
     * 当事务完成对某行数据的修改并提交（或回滚）后，应调用此方法释放写锁，解除对其他事务的阻塞。
     * 释放原则与 {@link #releaseSharedLock(int)} 相同，必须与获取操作一一配对。
     * </p>
     *
     * @param recordId 要释放锁的数据行唯一标识符。
     * @throws IllegalMonitorStateException 若当前线程并未持有该行的排他锁。
     */
    public void releaseExclusiveLock(int recordId) {
        ReentrantReadWriteLock lock = lockTable.get(recordId);
        if (lock != null) {
            lock.writeLock().unlock();
            System.out.println("🔓 [Thread-" + Thread.currentThread().getId() + "] 释放了数据 [" + recordId + "] 的【写锁(X)】");
        }
    }
}