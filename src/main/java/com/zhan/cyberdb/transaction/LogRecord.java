package com.zhan.cyberdb.transaction;

/**
 * WAL 日志记录（Write-Ahead Log Record）。
 * <p>
 * 代表预写式日志（WAL）中的一条原子操作记录，是数据库崩溃恢复和事务持久性的最小信息单元。
 * 每条日志记录描述了某个事务对数据库所做的<b>一次修改意图</b>，例如“将账户 A 的余额更新为 50”。
 * 在真正的生产级数据库中，日志记录通常包含更丰富的信息，如：
 * <ul>
 *   <li><b>操作类型</b>：INSERT、UPDATE、DELETE、COMMIT、ABORT 等。</li>
 *   <li><b>事务 ID</b>：标识该操作属于哪个事务。</li>
 *   <li><b>修改前的数据（前镜像 / Before Image）</b>：用于事务回滚（Undo）。</li>
 *   <li><b>修改后的数据（后镜像 / After Image）</b>：用于崩溃恢复时的重做（Redo）。</li>
 *   <li><b>页面 ID 和偏移量</b>：精确指明数据在磁盘上的物理位置。</li>
 * </ul>
 * </p>
 *
 * <h2>本类的简化设计</h2>
 * <p>
 * 为了在教学演示中突出 WAL 的核心思想——“先写日志，后写数据”，本类进行了极简化：
 * <ul>
 *   <li>仅包含事务 ID 和一个人类可读的操作描述字符串（如 {@code "UPDATE A = 50"}）。</li>
 *   <li>不包含具体的数据页地址、偏移量、前后镜像等二进制数据。</li>
 *   <li>{@link #toString()} 方法被重写，用于在控制台友好地展示日志内容，方便调试和观察恢复过程。</li>
 * </ul>
 * 在生产环境中，{@code LogRecord} 需要实现序列化/反序列化接口，并能将自身转换为紧凑的二进制格式
 * 以高效写入磁盘日志文件。
 * </p>
 *
 * <h2>在 WAL 流程中的角色</h2>
 * <ol>
 *   <li>事务执行修改操作时，上层组件构造一个 {@code LogRecord} 对象，描述本次修改。</li>
 *   <li>调用 {@link LogManager#appendLogRecord(LogRecord)} 将该记录追加到日志缓冲区（或直接刷盘）。</li>
 *   <li>只有在日志记录安全持久化后，事务才被允许修改缓冲池中的数据页。</li>
 *   <li>若系统在脏页刷盘前崩溃，重启时 {@link LogManager#recover()} 会遍历所有日志记录，
 *       并根据记录内容重做（Redo）操作，恢复数据一致性。</li>
 * </ol>
 *
 * @author Zhan
 * @version 1.0
 * @see LogManager
 */
public class LogRecord {

    /**
     * 事务标识符（Transaction ID）。
     * <p>
     * 每个活跃事务在系统内拥有一个全局唯一的整数 ID。该字段用于将日志记录与特定事务关联，
     * 在崩溃恢复时，恢复管理器需要根据事务 ID 判断哪些操作属于已提交事务（需 Redo），
     * 哪些属于未提交事务（需 Undo）。
     * </p>
     * <p>
     * 在真实系统中，事务 ID 通常由事务管理器（Transaction Manager）在事务开始时分配，
     * 并随日志记录一起持久化，确保恢复时能正确识别事务边界。
     * </p>
     */
    private int txnId;

    /**
     * 操作描述字符串（Human-readable Operation Description）。
     * <p>
     * 用简洁的自然语言描述本日志记录对应的数据库修改动作，例如：
     * <ul>
     *   <li>{@code "UPDATE account SET balance = 50 WHERE id = 1"}</li>
     *   <li>{@code "INSERT INTO users VALUES (1001, 'Alice')"}</li>
     *   <li>{@code "DELETE FROM orders WHERE order_id = 9527"}</li>
     * </ul>
     * 该字段为教学和调试提供了极大的便利，使得日志内容一目了然。
     * </p>
     * <p>
     * 在工业级实现中，此字段会被替换为结构化的数据，例如一个字节表示操作类型码，
     * 后跟变长的键值对或二进制前后镜像数据。文本描述通常不会被写入磁盘以避免空间浪费和解析开销。
     * </p>
     */
    private String operation;

    /**
     * 构造一条日志记录。
     * <p>
     * 通常在事务执行具体的数据修改操作时调用，例如在 {@code BufferPoolManager} 修改页面数据之前。
     * </p>
     *
     * @param txnId     执行该操作的事务 ID，必须为非负整数。
     * @param operation 描述该操作的可读字符串，例如 {@code "UPDATE A = 50"}。
     */
    public LogRecord(int txnId, String operation) {
        this.txnId = txnId;
        this.operation = operation;
    }

    /**
     * 返回日志记录的友好字符串表示，用于控制台输出和日志调试。
     * <p>
     * 格式示例：{@code [Txn-3] 执行了: UPDATE A = 50}
     * </p>
     * <p>
     * 该方法在 {@link LogManager#appendLogRecord(LogRecord)} 和
     * {@link LogManager#recover()} 中被调用，用于模拟日志落盘和恢复重做时的可视化输出。
     * </p>
     *
     * @return 格式化的日志记录字符串。
     */
    @Override
    public String toString() {
        return "[Txn-" + txnId + "] 执行了: " + operation;
    }
}