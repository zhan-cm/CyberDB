package com.zhan.cyberdb.execution;

import com.zhan.cyberdb.storage.Page;
import com.zhan.cyberdb.storage.TablePage;
import com.zhan.cyberdb.storage.Tuple;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;

public class ExecutionTest {

    @Test
    public void testVolcanoModelPipeline() {
        System.out.println("========== 🌋 火山模型执行引擎点火！ ==========");

        // ==========================================
        // 阶段 1：准备底层数据 (相当于模拟磁盘里的一页数据)
        // ==========================================
        Page rawPage = new Page();
        TablePage tablePage = new TablePage(rawPage);
        tablePage.init(-1, -1);

        // 插入 5 条测试数据
        tablePage.insertTuple(new Tuple("Hello Java".getBytes(StandardCharsets.UTF_8)));
        tablePage.insertTuple(new Tuple("CyberDB is awesome!".getBytes(StandardCharsets.UTF_8))); // 这条要查出来
        tablePage.insertTuple(new Tuple("Just some random text".getBytes(StandardCharsets.UTF_8)));
        tablePage.insertTuple(new Tuple("I love Cyberpunk 2077".getBytes(StandardCharsets.UTF_8))); // 这条也要查出来
        tablePage.insertTuple(new Tuple("Spring Boot and MyBatis".getBytes(StandardCharsets.UTF_8)));

        System.out.println("底层 TablePage 数据准备完毕，共 5 条数据。\n");

        // ==========================================
        // 阶段 2：拼装执行器流水线 (Query Plan / 执行计划)
        // ==========================================
        
        // 1. 最底层的扫描工人：负责从 TablePage 抠数据
        Executor seqScan = new SeqScanExecutor(tablePage);
        
        // 2. 过滤条件 (Predicate)：相当于 WHERE content LIKE '%Cyber%'
        Predicate<Tuple> condition = tuple -> {
            String content = new String(tuple.getData(), StandardCharsets.UTF_8);
            return content.contains("Cyber");
        };
        
        // 3. 上层的过滤工人：把底层的 seqScan 当作自己的下级
        Executor filter = new FilterExecutor(seqScan, condition);

        // ==========================================
        // 阶段 3：启动流水线拉取数据！
        // ==========================================
        
        System.out.println(">>> 开始执行查询：SELECT * WHERE content CONTAINS 'Cyber'");
        
        // 老板发话：全体工人就位，初始化！(这一步会顺着树形结构一路叫醒到最底层)
        filter.init();

        // 开始疯狂向最顶层的算子要数据
        Tuple resultTuple;
        int matchCount = 0;
        
        while ((resultTuple = filter.next()) != null) {
            matchCount++;
            String resultStr = new String(resultTuple.getData(), StandardCharsets.UTF_8);
            System.out.println("✅ 成功拉取到合格数据: [" + resultStr + "]");
        }

        System.out.println("\n查询结束！共找到 " + matchCount + " 条符合条件的数据。");
        System.out.println("========== 完美收工！ ==========");
    }
}