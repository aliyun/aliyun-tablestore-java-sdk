package com.alicloud.openservices.tablestore.reader;

import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * 并发场景测试类，用于验证 ReaderGroup 在多线程环境下的正确性
 * 
 * 测试重点：
 * 1. 多线程同时调用 finishOneRow() 时，索引分配的原子性和唯一性
 * 2. 验证所有 resultList 位置都被正确填充，没有 null 元素
 * 3. 验证 completeGroup() 在所有数据填充完成后才被调用
 * 4. 验证并发场景下不会出现索引重复、越界或丢失数据
 */
public class ReaderGroupConcurrentTest {

    /**
     * 测试场景1：高并发下的索引唯一性
     * 验证多个线程同时完成行操作时，每个索引只被使用一次
     */
    @Test
    public void testConcurrentIndexUniqueness() throws Exception {
        int threadCount = 100;
        int groupSize = 100;
        
        ReaderGroup group = new ReaderGroup(groupSize);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(groupSize);
        
        // 用于记录实际使用的索引
        Set<Integer> usedIndices = Collections.synchronizedSet(new HashSet<>());
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        try {
            // 创建 groupSize 个任务，每个任务调用 succeedOneRow
            for (int i = 0; i < groupSize; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        // 等待所有线程就绪后同时开始
                        startLatch.await();
                        
                        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(index))
                                .build();
                        
                        Row row = new Row(pk, new Column[0]);
                        BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(
                                "testTable", row, new ConsumedCapacity(new CapacityUnit(1, 0)), index);
                        
                        group.succeedOneRow(pk, rowResult);
                        doneLatch.countDown();
                    } catch (Exception e) {
                        exceptions.add(e);
                        doneLatch.countDown();
                    }
                });
            }
            
            // 所有线程同时开始执行
            startLatch.countDown();
            
            // 等待所有任务完成
            assertTrue("所有任务应在10秒内完成", doneLatch.await(10, TimeUnit.SECONDS));
            
            // 验证没有异常
            if (!exceptions.isEmpty()) {
                fail("并发执行过程中出现异常: " + exceptions.get(0).getMessage());
            }
            
            // 获取结果
            ReaderResult result = group.getFuture().get(1, TimeUnit.SECONDS);
            
            // 验证结果完整性
            assertNotNull("ReaderResult 不应为 null", result);
            assertEquals("总数应该匹配", groupSize, result.getTotalCount());
            assertEquals("所有行都应该成功", groupSize, result.getSucceedRows().size());
            assertEquals("不应该有失败的行", 0, result.getFailedRows().size());
            
            // 验证没有 null 元素
            for (RowReadResult rowResult : result.getSucceedRows()) {
                assertNotNull("结果列表中不应有 null 元素", rowResult);
                assertTrue("所有行都应标记为成功", rowResult.isSucceed());
            }
            
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
    
    /**
     * 测试场景2：混合成功和失败的并发场景
     * 验证在成功和失败混合的情况下，所有位置都被正确填充
     */
    @Test
    public void testConcurrentMixedSuccessAndFailure() throws Exception {
        int groupSize = 200;
        ReaderGroup group = new ReaderGroup(groupSize);
        ExecutorService executor = Executors.newFixedThreadPool(50);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(groupSize);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        try {
            for (int i = 0; i < groupSize; i++) {
                final int index = i;
                final boolean shouldSucceed = (i % 3 != 0); // 2/3 成功，1/3 失败
                
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        
                        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(index))
                                .build();
                        
                        if (shouldSucceed) {
                            Row row = new Row(pk, new Column[0]);
                            BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(
                                    "testTable", row, new ConsumedCapacity(new CapacityUnit(1, 0)), index);
                            group.succeedOneRow(pk, rowResult);
                            successCount.incrementAndGet();
                        } else {
                            BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(
                                    "testTable", new Error("Mock failure for index " + index, "MOCK_ERROR"), index);
                            group.failedOneRow(pk, rowResult, new Exception("Mock failure for index " + index));
                            failureCount.incrementAndGet();
                        }
                        
                        doneLatch.countDown();
                    } catch (Exception e) {
                        doneLatch.countDown();
                        e.printStackTrace();
                    }
                });
            }
            
            startLatch.countDown();
            assertTrue("所有任务应在10秒内完成", doneLatch.await(10, TimeUnit.SECONDS));
            
            // 获取结果
            ReaderResult result = group.getFuture().get(2, TimeUnit.SECONDS);
            
            // 验证结果完整性
            assertNotNull("ReaderResult 不应为 null", result);
            assertEquals("总数应该匹配", groupSize, result.getTotalCount());
            
            // 验证成功和失败的数量
            int actualSuccessCount = result.getSucceedRows().size();
            int actualFailureCount = result.getFailedRows().size();
            
            assertEquals("成功数量应该匹配", successCount.get(), actualSuccessCount);
            assertEquals("失败数量应该匹配", failureCount.get(), actualFailureCount);
            assertEquals("成功+失败应该等于总数", groupSize, actualSuccessCount + actualFailureCount);
            
            // 验证没有 null 元素
            for (RowReadResult rowResult : result.getSucceedRows()) {
                assertNotNull("成功列表中不应有 null 元素", rowResult);
                assertTrue("成功列表中的行应标记为成功", rowResult.isSucceed());
            }
            
            for (RowReadResult rowResult : result.getFailedRows()) {
                assertNotNull("失败列表中不应有 null 元素", rowResult);
                assertFalse("失败列表中的行应标记为失败", rowResult.isSucceed());
            }
            
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
    
    /**
     * 测试场景3：极端并发压力测试
     * 使用大量线程同时操作，验证系统稳定性
     */
    @Test
    public void testHighConcurrencyStressTest() throws Exception {
        int groupSize = 1000;
        ReaderGroup group = new ReaderGroup(groupSize);
        ExecutorService executor = Executors.newFixedThreadPool(200);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(groupSize);
        
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        try {
            for (int i = 0; i < groupSize; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        
                        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(index))
                                .build();
                        
                        Row row = new Row(pk, new Column[0]);
                        BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(
                                "testTable", row, new ConsumedCapacity(new CapacityUnit(1, 0)), index);
                        
                        group.succeedOneRow(pk, rowResult);
                        doneLatch.countDown();
                    } catch (Exception e) {
                        exceptions.add(e);
                        doneLatch.countDown();
                    }
                });
            }
            
            startLatch.countDown();
            assertTrue("所有任务应在15秒内完成", doneLatch.await(15, TimeUnit.SECONDS));
            
            // 验证没有异常
            if (!exceptions.isEmpty()) {
                fail("高并发场景下出现异常: " + exceptions.get(0).getMessage());
            }
            
            // 获取结果
            ReaderResult result = group.getFuture().get(3, TimeUnit.SECONDS);
            
            // 验证结果
            assertNotNull("ReaderResult 不应为 null", result);
            assertEquals("总数应该匹配", groupSize, result.getTotalCount());
            assertEquals("所有行都应该成功", groupSize, result.getSucceedRows().size());
            assertTrue("应该全部成功", result.isAllSucceed());
            
            // 验证没有 null 元素
            for (RowReadResult rowResult : result.getSucceedRows()) {
                assertNotNull("结果列表中不应有 null 元素", rowResult);
            }
            
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
    
    /**
     * 测试场景4：顺序完成 vs 并发完成的对比
     * 验证并发场景下的结果与顺序场景一致
     */
    @Test
    public void testSequentialVsConcurrentComparison() throws Exception {
        int groupSize = 100;
        
        // 顺序执行
        ReaderGroup sequentialGroup = new ReaderGroup(groupSize);
        for (int i = 0; i < groupSize; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i))
                    .build();
            Row row = new Row(pk, new Column[0]);
            BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(
                    "testTable", row, new ConsumedCapacity(new CapacityUnit(1, 0)), i);
            sequentialGroup.succeedOneRow(pk, rowResult);
        }
        ReaderResult sequentialResult = sequentialGroup.getFuture().get(1, TimeUnit.SECONDS);
        
        // 并发执行
        ReaderGroup concurrentGroup = new ReaderGroup(groupSize);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(groupSize);
        
        try {
            for (int i = 0; i < groupSize; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(index))
                                .build();
                        Row row = new Row(pk, new Column[0]);
                        BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(
                                "testTable", row, new ConsumedCapacity(new CapacityUnit(1, 0)), index);
                        concurrentGroup.succeedOneRow(pk, rowResult);
                        doneLatch.countDown();
                    } catch (Exception e) {
                        doneLatch.countDown();
                        e.printStackTrace();
                    }
                });
            }
            
            startLatch.countDown();
            assertTrue("所有任务应在10秒内完成", doneLatch.await(10, TimeUnit.SECONDS));
            
            ReaderResult concurrentResult = concurrentGroup.getFuture().get(1, TimeUnit.SECONDS);
            
            // 对比两种方式的结果
            assertEquals("总数应该一致", sequentialResult.getTotalCount(), concurrentResult.getTotalCount());
            assertEquals("成功数量应该一致", sequentialResult.getSucceedRows().size(), concurrentResult.getSucceedRows().size());
            assertEquals("失败数量应该一致", sequentialResult.getFailedRows().size(), concurrentResult.getFailedRows().size());
            assertEquals("完成状态应该一致", sequentialResult.isAllFinished(), concurrentResult.isAllFinished());
            assertEquals("成功状态应该一致", sequentialResult.isAllSucceed(), concurrentResult.isAllSucceed());
            
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
