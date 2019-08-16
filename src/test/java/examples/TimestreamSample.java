package examples;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.timestream.*;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.model.filter.Attribute;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;
import com.alicloud.openservices.tablestore.timestream.model.filter.Name;
import com.alicloud.openservices.tablestore.timestream.model.query.Sorter;
import com.alicloud.openservices.tablestore.writer.WriterConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alicloud.openservices.tablestore.timestream.model.filter.FilterFactory.*;
/**
 * Created by yanglian on 2019/4/11.
 */
public class TimestreamSample {
    private static String metaTableName = "index_table";
    private static String dataTableName = "data_table";

    public static void main(String[] args) throws Exception {
        // 1. 初始化TimestreamClient。
        final String endPoint = "";
        final String accessKeyId = "";
        final String accessKeySecret = "";
        final String instanceName = "";
        AsyncClient asyncClient = new AsyncClient(endPoint, accessKeyId, accessKeySecret, instanceName);
        TimestreamDBConfiguration conf = new TimestreamDBConfiguration(metaTableName);
        boolean withCallback = false;
        TimestreamDB db;
        if (!withCallback) {
            db = new TimestreamDBClient(asyncClient, conf);
        } else {
            db = getClientWithCallback(asyncClient, conf);
        }
        try {
            // 创建meta表和数据表
            createTable(db);

            // 写数据
            writeData(db);

            // 写meta
            writeMeta(db);

            // 查数据
            queryData(db);

            // 根据条件检索meta
            queryMeta(db);

            // 精确查询meta
            getMeta(db);

            // 删除meta表和数据表
            deleteTable(db);
        } finally {
            db.close();
        }
    }

    // 自定义TableStoreWriter callback实现
    static class DefaultCallback implements TableStoreCallback<RowChange, ConsumedCapacity> {
        private AtomicLong succeedCount = new AtomicLong();
        private AtomicLong failedCount = new AtomicLong();

        public void onCompleted(final RowChange req, final ConsumedCapacity res) {
            // 成功的业务逻辑
            succeedCount.incrementAndGet();
        }

        public void onFailed(final RowChange req, final Exception ex) {
            // 失败的业务逻辑
            System.out.println("Got error:" + ex.getMessage());
            failedCount.incrementAndGet();
        }
    }

    public static TimestreamDB getClientWithCallback(AsyncClient asyncClient, TimestreamDBConfiguration config) {
        DefaultCallback callback = new DefaultCallback();

        //TableStoreWriter配置
        WriterConfig writerConfig = new WriterConfig();

        // 设置writer的buffer为4096行
        writerConfig.setBufferSize(4096);

        TimestreamDBClient db = new TimestreamDBClient(asyncClient, config, new WriterConfig(), callback);
        return db;
    }

    public static void createTable(TimestreamDB db) {
        List<AttributeIndexSchema> attributeIndexs = new ArrayList<AttributeIndexSchema>();
        attributeIndexs.add(new AttributeIndexSchema("sendUser", AttributeIndexSchema.Type.KEYWORD));
        attributeIndexs.add(new AttributeIndexSchema("sendPhone", AttributeIndexSchema.Type.KEYWORD));
        attributeIndexs.add(new AttributeIndexSchema("sourceCity", AttributeIndexSchema.Type.KEYWORD));
        attributeIndexs.add(new AttributeIndexSchema("recvUser", AttributeIndexSchema.Type.KEYWORD));
        attributeIndexs.add(new AttributeIndexSchema("recvPhone", AttributeIndexSchema.Type.KEYWORD));
        attributeIndexs.add(new AttributeIndexSchema("targetCity", AttributeIndexSchema.Type.KEYWORD));
        attributeIndexs.add(new AttributeIndexSchema("latestPos", AttributeIndexSchema.Type.GEO_POINT));
        attributeIndexs.add(new AttributeIndexSchema("status", AttributeIndexSchema.Type.LONG));
        db.createMetaTable(attributeIndexs);
        db.createDataTable(dataTableName);
    }

    public static void deleteTable(TimestreamDB db) {
        db.deleteDataTable(dataTableName);
        db.deleteMetaTable();
    }

    public static void writeMeta(TimestreamDB db) throws InterruptedException {
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("中通") // 物流平台
                .addTag("id", "77110067935100") // 订单号
                .build();
        TimestreamMeta meta = new TimestreamMeta(identifier)
                .addAttribute("sendUser", "yan1")           //寄件人名字
                .addAttribute("sendPhone", "13058880000")   //寄件人手机号
                .addAttribute("sourceCity", "hangzhou")     //寄件城市
                .addAttribute("sendAddr", "杭州市云栖小镇飞天园区")//寄件地址
                .addAttribute("recvUser", "li2")            //收件人名字
                .addAttribute("recvPhone", "18056117263")   //收件人手机号
                .addAttribute("targetCity", "shanghai")     //收件城市
                .addAttribute("recvAddr", "上海市阿里中心")   //收件地址
                .addAttribute("latestPos", new Location(30.1319243712, 120.0881250209))   //收件地址
                .addAttribute("status", 0);               // 订单状态：未发出

        TimestreamMetaTable metaTable = db.metaTable();
        metaTable.put(meta);

        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
    }

    public static void getMeta(TimestreamDB db) {
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("中通") // 物流平台
                .addTag("id", "77110067935100") // 订单号
                .build();

        TimestreamMetaTable metaTable = db.metaTable();
        TimestreamMeta meta = metaTable.get(identifier).returnAll().fetch();
        System.out.print(meta);
    }

    public static void queryMeta(TimestreamDB db) {
        Filter filter = and(
                Name.equal("中通"),
                Attribute.equal("sendPhone", "13058880000"),
                Attribute.inGeoDistance("latestPos", "30.1319243712,120.0881250209", 10.0),
                Attribute.equal("status", 0)
        );

        {   // 只查询identifier信息
            TimestreamMetaTable metaTable = db.metaTable();
            TimestreamMetaIterator iter1 = metaTable.filter(filter)
                    .fetchAll();    //获取所有订单
            System.out.print(iter1.getTotalCount()); // 打印命中的时间线数量
            while (iter1.hasNext()) {
                TimestreamMeta point = iter1.next();
                System.out.print(point.toString());
            }
        }
        {   // 查询完整的meta信息
            TimestreamMetaTable metaTable = db.metaTable();
            TimestreamMetaIterator iter1 = metaTable.filter(filter)
                    .returnAll()    // 查询完整的meta信息
                    .limit(10)      // 设置每次查询多元索引的limit限制
                    .fetchAll();    //获取所有订单
            System.out.print(iter1.getTotalCount()); // 打印命中的时间线数量
            while (iter1.hasNext()) {
                TimestreamMeta point = iter1.next();
                System.out.print(point.toString());
            }
        }
        {   // 查询部分attributes
            TimestreamMetaTable metaTable = db.metaTable();
            TimestreamMetaIterator iter1 = metaTable.filter(filter)
                    .selectAttributes("status", "recvAddr")
                    .fetchAll();    //获取所有订单
            System.out.print(iter1.getTotalCount()); // 打印命中的时间线数量
            while (iter1.hasNext()) {
                TimestreamMeta point = iter1.next();
                System.out.print(point.toString());
            }
        }
        {   // 查询完整的meta信息，并根据attributes进行排序
            TimestreamMetaTable metaTable = db.metaTable();
            TimestreamMetaIterator iter1 = metaTable.filter(filter)
                    .returnAll()    // 查询完整的meta信息
                    .sort(
                            Sorter.Builder.newBuilder()
                                    .sortByAttributes("sendPhone", Sorter.SortOrder.ASC)
                                    .build())
                    .limit(10)      // 设置每次查询多元索引的limit限制
                    .fetchAll();    //获取所有订单
            System.out.print(iter1.getTotalCount()); // 打印命中的时间线数量
            while (iter1.hasNext()) {
                TimestreamMeta point = iter1.next();
                System.out.print(point.toString());
            }
        }
    }

    public static void writeData(TimestreamDB db) {
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("中通") // 物流平台
                .addTag("id", "77110067935100") // 订单号
                .build();
        Point point = new Point.Builder(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("loc", "30.130370,120.083263")   //包裹位置
                .build();

        TimestreamDataTable dataTable = db.dataTable(dataTableName);

        // 异步写入数据
        dataTable.asyncWrite(identifier, point);    //记录包裹到达某个地点

        // 同步写入数据
        try {
            dataTable.write(identifier, point);
        } catch (TableStoreException e) {
            // 异常处理
        }
    }

    public static void queryData(TimestreamDB db) {
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("中通") // 物流平台
                .addTag("id", "77110067935100") // 订单号
                .build();
        TimestreamDataTable dataTable = db.dataTable(dataTableName);

        //查询[now - 60 * 1000, now)范围内的数据，只查询"loc"这一列field
        long now = System.currentTimeMillis();
        Iterator<Point> iter1 = dataTable.get(identifier)
                .select("loc")    // 查询loc这一列
                .timeRange(TimeRange.range(now - 60 * 1000, now, TimeUnit.MILLISECONDS))
                .fetchAll();

        // 查询时间戳为now的数据，查询所有fields
        Iterator<Point> iter2 = dataTable.get(identifier)
                .timestamp(now, TimeUnit.MILLISECONDS)
                .fetchAll();

        // 对fields进行条件过滤
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                "loc",
                SingleColumnValueFilter.CompareOperator.GREATER_EQUAL,
                new ColumnValue("123", ColumnType.STRING));
        Iterator<Point> iter3 = dataTable.get(identifier)
                .timestamp(now, TimeUnit.MILLISECONDS)
                .filter(filter)
                .fetchAll();

        // 按照时间逆序查询数据，每次查询最大返回1条记录
        Iterator<Point> iter4 = dataTable.get(identifier)
                .timeRange(TimeRange.range(now - 60 * 1000, now, TimeUnit.MILLISECONDS))
                .descTimestamp()
                .limit(1)
                .fetchAll();
    }
}
