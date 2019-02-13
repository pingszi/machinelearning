package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;

/**
 *********************************************************
 ** @desc  ： hbase数据处理
 ** @author  Pings
 ** @date    2017/11/28
 ** @version v1.0
 * *******************************************************
 */
public class HbaseDao {

    private static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        //**设置Zookeeper,直接设置IP地址
        conf.set("hbase.zookeeper.quorum", "192.168.1.20");
    }

    /**
     *********************************************************
     ** @desc ： 按rowkey查找行数据
     ** @author Pings
     ** @date   2017/11/29
     ** @param  tableName 表名
     ** @param  rowKey
     ** @param  familyName 列族
     ** @param  columnName 列名
     * *******************************************************
     */
    public static Map<String, String> get(String tableName, String rowKey, String familyName, String columnName) {
        return connect(tableName, (conn, table) -> {
            Get get = new Get(Bytes.toBytes(rowKey));

            if (familyName != null && !"".equals(familyName.trim()))
                get.addFamily(Bytes.toBytes(familyName));
            if (columnName != null && !"".equals(columnName.trim()))
                get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));

            try {
                Result result = table.get(get);
                List<Cell> cells = result.listCells();
                return cells == null ? null : cells.stream().map(HbaseDao::cellToHbaseRst).collect(toMap(HbaseRst::getKey, HbaseRst::getValue));
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        });

    }

    /**
     *********************************************************
     ** @desc ： 范围查询数据
     ** @author Pings
     ** @date   2017/11/29
     ** @param  tableName 表名
     ** @param  beginRowKey 开始rowKey
     ** @param  endRowKey   结束rowKey
     * *******************************************************
     */
    public static List<Map<String, String>> scan(String tableName, String beginRowKey, String endRowKey) {
        return scan(tableName, null, null, scan -> {
            scan.setStartRow(Bytes.toBytes(beginRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
        });
    }

    /**
     *********************************************************
     ** @desc ： 模糊查询
     ** @author Pings
     ** @date   2017/11/29
     ** @param  tableName 表名
     ** @param  key       关键字
     ** @param  familyName 列族
     ** @param  columnName 列名
     * *******************************************************
     */
    public static List<Map<String, String>> scan(String tableName, String key, String familyName, String columnName) {
        return scan(tableName, null, null, scan -> {
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL, new SubstringComparator(key));
            scan.setFilter(filter);
        });
    }

    /**
     *********************************************************
     ** @desc ： 添加/更新数据
     ** @author Pings
     ** @date   2017/11/29
     ** @param tableName 表名
     ** @param rowKey
     ** @param familyName 列族
     ** @param columnName 列名
     ** @param value
     * *******************************************************
     */
    public static void put(String tableName, String rowKey, String familyName, String columnName, String value) {
        connect(tableName, (conn, table) -> {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
            try {
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        });
    }

    /**
     *********************************************************
     ** @desc ： 创建表
     ** @author Pings
     ** @date   2017/11/29
     ** @param  tableName   表名
     ** @param  familyNames 列族
     * *******************************************************
     */
    public static void create(String tableName, List<String> familyNames) {
        try(Connection conn = ConnectionFactory.createConnection(conf)) {

            Admin admin = conn.getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            familyNames.stream().forEach(familyName -> table.addFamily(new HColumnDescriptor(familyName)));
            admin.createTable(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *********************************************************
     ** @desc ： 获取连接
     ** @author Pings
     ** @date   2017/11/29
     ** @param  tableName   表名
     ** @param  func        处理函数
     * *******************************************************
     */
    private static <R> R connect(String tableName, BiFunction<Connection, Table, R> func) {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table table = conn.getTable(TableName.valueOf(tableName))) {
            return func.apply(conn, table);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *********************************************************
     ** @desc ： 把cell转换成map
     ** @author Pings
     ** @date   2017/11/29
     ** @param  cell   hbase单元格
     * *******************************************************
     */
    private static HbaseRst cellToHbaseRst(Cell cell) {
        String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
        String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

        return new HbaseRst(family + ":" + qualifier, value);
    }

    /**
     *********************************************************
     ** @desc ： 扫描表
     ** @author Pings
     ** @date   2017/11/30
     ** @param tableName    表名
     ** @param familyName   列族
     ** @param columnName   列名
     ** @param consumer     扫描条件
     * *******************************************************
     */
    private static List<Map<String, String>> scan(String tableName, String familyName, String columnName, Consumer<Scan> consumer) {
        Scan scan = new Scan();
        if (familyName != null && !"".equals(familyName.trim()))
            scan.addFamily(Bytes.toBytes(familyName));
        if (columnName != null && !"".equals(columnName.trim()))
            scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));

        consumer.accept(scan);

        return connect(tableName, (connect, table) -> {
            try {
                ResultScanner result = table.getScanner(scan);
                List<Map<String, String>> rst = new ArrayList<>();
                Stream<Result> cells = StreamSupport.stream(result.spliterator(), false);
                cells.forEach(cell -> rst.add(cell.listCells().stream().map(HbaseDao::cellToHbaseRst).collect(toMap(HbaseRst::getKey, HbaseRst::getValue))));
                return rst;
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        });
    }

    /**结果*/
    static class HbaseRst {
        String key;
        String value;

        public HbaseRst(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }
}