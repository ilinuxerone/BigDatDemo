package com.big.data.plan;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase api test
 */
public class Hbase {
    private Configuration conf = null;

    /**
     * 配置
     * @throws Exception
     */
    public Hbase() throws Exception {
        conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        conf.set("hbase.zookeeper.quorum", "Master.Hadoop,Salve.Hadoop");
        conf.set("hbase.zookeeper.master", "Master.Hadoop");
    }

    /**
     * 创建表
     * 
     * @param tableName
     * @param columnFamilys
     * @throws Exception
     */
    public void createTable(String tableName, List<String> columnFamilys) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

        for (String columnFamily : columnFamilys) {
            desc.addFamily(new HColumnDescriptor(columnFamily));
        }

        admin.createTable(desc);
        admin.close();
    }

    /**
     * 插入数据
     * @param tableName
     * @param rowKey
     * @param datas
     * @throws Exception
     */
    public void put(String tableName, String rowKey, List<Data> datas) throws Exception {
        HTable table = new HTable(conf, tableName);
        Put p = new Put(Bytes.toBytes(rowKey));

        for (Data data : datas) {
            p.add(Bytes.toBytes(data.getColumnFamily()), Bytes.toBytes(data.getColumn()),
                Bytes.toBytes(data.getValue()));
        }

        table.put(p);
    }

    /**
     * get
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public void get(String tableName, String rowKey) throws Exception {
        HTable table = new HTable(conf, tableName);
        Get g = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(g);

        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
        }
    }

    /**
     * scan
     * @param tableName
     * @throws Exception
     */
    public void scan(String tableName, String family, String qualifier) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan s = new Scan();

        if (family != null && !family.equals("") && qualifier != null && !qualifier.equals("")) {

            s.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        } else if (family != null && !family.equals("")) {

            s.addFamily(Bytes.toBytes(family));
        }

        ResultScanner scanner = table.getScanner(s);
        try {
            for (Result result : scanner) {
                for (KeyValue kv : result.list()) {
                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                    System.out.println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("Timestamp:" + kv.getTimestamp());
                }
            }
        } finally {
            scanner.close();
        }
    }

    /**
     * del
     * @param tableName
     * @throws Exception
     */
    public void del(String tableName, String rowKey, String family,
                    String qualifier) throws Exception {
        HTable table = new HTable(conf, tableName);
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));

        if (family != null && !family.equals("") && qualifier != null && !qualifier.equals("")) {

            deleteColumn.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        } else if (family != null && !family.equals("")) {

            deleteColumn.deleteFamily(Bytes.toBytes(family));
        }

        table.delete(deleteColumn);
    }

    /**
     * del table
     * @throws Exception
     */
    public void delTable(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
    }

    public static void main(String[] args) throws Exception {
        Hbase hbase = new Hbase();
        hbase.scan("student", null, null);

        List<String> fy = new ArrayList<String>();
        fy.add("hh");
        hbase.createTable("tt", fy);
    }
}
