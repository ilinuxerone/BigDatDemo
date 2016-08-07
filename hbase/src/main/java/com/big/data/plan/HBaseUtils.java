package com.big.data.plan;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase spring
 */
public class HBaseUtils {
    private static Configuration config = null;
    private static HTablePool tp = null;

    static {

        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "xx.xx.xx");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        tp = new HTablePool(config, 10);
    }

    /**
     * get table
     */
    public static HTableInterface getTable(String tableName) {
        if (StringUtils.isEmpty(tableName))
            return null;
        return tp.getTable(getBytes(tableName));
    }

    /**
     * string to byte
     * @param str
     * @return
     */
    public static byte[] getBytes(String str) {
        if (str == null)
            str = "";
        return Bytes.toBytes(str);
    }

    public static TBData getDataMap(String tableName, String startRow, String stopRow,
                                    Integer currentPage, Integer pageSize) throws IOException {
        List<Map<String, String>> mapList = null;
        mapList = new LinkedList<Map<String, String>>();
        ResultScanner scanner = null;
        TBData tbData = null;
        try {
            if (pageSize == null || pageSize == 0L)
                pageSize = 100;
            if (currentPage == null || currentPage == 0)
                currentPage = 1;

            Integer firstPage = (currentPage - 1) * pageSize;

            Integer endPage = firstPage + pageSize;
            HTableInterface table = getTable(tableName);
            Scan scan = getScan(startRow, stopRow);
            scan.setFilter(packageFilters(true));
            scan.setCaching(1000);
            scan.setCacheBlocks(false);
            scanner = table.getScanner(scan);
            int i = 0;
            List<byte[]> rowList = new LinkedList<byte[]>();
            for (Result result : scanner) {
                String row = toStr(result.getRow());
                if (i >= firstPage && i < endPage) {
                    rowList.add(getBytes(row));
                }
                i++;
            }
            List<Get> getList = getList(rowList);
            Result[] results = table.get(getList);
            for (Result result : results) {
                Map<byte[], byte[]> fmap = packFamilyMap(result);
                Map<String, String> rmap = packRowMap(fmap);
                mapList.add(rmap);
            }
            tbData = new TBData();
            tbData.setCurrentPage(currentPage);
            tbData.setPageSize(pageSize);
            tbData.setTotalCount(i);
            tbData.setTotalPage(getTotalPage(pageSize, i));
            tbData.setResultList(mapList);
        } catch (IOException e) {
            e.printStackTrace();

        } finally {

            closeScanner(scanner);
        }
        return tbData;
    }

    private static String toStr(byte[] row) {
        return new String(row);
    }

    private static int getTotalPage(int pageSize, int totalCount) {
        int n = totalCount / pageSize;
        if (totalCount % pageSize == 0) {
            return n;
        } else {
            return ((int) n) + 1;
        }
    }

    private static Scan getScan(String startRow, String stopRow) {
        Scan scan = new Scan();
        scan.setStartRow(getBytes(startRow));
        scan.setStopRow(getBytes(stopRow));
        return scan;
    }

    private static FilterList packageFilters(boolean isPage) {
        FilterList filterList = null;
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter1 = null;
        Filter filter2 = null;
        filter1 = newFilter(getBytes("family1"), getBytes("column1"), CompareOp.EQUAL,
            getBytes("condition1"));
        filter2 = newFilter(getBytes("family2"), getBytes("column1"), CompareOp.LESS,
            getBytes("condition2"));
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        if (isPage) {
            filterList.addFilter(new FirstKeyOnlyFilter());
        }
        return filterList;
    }

    public static Map<byte[], byte[]> packFamilyMap(Result result) {
        Map<byte[], byte[]> dataMap = null;
        dataMap = new LinkedHashMap<byte[], byte[]>();
        dataMap.putAll(result.getFamilyMap(getBytes("family1")));
        dataMap.putAll(result.getFamilyMap(getBytes("family2")));
        return dataMap;
    }

    private static Filter newFilter(byte[] f, byte[] c, CompareOp op, byte[] v) {
        return new SingleColumnValueFilter(f, c, op, v);
    }

    private static void closeScanner(ResultScanner scanner) {
        if (scanner != null)
            scanner.close();
    }

    private static Map<String, String> packRowMap(Map<byte[], byte[]> dataMap) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (byte[] key : dataMap.keySet()) {
            byte[] value = dataMap.get(key);
            map.put(toStr(key), toStr(value));
        }
        return map;
    }

    private static List<Get> getList(List<byte[]> rowList) {
        List<Get> list = new LinkedList<Get>();
        for (byte[] row : rowList) {
            Get get = new Get(row);
            get.addColumn(getBytes("family1"), getBytes("column1"));
            get.addColumn(getBytes("family1"), getBytes("column2"));
            get.addColumn(getBytes("family2"), getBytes("column1"));
            list.add(get);
        }
        return list;
    }
}
