package com.big.data.plan;

/**
 * hbase 操作实体
 */
public class Data {

    /**
     * 列簇
     */
    private String columnFamily;
    /**
     * 列
     */
    private String column;
    /**
     * 列植
     */
    private String value;

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
