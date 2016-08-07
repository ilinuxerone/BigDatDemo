package com.big.data.plan;

import java.net.InetSocketAddress;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 订阅mysql的binlog,监控数据库的变化
 */
public class ClientSample {

    public static void main(String args[]) {
        // 创建链接  
        CanalConnector connector = CanalConnectors.newSingleConnector(
            new InetSocketAddress(AddressUtils.getHostIp(), 11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmtryCount = 1200;
            while (emptyCount < totalEmtryCount) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    emptyCount = 0;
                    printEntry(message.getEntries());
                }

                // 提交确认
                connector.ack(batchId);
                // 处理失败, 回滚数据 
                // connector.rollback(batchId);  
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    /**
     * 处理变更
     * @param entrys
     */
    private static void printEntry(@NotNull List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(
                    "ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format(
                "================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(@NotNull List<Column> columns) {
        for (Column column : columns) {
            System.out.println(
                column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}