package com.wzfq.util;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBase {
    public static HTable creatTable(String tableName, String family){
        Configuration configuration = HBaseConfiguration.create();
        HTable table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            HTableDescriptor hTable = new HTableDescriptor(TableName.valueOf(tableName));
            hTable.addFamily(new HColumnDescriptor(family).setCompressionType(Compression.Algorithm.NONE));
            if (admin.tableExists(hTable.getTableName())){
                admin.disableTable(hTable.getTableName());
                admin.deleteTable(hTable.getTableName());
            }
            admin.createTable(hTable);
            table = new HTable(configuration, Bytes.toBytes(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }
}
