package com.wzfq.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

public class HBase {
    public static void creatTable(String tableName, String family){
        try {
            Connection connection = getConnection();
            Admin admin = connection.getAdmin();
            HTableDescriptor hTable = new HTableDescriptor(TableName.valueOf(tableName));
            hTable.addFamily(new HColumnDescriptor(family).setCompressionType(Compression.Algorithm.NONE));
            if (admin.tableExists(hTable.getTableName())){
                admin.disableTable(hTable.getTableName());
                admin.deleteTable(hTable.getTableName());
            }
            admin.createTable(hTable);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Connection getConnection () {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
