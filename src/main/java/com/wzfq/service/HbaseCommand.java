package com.wzfq.service;

import com.wzfq.util.HBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseCommand implements IHbaseCommand {
    private static final Logger logger = LoggerFactory.getLogger(HbaseCommand.class);

    public String getData(String tableName, String family, String testRow, String testComun) {
        Connection connection = HBase.getConnection();
        String stValue = "";
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(testRow));
            Result result = table.get(get);
            byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(testComun));
            stValue = Bytes.toString(value);
        } catch (IOException e) {
            logger.error("HbaseCOmmand.getData" + e);
        }
        return stValue;
    }

    public void putData(String tableName, String family, String testRow, String testComun, String testValue) {
        Connection connection = HBase.getConnection();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(testRow));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(testComun), Bytes.toBytes(testValue));
            table.put(put);
        } catch (IOException e) {
            logger.error("HbaseCOmmand.putData" + e);
        }
    }

    public void dropData(String tableName, String family, String testRow, String testComun) {
        Connection connection = HBase.getConnection();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(testRow));
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(testComun));
            table.delete(delete);
        } catch (IOException e) {
            logger.error("HbaseCOmmand.dropData" + e);
        }
    }

    public void upData(String data) {

    }

    public List<Result> scanData(String tableName) {
        Connection connection = HBase.getConnection();
        List<Result> results = new ArrayList<Result>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                results.add(result);
            }
        } catch (IOException e) {
            logger.error("HbaseCOmmand.getData" + e);
        }
        return results;
    }

    @Test
    public void testG() {
        //putData("testq","cfg","ff","hh","kkk");
        logger.debug(scanData("testq")+"");
        //dropData("testq", "cfg", "ff", "hh");
    }
}
