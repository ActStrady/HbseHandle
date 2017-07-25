package com.wzfq.service;

import com.wzfq.util.HBase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class HbaseCommand implements IHbaseCommand{

    public String getData(String data){

        return null;
    }

    public void putData(String tableName, String family, String testRow, String testComun, String testValue) {
        HTable table = HBase.creatTable(tableName, family);
        Put put = new Put(Bytes.toBytes(testRow));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(testComun), Bytes.toBytes(testValue));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dropData(String data) {

    }

    public void upData(String data) {

    }
    @Test
    public void testG()throws IOException{
        putData("testq","cfg","ff","gg","mm");
    }
}
