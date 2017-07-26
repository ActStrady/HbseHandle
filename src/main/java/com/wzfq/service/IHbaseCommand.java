package com.wzfq.service;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public interface IHbaseCommand {
    String getData (String tableName, String family, String testRow, String testComun);
    void putData (String tableName, String family, String testRow, String testComun, String testValue);
    void dropData (String tableName, String family, String testRow, String testComun);
    void upData(String data);
    List<Result> scanData (String tableName);
}
