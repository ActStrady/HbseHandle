package com.wzfq.service;

public interface IHbaseCommand {
    String getData (String data);
    void putData (String tableName, String family, String testRow, String testComun, String testValue);
    void dropData (String data);
    void upData(String data);
}
