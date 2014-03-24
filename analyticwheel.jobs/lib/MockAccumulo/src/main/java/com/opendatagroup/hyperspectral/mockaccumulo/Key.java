package com.opendatagroup.hyperspectral.mockaccumulo;

public class Key {
    String key = null;
    String columnFamily = null;
    String columnQualifier = null;

    public Key(String _key, String _columnFamily, String _columnQualifier) {
        key = _key;
        columnFamily = _columnFamily;
        columnQualifier = _columnQualifier;
    }

    public String getRow() {
        return key;
    }
}
