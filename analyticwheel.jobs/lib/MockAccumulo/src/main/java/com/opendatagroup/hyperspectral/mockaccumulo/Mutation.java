package com.opendatagroup.hyperspectral.mockaccumulo;

import com.opendatagroup.hyperspectral.mockaccumulo.Text;
import com.opendatagroup.hyperspectral.mockaccumulo.Key;
import com.opendatagroup.hyperspectral.mockaccumulo.Value;

public class Mutation {
    public String keyString = null;
    public Key key = null;
    public Value value = null;

    public Mutation(Text _keyString) {
        keyString = _keyString.toString();
    }

    public void put(Text columnFamily, Text columnQualifier, Value _value) {
        key = new Key(keyString, columnFamily.toString(), columnQualifier.toString());
        value = _value;
    }
}
