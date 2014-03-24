package com.opendatagroup.hyperspectral.mockaccumulo;

public class Value {
    byte[] value = null;

    public Value(byte[] _value) {
        value = _value;
    }

    public byte[] get() {
        return value;
    }
}
