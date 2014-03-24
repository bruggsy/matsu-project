package com.opendatagroup.hyperspectral.mockaccumulo;

public class Range {
    public String startRow = null;
    public String endRow = null;

    public Range() { }
    public Range(String _startRow, String _endRow) {
        startRow = _startRow;
        endRow = _endRow;
    }
}
