package com.opendatagroup.hyperspectral.mockaccumulo;

import java.lang.Iterable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.opendatagroup.hyperspectral.mockaccumulo.Key;
import com.opendatagroup.hyperspectral.mockaccumulo.Value;

public class Scanner implements Iterable<Map.Entry<Key, Value>> {
    public Map<Key, Value> table = null;
    public Range range = null;

    public Scanner(Map<Key, Value> _table) {
        table = _table;
    }

    public void setRange(Range _range) {
        range = _range;
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        if (range.startRow == null)
            return table.entrySet().iterator();
        else {
            Map<Key, Value> subtable = new HashMap<Key, Value>();
            for (Map.Entry<Key, Value> entry : table.entrySet()) {
                if (entry.getKey().getRow().toString().compareTo(range.startRow) >= 0  &&  entry.getKey().getRow().toString().compareTo(range.endRow) <= 0) {
                    subtable.put(entry.getKey(), entry.getValue());
                }
            }
            return subtable.entrySet().iterator();
        }
    }
}
