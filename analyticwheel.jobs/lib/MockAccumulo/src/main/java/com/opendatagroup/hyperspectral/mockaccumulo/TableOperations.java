package com.opendatagroup.hyperspectral.mockaccumulo;

import java.util.HashMap;

import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloException;
import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloSecurityException;
import com.opendatagroup.hyperspectral.mockaccumulo.Connector;
import com.opendatagroup.hyperspectral.mockaccumulo.Key;
import com.opendatagroup.hyperspectral.mockaccumulo.TableExistsException;
import com.opendatagroup.hyperspectral.mockaccumulo.Value;

public class TableOperations {
    public Connector connector = null;

    public TableOperations(Connector _connector) {
        connector = _connector;
    }

    public boolean exists(String tableName) {
        return connector.tables.containsKey(tableName);
    }

    public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        if (0 == 1)
            throw new AccumuloException();
        if (0 == 1)
            throw new AccumuloSecurityException();

        if (exists(tableName))
            throw new TableExistsException();
        connector.tables.put(tableName, new HashMap<Key, Value>());
        connector.rewrite();
    }

}
