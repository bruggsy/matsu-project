package com.opendatagroup.hyperspectral.mockaccumulo;

import com.opendatagroup.hyperspectral.mockaccumulo.Connector;
import com.opendatagroup.hyperspectral.mockaccumulo.BatchWriter;
import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloException;
import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloSecurityException;
import com.opendatagroup.hyperspectral.mockaccumulo.TableNotFoundException;

public class MultiTableBatchWriter {
    public Connector connector = null;

    public MultiTableBatchWriter(long what, int ev, int er, Connector _connector) {
        connector = _connector;
    }

    public BatchWriter getBatchWriter(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (0 == 1)
            throw new AccumuloException();
        if (0 == 1)
            throw new AccumuloSecurityException();
        if (0 == 1)
            throw new TableNotFoundException();

        return new BatchWriter(connector, tableName);
    }
}
