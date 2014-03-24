package com.opendatagroup.hyperspectral.mockaccumulo;

import com.opendatagroup.hyperspectral.mockaccumulo.Connector;
import com.opendatagroup.hyperspectral.mockaccumulo.MultiTableBatchWriter;
import com.opendatagroup.hyperspectral.mockaccumulo.Mutation;
import com.opendatagroup.hyperspectral.mockaccumulo.MutationsRejectedException;

public class BatchWriter {
    Connector connector = null;
    String tableName = null;

    public BatchWriter(Connector _connector, String _tableName) {
        connector = _connector;
        tableName = _tableName;
    }

    public void addMutation(Mutation mutation) throws MutationsRejectedException {
        if (0 == 1)
            throw new MutationsRejectedException();

        connector.tables.get(tableName).put(mutation.key, mutation.value);
        connector.rewrite();
    }
}
