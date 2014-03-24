package com.opendatagroup.hyperspectral.mockaccumulo;

import java.io.File;

import com.opendatagroup.hyperspectral.mockaccumulo.Connector;
import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloException;
import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloSecurityException;

public class Instance {
    public Instance(String dbName, String zooKeeperList) { }

    public Connector getConnector(String userName, byte[] password) throws AccumuloException, AccumuloSecurityException {
        return new Connector(new File("/tmp/mockaccumulo.json"));
    }
}

