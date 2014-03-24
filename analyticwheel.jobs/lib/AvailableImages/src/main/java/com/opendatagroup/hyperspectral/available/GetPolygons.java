package com.opendatagroup.hyperspectral.available;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;

// For testing with a fake database backend
// import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloException;
// import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloSecurityException;
// import com.opendatagroup.hyperspectral.mockaccumulo.BatchWriter;
// import com.opendatagroup.hyperspectral.mockaccumulo.Connector;
// import com.opendatagroup.hyperspectral.mockaccumulo.Constants;
// import com.opendatagroup.hyperspectral.mockaccumulo.Instance;
// import com.opendatagroup.hyperspectral.mockaccumulo.Key;
// import com.opendatagroup.hyperspectral.mockaccumulo.MultiTableBatchWriter;
// import com.opendatagroup.hyperspectral.mockaccumulo.Mutation;
// import com.opendatagroup.hyperspectral.mockaccumulo.MutationsRejectedException;
// import com.opendatagroup.hyperspectral.mockaccumulo.Range;
// import com.opendatagroup.hyperspectral.mockaccumulo.Scanner;
// import com.opendatagroup.hyperspectral.mockaccumulo.TableExistsException;
// import com.opendatagroup.hyperspectral.mockaccumulo.TableNotFoundException;
// import com.opendatagroup.hyperspectral.mockaccumulo.Value;
// import com.opendatagroup.hyperspectral.mockaccumulo.ZooKeeperInstance;

import org.apache.hadoop.io.Text;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class GetPolygons extends HttpServlet {
    protected static Instance zooKeeperInstance = null;
    protected static Connector connector = null;

    static {
        zooKeeperInstance = new ZooKeeperInstance("ENV_NAME", "node-1:port,node-2:port,node-3:port");
        try {
            if (zooKeeperInstance != null)
                connector = zooKeeperInstance.getConnector("root", "YOUR_PASSWORD".getBytes());
        }
        catch (AccumuloException exception) {
            connector = null;
        }
        catch (AccumuloSecurityException exception) {
            connector = null;
        }
    }

    @Override public void init(ServletConfig servletConfig) throws ServletException {

    }

    @Override protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        processRequest(request, response);
    }

    @Override protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        processRequest(request, response);
    }

    private Scanner scanner(String tableName) {
        Scanner out = null;
        try {
            out = connector.createScanner(tableName, Constants.NO_AUTHS);
        }
        catch (TableNotFoundException exception) {
            throw new RuntimeException("TableNotFoundException: " + exception.toString());
        }

        out.setRange(new Range());
        return out;
    }

    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (connector == null) {
            throw new IOException("No connection to Accumulo!");
        }

        response.setContentType("application/json");
        PrintWriter out = response.getWriter();
        out.write("{\"hyperion\": [");

        Scanner hyperionScanner = scanner("AvailableImages_hyperion");
        boolean first = true;
        for (Map.Entry<Key, Value> entry : hyperionScanner) {
            if (first)
                first = false;
            else
                out.write(", ");

            out.write(new String(entry.getValue().get()));
        }

        out.write("], \"ali\": [");

        // Scanner aliScanner = scanner("AvailableImages-ali");
        // boolean first = true;
        // for (Map.Entry<Key, Value> entry : aliScanner) {
        //     if (first)
        //         first = false;
        //     else
        //         out.write(", ");

        //     out.write(new String(entry.getValue().get()));
        // }

        out.write("]}");
    }

}
