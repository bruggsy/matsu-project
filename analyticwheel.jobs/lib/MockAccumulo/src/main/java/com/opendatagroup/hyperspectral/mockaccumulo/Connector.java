package com.opendatagroup.hyperspectral.mockaccumulo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.io.JsonStringEncoder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.opendatagroup.hyperspectral.mockaccumulo.Key;
import com.opendatagroup.hyperspectral.mockaccumulo.Value;
import com.opendatagroup.hyperspectral.mockaccumulo.Scanner;
import com.opendatagroup.hyperspectral.mockaccumulo.TableOperations;
import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloException;
import com.opendatagroup.hyperspectral.mockaccumulo.AccumuloSecurityException;
import com.opendatagroup.hyperspectral.mockaccumulo.TableNotFoundException;

public class Connector {
    private File file = null;
    public Map<String, Map<Key, Value>> tables = null;

    ObjectMapper objectMapper = new ObjectMapper();

    public Connector(File _file) {
        file = _file;
        reread();
    }

    public void reread() {
        tables = new HashMap<String, Map<Key, Value>>();

        if (!file.exists()) {
            try {
                PrintWriter out = new PrintWriter(file);
                out.write("{}");
                out.close();
            }
            catch (IOException exception) {
                throw new RuntimeException("couldn't create " + file.getAbsolutePath());
            }
        }

        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
        }
        catch (FileNotFoundException exception) {
            throw new RuntimeException("couldn't find " + file.getAbsolutePath());
        }
        ObjectNode rootNode = null;
        try {
            rootNode = (ObjectNode)objectMapper.readTree(fileInputStream);
        }
        catch (IOException exception) {
            throw new RuntimeException("couldn't read " + file.getAbsolutePath());
        }
        Iterator<Map.Entry<String, JsonNode>> rootNodeIterator = rootNode.getFields();
        while (rootNodeIterator.hasNext()) {
            Map.Entry<String, JsonNode> tablesEntry = rootNodeIterator.next();
            Map<Key, Value> tableMap = new HashMap<Key, Value>();
            tables.put(tablesEntry.getKey(), tableMap);

            ObjectNode tableNode = (ObjectNode)tablesEntry.getValue();
            Iterator<Map.Entry<String, JsonNode>> tableNodeIterator = tableNode.getFields();
            while (tableNodeIterator.hasNext()) {
                Map.Entry<String, JsonNode> valuesEntry = tableNodeIterator.next();
                tableMap.put(new Key(valuesEntry.getKey(), "", ""), new Value(valuesEntry.getValue().getTextValue().getBytes()));
            }
        }
        try {
            fileInputStream.close();
        }
        catch (IOException exception) {
            throw new RuntimeException("IOException: " + exception.getMessage());
        }
    }

    public void rewrite() {
        file.delete();
        PrintWriter out = null;
        try {
            file.createNewFile();
            out = new PrintWriter(file);
        }
        catch (IOException exception) {
            throw new RuntimeException("IOException: " + exception.getMessage());
        }
        out.write("{");

        boolean first1 = true;
        for (Map.Entry<String, Map<Key, Value>> tableEntry : tables.entrySet()) {
            if (first1)
                first1 = false;
            else
                out.write(", ");

            out.write("\"" + tableEntry.getKey() + "\": {");

            boolean first2 = true;
            for (Map.Entry<Key, Value> valuesEntry : tableEntry.getValue().entrySet()) {
                if (first2)
                    first2 = false;
                else
                    out.write(", ");

                out.write("\"" + valuesEntry.getKey().getRow() + "\": \"" + new String(JsonStringEncoder.getInstance().quoteAsString(new String(valuesEntry.getValue().get()))) + "\"");
            }
            out.write("}");
        }

        out.write("}");
        out.close();
    }

    public Scanner createScanner(String tableName, int whatever) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        reread();
        if (tables.containsKey(tableName))
            return new Scanner(tables.get(tableName));
        else
            throw new TableNotFoundException();
    }

    public TableOperations tableOperations() {
        return new TableOperations(this);
    }

    public MultiTableBatchWriter createMultiTableBatchWriter(long what, int ev, int er) {
        return new MultiTableBatchWriter(what, ev, er, this);
    }
}
