package base.models;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static base.models.AttributeSchema.createAttributeSchemaFromQuery;


public class TableSchema {

    public String tableName;
    private int numOfAttributes;
    private LinkedHashMap<String, AttributeSchema> attributeSchemas;
    public String primaryKey;
    private int recordSize; // not stored on disk, calculated on load from disk or instantiation.
    private int rootPageID;

    public TableSchema() {}



    public static TableSchema createTableSchemaFromDisk(DataInputStream in) throws IOException {
        TableSchema ts = new TableSchema();

        // First int in a table schema block is the length of the name
        byte[] nameBytes = new byte[in.readInt()];
        in.readFully(nameBytes);
        ts.tableName = new String(nameBytes, StandardCharsets.UTF_8);

        ts.numOfAttributes = in.readInt();
        ts.attributeSchemas = new LinkedHashMap<>();
        // accumulator for total record size as well as offset
        int tempSize = 0;
        for (int i = 0; i < ts.numOfAttributes; i++) {
            AttributeSchema a = AttributeSchema.createAttributeSchemaFromDisk(in, tempSize);
            tempSize += a.getLength();
            ts.attributeSchemas.put(a.attributeName, a);
        }

        ts.recordSize = tempSize;
        ts.rootPageID = in.readInt();

        return ts;
    }

    public static TableSchema createTableSchemaFromQuery(String name, ArrayList<String> attributes) throws Exception {
        DataCatalog dc = DataCatalog.getInstance();

        TableSchema ts = new TableSchema();
        ts.tableName = name;
        ts.rootPageID = dc.getNextAvailablePageID();
        for(String attribute : attributes) {
            AttributeSchema atb = createAttributeSchemaFromQuery(attribute);
            ts.attributeSchemas.put(atb.attributeName, atb);
        }
        return ts;
    }

    public void saveTableSchemaToDisk(DataOutputStream out) throws IOException {
        out.writeInt(tableName.length());

        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
        out.write(nameBytes);

        out.writeInt(numOfAttributes);

        for (AttributeSchema a : attributeSchemas.values()) {
            a.saveAttributeSchemaToDisk(out);
        }

        out.writeInt(recordSize);
        out.writeInt(rootPageID);
    }

    public int getRecordSize() { return recordSize; }

    public int getRootPageID() { return rootPageID; }

    public String getPrimaryKey() { return primaryKey; }

    public Integer getPrimaryIndex() {
        Integer index = 0;
        for (String key : attributeSchemas.sequencedKeySet()) {
            if (primaryKey.equals(key)) {
                return index;
            }else{
                index++;
            }
        }
        return null;
    }

    public AttributeSchema getAttributeSchema(String name) {
        return attributeSchemas.get(name);
    }

    public void removeAttributeSchema(String name) {
        attributeSchemas.remove(name);
        numOfAttributes -= 1;
    }

    public void addAttributeSchema(AttributeSchema a) {
        attributeSchemas.put(a.attributeName, a);
        numOfAttributes += 1;
    }
}
