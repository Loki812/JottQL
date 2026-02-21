package base.models;

import base.buffer.BufferManager;

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
    public int rootPageID;
    private static final DataCatalog dc = DataCatalog.getInstance();

    public TableSchema() {
        rootPageID = dc.getNextAvailablePageID();
        attributeSchemas = new LinkedHashMap<>();
    }



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
            if(a.isPrimaryKey()){
                ts.primaryKey = a.attributeName;
            }
            ts.attributeSchemas.put(a.attributeName, a);
        }
        ts.rootPageID = in.readInt();

        return ts;
    }

    public static TableSchema createTableSchemaFromQuery(String name, ArrayList<String> attributes) throws Exception {

        TableSchema ts = new TableSchema();
        ts.tableName = name;

        for(String attribute : attributes) {
            AttributeSchema atb = createAttributeSchemaFromQuery(attribute);
            if(atb.isPrimaryKey()){
                if(ts.primaryKey == null){
                    ts.primaryKey = atb.attributeName;
                }else{
                    System.out.println("Duplicate primary key");
                    throw new Exception();
                }

            }
            ts.attributeSchemas.put(atb.attributeName, atb);
        }
        if(ts.primaryKey == null){
            System.out.println("No primary key found");
            throw new Exception();
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

        out.writeInt(rootPageID);
    }


    public int getRootPageID() { return rootPageID; }

    public String getPrimaryKey() { return primaryKey; }

    public Integer getIndex(String attribute_key) {
        Integer index = 0;
        for (String key : attributeSchemas.sequencedKeySet()) {
            if (attribute_key.equals(key)) {
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

    public void removeAttributeSchema(String name) throws Exception {
        if(primaryKey.equals(name)){
            System.out.println("Cannot remove primary key");
            throw new Exception();
        }
        Integer index = getIndex(name);
        if(index == null){
            System.out.println("Column does not exist");
            throw new Exception();
        }
        BufferManager.getPage(rootPageID).deleteColumn(index);
        attributeSchemas.remove(name);
        numOfAttributes -= 1;

    }

    public void addAttributeSchema(AttributeSchema a) throws Exception {
        if(a.isPrimaryKey()){
            System.out.println("Primary key already exists");
            throw new Exception();
        }
        if(a.getNotNull()){
            if(a.getDefaultVal() == null){
                System.out.println("Not null requires a default value when altering a table");
                throw new Exception();
            }
        }
        attributeSchemas.put(a.attributeName, a);
        numOfAttributes += 1;
        Page page = BufferManager.getPage(rootPageID);
        assert page != null;
        page.addColumn(new AttributeValue<>(a.getDefaultVal(),a.getDataType()));

    }

    public LinkedHashMap<String, AttributeSchema> getAttributeSchemas() {
        return attributeSchemas;
    }
}
