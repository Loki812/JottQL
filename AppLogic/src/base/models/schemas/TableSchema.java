package base.models.schemas;


import base.buffer.BufferManager;
import base.models.DataCatalog;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;


import static base.models.schemas.AttributeSchema.createAttributeSchemaFromQuery;


public class TableSchema {

    private static final DataCatalog dataCatalog = DataCatalog.getInstance();

    public String tableName;
    private int numOfAttributes;
    private LinkedHashMap<String, AttributeSchema> attributeSchemas;
    public String primaryKey;
    public int rootPageID;
    private static ArrayList<String> tempTableNames = new ArrayList<>();

    public TableSchema() {
        attributeSchemas = new LinkedHashMap<>();
    }

    public TableSchema(String tableName, ArrayList<AttributeSchema> attributeSchemas) throws Exception {
        this.tableName = tableName;
        this.numOfAttributes = attributeSchemas.size();
        this.attributeSchemas = new LinkedHashMap<>();
        for (AttributeSchema attributeSchema : attributeSchemas) {
            this.attributeSchemas.put(attributeSchema.attributeName, attributeSchema);
        }
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
        // root pageID gets set in DataCatalog.addTableSchema()
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
            ts.numOfAttributes += 1;
        }
        if(ts.primaryKey == null){
            System.out.println("No primary key found");
            throw new Exception();
        }
        return ts;
    }

    public void saveTableSchemaToDisk(DataOutputStream out) throws IOException {
        out.writeInt(tableName.getBytes(StandardCharsets.UTF_8).length);

        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
        out.write(nameBytes);

        out.writeInt(numOfAttributes);

        for (AttributeSchema a : attributeSchemas.values()) {
            a.saveAttributeSchemaToDisk(out);
        }

        out.writeInt(rootPageID);
    }


    public int getRootPageID() { return rootPageID; }


    public Integer getIndex(String attribute_key) {

        if (attribute_key == null) {
            return null;
        }

        Integer index = 0;

        for (String key : attributeSchemas.sequencedKeySet()) {
            if (attribute_key.equals(key)) {
                return index;
            } else {
                index++;
            }
        }

        return null;
    }


    public void removeAttributeSchema(String name) throws Exception {
        if(primaryKey.equals(name)){
            System.out.println("Cannot remove primary key");
            throw new RuntimeException();
        }

        if(!attributeSchemas.containsKey(name)){
            System.out.println("Column does not exist");
            throw new RuntimeException();
        }
        attributeSchemas.remove(name);
        numOfAttributes -= 1;

    }

    public void addAttributeSchema(AttributeSchema a) throws Exception {
        if(a.isPrimaryKey()){
            System.out.println("Primary key already exists");
            throw new RuntimeException();
        }
        if(a.getNotNull()){
            if(a.getDefaultVal() == null){
                System.out.println("Not null requires a default value when altering a table");
                throw new RuntimeException();
            }
        }
        attributeSchemas.put(a.attributeName, a);
        numOfAttributes += 1;
    }


    public LinkedHashMap<String, AttributeSchema> getAttributeSchemas() {
        return attributeSchemas;
    }

    /**
     * Create a temporary copy of a TableSchema.
     *
     * @param selectedIncdices only used during projection, will only copy certain attributeSchemas over to
     *                         the copy. if a primary key is not selected, the first projected attribute will be
     *                         the new primary key
     *
     * @return the copied TableSchema
     * @throws Exception if the given schema shares a name with a database already inside the disk.
     */
    public TableSchema makeTempCopy(ArrayList<Integer> selectedIncdices) throws Exception{
        // Create new copy of the table schema
        TableSchema copy = new TableSchema();

        // Give the table a name indicating that it is temporary
        String name = "_TEMP_" + tableName;
        //make sure name is unique
        while (tempTableNames.contains(name)) {
            name = "_TEMP_" + name;
        }
        copy.tableName = name;
        tempTableNames.add(name);
        copy.primaryKey = primaryKey;

        if (selectedIncdices.isEmpty()) {
            // Copy the other fields
            copy.attributeSchemas = new LinkedHashMap<>();
            for (AttributeSchema attributeSchema : attributeSchemas.values()) {
                copy.attributeSchemas.put(attributeSchema.attributeName, attributeSchema);
                copy.numOfAttributes += 1;
            }
        } else {
            copy.numOfAttributes = selectedIncdices.size();
            ArrayList<AttributeSchema> attrSchemas = new ArrayList<>(attributeSchemas.sequencedValues());

            boolean foundPrimaryKey = false;
            for (Integer index : selectedIncdices) {
                AttributeSchema schema = attrSchemas.get(index);
                if (schema.isPrimaryKey()) {
                    foundPrimaryKey = true;
                    copy.primaryKey = schema.attributeName;
                }
                copy.attributeSchemas.put(schema.attributeName, schema);
                copy.numOfAttributes += 1;
            }
            if (!foundPrimaryKey) {
                copy.primaryKey = null;
            }
        }

        // Give it a new root page ID and add it to the list of tables in the DataCatalog
        dataCatalog.addTableSchema(copy);

        // Return the new copy
        return copy;
    }

    public static void addTemp(String tableName){
        tempTableNames.add(tableName);
    }

    public static void deleteTemps(){
        BufferManager bm = BufferManager.getInstance();
        while (!tempTableNames.isEmpty()) {
            String tableName = tempTableNames.removeFirst();
            try {
                bm.deleteTable(tableName);
            } catch (Exception e) {
                //IDK why but this is necessary
                DataCatalog.getInstance().removeTableSchema(tableName);
            }
        }
    }
}
