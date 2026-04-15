package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.concrete.Record;
import base.models.concrete.Page;
import base.models.schemas.AttributeSchema;
import base.models.schemas.TableSchema;

import java.util.ArrayList;

public class Cartesian {

    private static final DataCatalog catalog = DataCatalog.getInstance();
    private static final BufferManager buffer = BufferManager.getInstance();

    public static String Product(ArrayList<String> tableNames) throws Exception {
        //if we only have one table return it
        //recursion base case
        if(tableNames.size() == 1){
            TableSchema finalTable = catalog.getTableSchema(tableNames.getFirst());
            if(finalTable == null){
                System.out.println("Table " + tableNames.getFirst() + " not found");
                throw new Exception();
            }
            return tableNames.getFirst();
        }
        //get both table schemas
        String table1 = tableNames.removeFirst().trim().toUpperCase();
        String table2 = tableNames.removeFirst().trim().toUpperCase();
        TableSchema table1Schema = catalog.getTableSchema(table1);
        TableSchema table2Schema = catalog.getTableSchema(table2);
        if(table1Schema == null || table2Schema == null){
            System.out.println("Could not find table");
            throw new Exception();
        }
        //make new table and add both tables attribute schemas to it
        String newTable = "_TEMP_"+table1+'X'+table2;
        ArrayList<AttributeSchema> newAttributeSchemas = new ArrayList<>();
        for(AttributeSchema schema: new ArrayList<>(table1Schema.getAttributeSchemas().sequencedValues())){
            newAttributeSchemas.add(new AttributeSchema(schema, table1));
        }
        for(AttributeSchema schema: new ArrayList<>(table2Schema.getAttributeSchemas().sequencedValues())){
            newAttributeSchemas.add(new AttributeSchema(schema, table2));
        }
        TableSchema newTableSchema = new TableSchema(newTable, newAttributeSchemas);
        catalog.addTableSchema(newTableSchema);
        TableSchema.addTemp(newTable);
        //Block loop join them together
        Page table1page = buffer.getPage(table1Schema.rootPageID);
        while(table1page != null) {
            ArrayList<Record> table1Records = new ArrayList<>(table1page.recordList);
            Page table2page = buffer.getPage(table2Schema.rootPageID);
            while (table2page != null) {
                ArrayList<Record> table2Records = new ArrayList<>(table2page.recordList);
                for (Record record1 : table1Records) {
                    for (Record record2 : table2Records) {
                        var newRecordList = new ArrayList<>(record1.attributeList);
                        newRecordList.addAll(record2.attributeList);
                        Record newRecord = new Record();
                        newRecord.attributeList = newRecordList;
                        buffer.insertRecordIntoTableNoOrder(newTable, newRecord);
                    }
                }
                table2page = buffer.getPage(table2page.nextPageId);
            }
            table1page = buffer.getPage(table1page.nextPageId);
        }

        //recurse
        tableNames.addFirst(newTable);
        return Product(tableNames);
    }
}
