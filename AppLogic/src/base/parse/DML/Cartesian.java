package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;
import base.parse.DDL.DropTable;

import java.util.ArrayList;

public class Cartesian {
    private static final DataCatalog catalog = DataCatalog.getInstance();
    private static final BufferManager buffer = BufferManager.getInstance();
    public static String Product(ArrayList<String> tableNames) throws Exception {
        if(tableNames.size() == 1){
            return tableNames.getFirst();
        }
        String table1 = tableNames.removeFirst().trim().toUpperCase();
        String table2 = tableNames.removeFirst().trim().toUpperCase();
        TableSchema table1Schema = catalog.getTableSchema(table1);
        TableSchema table2Schema = catalog.getTableSchema(table2);
        if(table1Schema == null || table2Schema == null){
            System.out.println("Could not find table");
            throw new Exception();
        }
        String newTable = "_"+table1+'X'+table2;
        ArrayList<AttributeSchema> newAttributeSchemas = new ArrayList<>();
        for(AttributeSchema schema: new ArrayList<>(table1Schema.getAttributeSchemas().sequencedValues())){
            newAttributeSchemas.add(new AttributeSchema(schema, table1));
        }
        for(AttributeSchema schema: new ArrayList<>(table2Schema.getAttributeSchemas().sequencedValues())){
            newAttributeSchemas.add(new AttributeSchema(schema, table2));
        }
        TableSchema newTableSchema = new TableSchema(newTable, newAttributeSchemas);
        catalog.addTableSchema(newTableSchema);
        buffer.createNewPage(newTableSchema.getRootPageID(), newTable);
        Page newRoot = buffer.getPage(newTableSchema.getRootPageID());
        Page table1page = buffer.getPage(table1Schema.rootPageID);
        Page table2page = buffer.getPage(table2Schema.rootPageID);
        while(table1page != null) {
            ArrayList<Record> table1Records = new ArrayList<>(table1page.recordList);
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
        if(table1.startsWith("_")){
            DropTable.execute("DROP TABLE "+table1+";");
        }
        tableNames.addFirst(newTable);
        return Product(tableNames);
    }
}
