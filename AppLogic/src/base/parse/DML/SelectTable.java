package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;
import base.parse.DDL.DropTable;

import java.util.ArrayList;
import java.util.List;


public class SelectTable {
    public static void parse(String command) throws Exception {

        BufferManager bm = BufferManager.getInstance();
        String trimmedCommand = command.trim().toUpperCase();
        if(!trimmedCommand.startsWith("SELECT")) {

            System.out.println("Invalid SELECT Command");
            throw new Exception();

        }

        if(!trimmedCommand.endsWith(";")) {

            System.out.println("Missing Semicolon");
            throw new Exception();

        }

        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        String remainder = trimmedCommand.substring("SELECT ".length()).trim();
        if(!remainder.startsWith("*")) {

            System.out.println("Invalid Query Command");
            throw new Exception();

        }

        remainder = remainder.substring(1).trim();
        if(!remainder.startsWith("FROM")) {

            System.out.println("Missing from");
            throw new Exception();

        }

        remainder = remainder.substring("FROM".length()).trim();
        if(remainder.isEmpty()) {

            System.out.println("Missing table name");
            throw new Exception();

        }
        ArrayList<String> tableNames = new ArrayList<>(List.of(remainder.split(",")));
        ArrayList<String> tempTables = new ArrayList<>();
        String tableName = Cartesian.Product(tableNames);
        if(tableName.startsWith("_")){
            tempTables.add(tableName);
        }

        DataCatalog dataCatalog = DataCatalog.getInstance();
        TableSchema tableSchema = dataCatalog.getTableSchema(tableName);
        if(tableSchema == null) {

            System.out.println("Table " + tableName + " not found");
            throw new Exception();

        }

        int pageId = tableSchema.getRootPageID();

        ArrayList<String> attrNames = new ArrayList<>();
        for (AttributeSchema att : tableSchema.getAttributeSchemas().sequencedValues()) {
            attrNames.add(att.attributeName);
        }

        ArrayList<Integer> widths = DMLParser.printTopLine(attrNames);
        int totalNumRecords = 0;

        while (true) {
            Page p = bm.getPage(pageId);
            DMLParser.printRecords(widths, p.recordList);
            totalNumRecords += p.recordList.size();
            if (p.nextPageId == -1) {
                break;
            } else {
                pageId = p.nextPageId;
            }
        }

        for(String table: tempTables) {
            DropTable.execute("DROP TABLE " + table.trim().toUpperCase()+";");
        }

        System.out.println("\n" + totalNumRecords + " Rows Returned");
    }

}
