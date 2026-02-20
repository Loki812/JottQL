package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;


public class SelectTable {

    private static void printRecord(Record record) {

        for(int i = 0; i < record.attributeList.size(); i++) {

            AttributeValue<?> av = record.attributeList.get(i);
            Object data = av.data;
            if(data == null) {

                System.out.println("NULL");

            }

            else {

                System.out.println(data.toString());

            }

            if(i < record.attributeList.size() - 1) {

                System.out.println("|");

            }

        }

        System.out.println();

    }

    public static void parse(String command) throws Exception {

        String trimmedCommand = command.trim();

        if(!trimmedCommand.startsWith("SELECT ")) {

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

        String tableName = remainder.trim();
        DataCatalog dataCatalog = DataCatalog.getInstance();
        TableSchema tableSchema = dataCatalog.getTableSchema(tableName);
        if(tableSchema == null) {

            System.out.println("Table " + tableName + " not found");
            throw new Exception();

        }

        int pageId = tableSchema.getRootPageID();
        Page p = BufferManager.getPage(pageID);
        while(p != null) {

            for(Record record : p.recordList) {

                printRecord(record);

            }

            if(p.nextPageId < 0) {

                break;

            }

            p = BufferManager.getPage(p.nextPageId);

        }

    }

}
