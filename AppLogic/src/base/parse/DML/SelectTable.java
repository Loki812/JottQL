package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;

import java.util.ArrayList;


public class SelectTable {

    private static void printRecord(Record record, int[] spacing) {
        System.out.print(" | ");
        for(int i = 0; i < record.attributeList.size(); i++) {
            AttributeValue<?> av = record.attributeList.get(i);
            System.out.print(" ".repeat(spacing[i]-av.toString().length())+av.toString());
            System.out.print(" | ");
        }
        System.out.println();

    }

    private static void printAttributes(TableSchema tableSchema, int[] spacing) {
        System.out.print(" | ");
        ArrayList<AttributeSchema> attributeSchemas = new ArrayList<>(tableSchema.getAttributeSchemas().sequencedValues());
        if(spacing != null) {
            for (int i = 0; i < attributeSchemas.size(); i++) {
                String attr = attributeSchemas.get(i).attributeName;
                System.out.print(" ".repeat(spacing[i] - attr.length()) + attr);
                System.out.print(" | ");
            }
            System.out.println();
            for (int i = 0; i < attributeSchemas.size(); i++) {
                System.out.print("-");
                System.out.print("-".repeat(spacing[i]));
                System.out.print("-");
            }
        }else {
            for (AttributeSchema attributeSchema : attributeSchemas) {
                String attr = attributeSchema.attributeName;
                System.out.print(attr);
                System.out.print(" | ");
            }
            System.out.println();
            for (AttributeSchema attributeSchema : attributeSchemas) {
                System.out.print("-");
                String attr = attributeSchema.attributeName;
                System.out.print("-".repeat(attr.length()));
                System.out.print("-");
            }
        }
        System.out.println();
    }

    private static int[] findSpacing(Page page, TableSchema tableSchema) {
        try{
            int[] spacing = new int[page.recordList.getFirst().attributeList.size()];
            ArrayList<AttributeSchema> attributeSchemas = new ArrayList<>(tableSchema.getAttributeSchemas().sequencedValues());
            for (int i = 0; i < attributeSchemas.size(); i++) {
                if (spacing[i] < attributeSchemas.get(i).attributeName.length()) {
                    spacing[i] = attributeSchemas.get(i).attributeName.length();
                }
            }
            while (page != null) {
                for (Record r : page.recordList) {
                    for (int i = 0; i < r.attributeList.size(); i++) {
                        if (spacing[i] < r.attributeList.get(i).toString().length()) {
                            spacing[i] = r.attributeList.get(i).toString().length();
                        }
                    }
                }
                page = BufferManager.getPage(page.nextPageId);
            }
            return spacing;
        }catch (Exception e) {
            return null;
        }
    }

    public static void parse(String command) throws Exception {

        String trimmedCommand = command.trim();
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

        String tableName = remainder.trim().toUpperCase();
        DataCatalog dataCatalog = DataCatalog.getInstance();
        TableSchema tableSchema = dataCatalog.getTableSchema(tableName);
        if(tableSchema == null) {

            System.out.println("Table " + tableName + " not found");
            throw new Exception();

        }

        int pageId = tableSchema.getRootPageID();
        Page p = BufferManager.getPage(pageId);
        int[] spacing = findSpacing(p, tableSchema);
        printAttributes(tableSchema, spacing);
        if(spacing != null) {
            while (p != null) {

                for (Record record : p.recordList) {

                    printRecord(record, spacing);

                }

                if (p.nextPageId < 0) {

                    break;

                }

                p = BufferManager.getPage(p.nextPageId);

            }
        }

    }

}
