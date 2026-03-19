package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;

import java.util.ArrayList;


public class SelectTable {

    /*
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

    */

    public static void parse(String command)  {

        String trimmedCommand = command.trim();
        if(!trimmedCommand.startsWith("SELECT")) {

            System.err.println("Invalid SELECT Command");
            return;

        }

        if(!trimmedCommand.endsWith(";")) {

            System.err.println("Missing Semicolon");
            return;

        }

        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();

        if (trimmedCommand.length() <= "SELECT".length()) {
            System.err.println("Invalid SELECT command");
            return;
        }

        String remainder = trimmedCommand.substring("SELECT".length()).trim();

        int fromIndex = remainder.indexOf("FROM");
        if (fromIndex == -1) {
            System.err.println("Missing FROM");
            return;
        }

        String projectionPart = remainder.substring(0, fromIndex).trim();
        String tablePart = remainder.substring(fromIndex + "FROM".length()).trim();

        if (projectionPart.isEmpty()) {
            System.err.println("Missing projection attributes");
            return;
        }

        if (tablePart.isEmpty()) {
            System.err.println("Missing table name");
            return;
        }

        String tableName = remainder.trim().toUpperCase();
        DataCatalog dataCatalog = DataCatalog.getInstance();
        TableSchema tableSchema = dataCatalog.getTableSchema(tableName);
        if(tableSchema == null) {

            System.err.println("Table " + tableName + " not found");
            return;

        }

        ArrayList<AttributeSchema> tableAttributes =
                new ArrayList<>(tableSchema.getAttributeSchemas().sequencedValues());

        ArrayList<String> attrNames = new ArrayList<>();
        ArrayList<Integer> selectedIndexes = new ArrayList<>();

        if (projectionPart.equals("*")) {
            for (int i = 0; i < tableAttributes.size(); i++) {
                attrNames.add(tableAttributes.get(i).attributeName);
                selectedIndexes.add(i);
            }
        }
        else {
            String[] requestedAttributes = projectionPart.split(",");

            for (String rawAttr : requestedAttributes) {
                String attr = rawAttr.trim();

                if (attr.isEmpty()) {
                    System.err.println("Invalid projection list");
                    return;
                }

                boolean found = false;
                for (int i = 0; i < tableAttributes.size(); i++) {
                    if (tableAttributes.get(i).attributeName.equalsIgnoreCase(attr)) {
                        attrNames.add(tableAttributes.get(i).attributeName);
                        selectedIndexes.add(i);
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    System.err.println("Attribute " + attr + " not found in table " + tableName);
                    return;
                }
            }
        }

        int pageId = tableSchema.getRootPageID();
        ArrayList<Record> records = new ArrayList<>();

        while (true) {
            Page p = BufferManager.getPage(pageId);

            if (p == null) {
                System.err.println("Error: page " + pageId + " could not be loaded.");
                return;
            }

            for (Record originalRecord : p.recordList) {
                Record projectedRecord = new Record();

                for (Integer index : selectedIndexes) {
                    projectedRecord.attributeList.add(originalRecord.attributeList.get(index));
                }

                records.add(projectedRecord);
            }

            if (p.nextPageId == -1) {
                break;
            }
            else {
                pageId = p.nextPageId;
            }
        }

        DMLParser.printResultSet(attrNames, records);
    }

}
