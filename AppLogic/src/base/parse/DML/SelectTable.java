package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;
import base.parse.DDL.DropTable;

import java.util.ArrayList;
import java.util.List;


public class SelectTable {
    public static void parse(String command) throws Exception {

        String trimmedCommand = command.trim().toUpperCase();
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
        ArrayList<String> tableNames = new ArrayList<>(List.of(remainder.split(",")));
        ArrayList<String> tempTables = new ArrayList<>();
        String tableName = Cartesian.Product(tableNames);
        if(tableName.startsWith("_")){
            tempTables.add(tableName);
        }

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

                int matchedIndex = -1;
                boolean ambiguous = false;

                for(int i = 0; i < tableAttributes.size(); i++) {

                    String schemaAttr = tableAttributes.get(i).attributeName;
                    int dotIndex = schemaAttr.lastIndexOf(".");
                    if(schemaAttr.equalsIgnoreCase(attr)) {

                        if(matchedIndex != -1) {
                            ambiguous = true;
                            break;
                        }

                        matchedIndex = i;

                    }
                    else if(dotIndex != -1) {


                        String unqualifiedName =
                                schemaAttr.substring(dotIndex + 1);

                        if(unqualifiedName.equalsIgnoreCase(attr)) {

                            if(matchedIndex != -1) {
                                ambiguous = true;
                                break;
                            }

                            matchedIndex = i;

                        }

                    }

                }

                if(matchedIndex == -1) {

                    System.err.println("Attribute " + attr + " not found in table " + tableName);
                    return;

                }

                attrNames.add(tableAttributes.get(matchedIndex).attributeName);
                selectedIndexes.add(matchedIndex);

            }
        }

        int pageId = tableSchema.getRootPageID();

        ArrayList<Integer> widths = DMLParser.printTopLine(attrNames);

        while (true) {
            Page p = BufferManager.getInstance().getPage(pageId);

            if (p == null) {
                System.err.println("Error: page " + pageId + " could not be loaded.");
                return;
            }

            for (Record originalRecord : p.recordList) {
                Record projectedRecord = new Record();

                for (Integer index : selectedIndexes) {
                    projectedRecord.attributeList.add(originalRecord.attributeList.get(index));
                }

            }

            DMLParser.printRecords(widths, p.recordList);
            if (p.nextPageId == -1) {
                break;
            }
            else {
                pageId = p.nextPageId;
            }



        }

        for(String table: tempTables) {
            DropTable.execute("DROP TABLE " + table.trim().toUpperCase()+";");
        }


    }

}
