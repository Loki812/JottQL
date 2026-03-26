package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;
import base.parse.DDL.DropTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


public class SelectTable {
    public static void parse(String command) throws Exception {

        // TODO: test query with "select a, b, c"
        // TODO: test query with "select a.b"

        // For testing
        // boot app, insert junk data via command line
        // or call CREATETABLE(string query);
        // insert data(string query)
        // call parse("select a, b, c")
        // make 2nd table
        // insert into 2nd table
        // call parse("select a.a, b.a from a, b")
        // ensure select * works




        // take whitespace off, convert all to uppercase
        command = command.trim().toUpperCase();
        if(!command.startsWith("SELECT")) {
            System.err.println("Invalid SELECT Command");
            return;
        }

        if(!command.endsWith(";")) {
            System.err.println("Missing Semicolon");
            return;
        } else {
            // remove semicolon for cleaner parsing
            command = command.substring(0, command.length() - 1);
        }

        // grab these at start so we know how to break up query
        int selectIndex = command.indexOf("SELECT");
        int fromIndex = command.indexOf("FROM");
        int whereIndex = command.indexOf("WHERE");
        int orderIndex = command.indexOf("ORDERBY");


        // these lines will never execute due to previous check of .startsWith("SELECT")
        //        if (trimmedCommand.length() <= "SELECT".length()) {

        if (fromIndex == -1) {
            System.err.println("Missing FROM");
            return;
        }

        String projectionPart = command.substring(selectIndex + "SELECT".length(), fromIndex).trim();

        String tablePart;
        if (whereIndex == -1) {
            tablePart = command.substring(fromIndex + "FROM".length()).trim();
        } else {
            tablePart = command.substring(fromIndex + "FROM".length(), whereIndex).trim();
        }

        if (projectionPart.isEmpty()) {
            System.err.println("Missing projection attributes");
            return;
        }


        // Query is parsed

        if (tablePart.isEmpty()) {
            System.err.println("Missing table name");
            return;
        }
        ArrayList<String> tableNames = new ArrayList<>(List.of(tablePart.split(",")));
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



        ArrayList<String> attrNames = new ArrayList<>();
        ArrayList<Integer> selectedIndexes = new ArrayList<>();

        LinkedHashMap<String, AttributeSchema> attrSchemas = tableSchema.getAttributeSchemas();
        if (projectionPart.equals("*")) {
            attrSchemas.values()
                    .forEach(e -> attrNames.add(e.attributeName));
            for (int i = 0; i < attrSchemas.size(); i++) {
                selectedIndexes.add(i);
            }
        }
        else {
            String[] requestedAttributes = projectionPart.split(",");
            List<String> tableAttrNames = new ArrayList<>(attrSchemas.keySet());
            for (String attr : requestedAttributes) {

                attr = attr.trim();
                if (attr.isEmpty()) {
                    System.err.println("Invalid projection list");
                    return;
                }

                // TODO for alex:
                // so "select students.id and professors.id"
                // is NOT ambiguous because of the '.' in between the parser can detect where
                // each column is coming from.
                // a attribute is only ambiguous if you do:
                //
                // select id from students, professors

                // luckily for us if Om did the cartesian product correctly, the new tableschema
                // will have all the duplicate columns formatted like "students.id" so
                // we dont even have to check for ambiguity inside here.
                // if will just be caught by the column not existing in the schema


                // TODO Tommorow: take this out, make parseSelect() function that makes a temp
                // table, where only the selected columns exist

                // so then we go full query -> cartesianParse -> whereParse -> orderbyParse -> selectParse
                // in the main parse() command it just has a tableschema = cartesianParse(cartesianString, tableschema)
                //  if wherePart != null: tableSchema = whereParse(wherePart, tableSchema)
                // if orderByPart != null: tableSchema = orderByParse(orderBYPart, tableSchema)
                // if selectPart != null: tableSchema = selectParse(selectPart, tableSchema)
                // then look up the tableName and use tableSchema to print
                // this should be the parse() function in this file, all other logic already written in here
                // should be ported into parseSelect()
                // if this doesn't make sense in the morning do not implement, just a design solution
                // - Connor


                int indexOfAttribute = tableAttrNames.indexOf(attr);
                if (indexOfAttribute != -1) {
                    attrNames.add(attr);
                    selectedIndexes.add(indexOfAttribute);
                } else {
                    throw new RuntimeException("Invalid column referenced in select statement... (" + attr + ")");
                }
            }
        }

        //----------------------------------------
        // END OF SELECT, start of printing results
        // ---------------------------------------

        int pageId = tableSchema.getRootPageID();

        ArrayList<Integer> widths = DMLParser.printTopLine(attrNames);

        while (true) {
            Page p = BufferManager.getInstance().getPage(pageId);

            if (p == null) {
                System.err.println("Error: page " + pageId + " could not be loaded.");
                return;
            }
            ArrayList<Record> newRecords = new ArrayList<>();

            for (Record originalRecord : p.recordList) {
                Record projectedRecord = new Record();

                for (Integer index : selectedIndexes) {
                    projectedRecord.attributeList.add(originalRecord.attributeList.get(index));
                }
                newRecords.add(projectedRecord);

            }

            DMLParser.printRecords(widths, newRecords);
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
