package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;
import base.parse.DDL.DropTable;

import java.util.*;

import static base.parse.DML.OrderBy.executeOrderBy;


public class SelectTable {
    public static void parse(String command) throws Exception {


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

        // -----------------------
        // From portion
        // -----------------------
        String tableName = Cartesian.Product(tableNames);
        if(tableName.startsWith("_")){
            tempTables.add(tableName);
        }



        // -----------------------
        // WHERE portion
        // -----------------------
        // TODO:
        // if wherePart.length != 0:
        //      tableName = parseWhere(whereString, tableName)


        // ------------------------
        // ORDERBY portion
        // -----------------------

        if (orderIndex != -1) {
            String orderByPart = command.substring(orderIndex + "ORDERBY".length()).trim();
            if (!orderByPart.isEmpty()) {
                tableName = parseOrderBy(orderByPart, tableName);
            }
        }


        //-------------------------
        // SELECT PORTION
        //-------------------------

        tableName = parseSelect(projectionPart, tableName);

        //----------------------------------------
        // END OF SELECT, start of printing results
        // ---------------------------------------

        TableSchema finalTableSchema = DataCatalog.getInstance().getTableSchema(tableName);

        int pageId = finalTableSchema.getRootPageID();



        ArrayList<Integer> widths = DMLParser.printTopLine(
                new ArrayList<>(finalTableSchema.getAttributeSchemas().sequencedKeySet())
        );

        while (true) {
            Page p = BufferManager.getInstance().getPage(pageId);

            if (p == null) {
                System.err.println("Error: page " + pageId + " could not be loaded.");
                return;
            }

            DMLParser.printRecords(widths, p.recordList);
            if (p.nextPageId == -1) {
                break;
            }
            else {
                pageId = p.nextPageId;
            }



        }

        ArrayList<String> finalTempTables = DataCatalog.getInstance().tempTables();

        // perfered to move all temp tables to be recorded in DataCatalog for single source of truth
        for (String table : finalTempTables) {
            BufferManager.getInstance().deleteTable(table);
        }

        for(String table: tempTables) {
            BufferManager.getInstance().deleteTable(table.trim().toUpperCase());
        }


    }



    /**
     * Performs a conditional where on a table, creating a temporary copy to complete the query
     *
     * @param wherePart a parsed substring of a sql query
     *                  beginning after the "FROM" portion until the orderBy or ';' symbol
     *                  ex. "WHERE name = "Joe""
     *
     * @param tableName the table you are applying the operation to
     * @return the name of the temp table with the WHERE applied
     */
    public static String parseWhere(String wherePart, String tableName) throws Exception {
        return tableName;
    }

    /**
     * Performs an orderBy operation on a table, creating a temporary copy to complete the query
     *
     * @param orderByPart a parsed substring of a sql query
     *                    beginning after the "WHERE" portion until the ";" symbol
     *                    ex. "ORDERBY <insert column here>"
     *
     * @param tableName the name of the table you are applying this operation to
     * @return the name of the temp table with the ordering finished
     */
    public static String parseOrderBy(String orderByPart, String tableName) throws Exception {
        TableSchema ordered = executeOrderBy(tableName, orderByPart);
        return ordered.tableName;
    }

    /**
     * The parse select handles projection.
     * If the selectPart == *: it simply returns the original name, because we select all queries
     *
     * If not, it creates a copy of the table, only including the selected columns from the table
     *
     * @param selectPart a parsed substring of a sql query
     *                   beginning after the "SELECT" and ending before the "FROM"
     *                   ex. "*", "id, name, gpa", "students.id, takes.id"
     *
     * @return the table name where the results of the operation were stored
     */
    public static String parseSelect(String selectPart, String tableName) throws Exception {
        if (selectPart.equals("*")) {
            return tableName;
        }

        BufferManager bm = BufferManager.getInstance();
        DataCatalog dataCatalog = DataCatalog.getInstance();
        TableSchema tableSchema = dataCatalog.getTableSchema(tableName);



        selectPart = selectPart.replace(" ", "");
        Set<String> requestedAttributes = new HashSet<>(Arrays.asList(selectPart.split(",")));
        ArrayList<Integer> selectedIndices = new ArrayList<>();

        ArrayList<AttributeSchema> existingAttributes = new ArrayList<>(tableSchema.getAttributeSchemas().sequencedValues());

        for (int i = 0; i < existingAttributes.size(); i++) {
            if (requestedAttributes.contains(existingAttributes.get(i).attributeName)) {
                // add it to the copied table schema
                selectedIndices.add(i);
            }
        }

        // with the given selected column indices, make a temp copy table

        TableSchema copy = tableSchema.makeTempCopy(selectedIndices);
        bm.createNewPage(copy.rootPageID, copy.tableName);
        // take all records from original table and make copies of the records
        int pageID = tableSchema.getRootPageID();
        while (pageID != -1) {
            Page p = bm.getPage(pageID);


            for (Record r : p.recordList) {
                // create new modified records
                Record temp = new Record();
                for (Integer index : selectedIndices) {
                    temp.attributeList.add(r.attributeList.get(index));
                }
                // insert into copied table pages
                bm.insertRecordIntoTable(copy.tableName, temp);
            }

            pageID = p.nextPageId;
        }

            return copy.tableName;

    }

}
