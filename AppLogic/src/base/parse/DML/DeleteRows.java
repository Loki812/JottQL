package base.parse.DML;

import base.buffer.BufferManager;
import base.models.DataCatalog;
import base.models.Page;
import base.models.Record;
import base.models.TableSchema;
import base.models.whereNodes.WhereTreeNode;

/**
 * Delete rows from a table.
 */
public class DeleteRows {

    /**
     * Parse an SQL DELETE query and call the deleteRows function.
     *
     * @param command the command to be parsed
     * @throws Exception if the command is invalid
     */
    public static void execute(String command) throws Exception {
        // If the command doesn't start with DELETE FROM or end with a semicolon, it's invalid
        String trimmedCommand = command.trim();
        if (!trimmedCommand.startsWith("DELETE FROM ")) {
            System.out.println("Invalid DELETE Command");
            throw new Exception();
        }
        if (!trimmedCommand.endsWith(";")) {
            System.out.println("Missing ';'");
            throw new Exception();
        }

        // Everything in the command after the DELETE FROM clause
        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        String afterDeleteFrom = trimmedCommand.substring("DELETE FROM".length()).trim();
        int firstSpace = afterDeleteFrom.indexOf(' ');

        String tableName;
        WhereTreeNode whereTreeNode = null;

        // If there is no WHERE clause, afterDeleteFrom is just the tableName
        if (firstSpace == -1) {
            tableName = afterDeleteFrom;
        } else {
            tableName = afterDeleteFrom.substring(0, firstSpace).trim();
            String afterTableName = afterDeleteFrom.substring(firstSpace + 1).trim();

            if (afterTableName.startsWith("WHERE ")) {
                String whereClause = afterTableName.substring("WHERE".length()).trim();
                //TODO: whereTreeNode = ???;
            } else {
                System.out.println("Invalid DELETE Command");
                throw new Exception();
            }
        }

        delete(tableName, whereTreeNode);
    }

    /**
     * Delete rows from a table.
     *
     * @param tableName the name of the table to delete rows from
     * @param whereTreeNode which rows to delete from the table
     * @return the TableSchema with rows deleted
     */
    private static TableSchema delete(String tableName, WhereTreeNode whereTreeNode) {
        // Get instances of the DataCatalog and BufferManager
        DataCatalog dc = DataCatalog.getInstance();
        BufferManager bm = BufferManager.getInstance();

        // Get the tableSchema and make a copy of it
        TableSchema tableSchema = dc.getTableSchema(tableName);
        TableSchema copy = tableSchema.makeTempCopy();

        // Get the tableSchema's name
        String name = tableSchema.tableName;

        // Go through the table's pages and insert non-deleted rows into the copy
        int pageId = tableSchema.rootPageID;
        while (pageId != -1) {
            Page page = bm.getPage(pageId);
            for (Record r : page.recordList) {
                // If the where tree node isn't evaluating this record, insert it into the copy
                if (!whereTreeNode.eval(r, tableSchema)) {
                    bm.insertRecordIntoTable(copy.tableName, r);
                }
            }

            pageId = page.nextPageId;
        }

        // Drop the original tableSchema
        dc.removeTableSchema(tableName);

        // Remove the copy's name from the list of temporary table names so it doesn't get deleted
        dc.tempTableNames.remove(copy.tableName);

        // Change the copy's name to the original tableSchema's name
        copy.tableName = name;

        // Return the copy
        return copy;
    }
}
