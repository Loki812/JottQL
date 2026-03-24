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
     * Delete rows from a table.
     *
     * @param tableName the name of the table to delete rows from
     * @param whereTreeNode which rows to delete from the table
     * @return the TableSchema with rows deleted
     */
    public TableSchema delete(String tableName, WhereTreeNode whereTreeNode) {
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

        // TODO: Drop tableSchema

        // Remove the copy's name from the list of temporary table names so it doesn't get deleted
        dc.tempTableNames.remove(copy.tableName);

        // Change the copy's name to the original tableSchema's name
        copy.tableName = name;

        // Return the copy
        return copy;
    }
}
