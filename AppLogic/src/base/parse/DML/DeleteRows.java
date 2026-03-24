package base.parse.DML;

import base.models.DataCatalog;
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
        // Get an instance of the DataCatalog
        DataCatalog dc = DataCatalog.getInstance();

        // Get the tableSchema and make a copy of it
        TableSchema tableSchema = dc.getTableSchema(tableName);
        TableSchema copy = tableSchema; //tableSchema.makeTempCopy();

        return copy;
    }

}
