package base.parse.DML;

import base.buffer.BufferManager;
import base.models.AttributeSchema;
import base.models.DataCatalog;
import base.models.Page;
import base.models.Record;
import base.models.TableSchema;
import java.util.ArrayList;
import java.util.List;

/**
 * Sort results by a single attribute.
 */
public class OrderBy {

    /**
     * Executes an ORDER BY clause in an SQL statement
     *
     * @param tableName the name of the table the order by is being performed on
     * @param orderByAttr the name of the attribute that the table is being ordered by
     * @return a reordered copy of the table schema
     * @throws Exception if the orderByAttr doesn't exist in the table
     */
    public static TableSchema executeOrderBy(String tableName, String orderByAttr) throws Exception {
        // Get instances of the DataCatalog and BufferManager
        DataCatalog dc = DataCatalog.getInstance();
        BufferManager bm = BufferManager.getInstance();

        // Duplicate the table schema
        TableSchema tableSchema = dc.getTableSchema(tableName);
        TableSchema copy = tableSchema.makeTempCopy(new ArrayList<>());

        // Try exact match first, then try suffix match (e.g. "a" matches "t1.a")
        AttributeSchema orderBySchema = copy.getAttributeSchemas().get(orderByAttr);
        if (orderBySchema == null) {
            List<String> matches = new ArrayList<>();
            for (String attr : copy.getAttributeSchemas().keySet()) {
                if (attr.endsWith(orderByAttr)) {
                    matches.add(attr);
                }
            }

            if (matches.size() == 1) {
                orderByAttr = matches.getFirst();
            } else if (matches.size() > 1) {
                throw new Exception("ORDER BY column '" + orderByAttr + "' is ambiguous: " + matches);
            } else {
                throw new Exception("ORDER BY column '" + orderByAttr + "' does not exist in table '" + tableName + "'");
            }
        }

        // Set the new copy's primary key to the order by attribute
        copy.primaryKey = orderByAttr;

        // Go through the table's pages and insert each record into the table
        int pageId = tableSchema.rootPageID;
        while (pageId != -1) {
            Page page = bm.getPage(pageId);
            for (Record r : page.recordList) {
                bm.insertRecordIntoTableAllowDuplicates(copy.tableName, r);
            }

            pageId = page.nextPageId;
        }

        return copy;
    }
}
