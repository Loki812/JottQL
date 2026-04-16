package base.models;

import base.buffer.BufferManager;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IndexSchema {

    public int rootPageID;
    public String tableName;
    public String columnName;
    public int n;

    private static final int POINTER_SIZE = 4;
    public IndexSchema(String tableName, String columnName) throws Exception {

        this.tableName = tableName;
        this.columnName = columnName;
        DataCatalog catalog = DataCatalog.getInstance();
        rootPageID = catalog.getNextAvailablePageID();
        int pageSize = catalog.getPageSize();
        n = find_n(pageSize);
        BufferManager bm = BufferManager.getInstance();
        String indexTableName = tableName + "_idx_" + columnName;
        bm.createNewPage(rootPageID, indexTableName);

    }

    private IndexSchema(){}
    private int find_n(int pageSize) throws Exception {

        TableSchema tableSchema = DataCatalog.getInstance().getTableSchema(tableName);
        if (tableSchema == null) {

            throw new Exception("Table not found: " + tableName);

        }

        AttributeSchema attr = tableSchema.getAttributeSchemas().get(columnName);
        if (attr == null) {

            throw new Exception("Column not found: " + columnName);

        }

        int keySize = attr.getLength();
        int headerSize = getNodeHeaderSize();
        int pairSize = POINTER_SIZE + keySize;
        if (pairSize <= 0) {

            throw new Exception("Invalid index entry size");

        }

        int calculatedN = (pageSize - headerSize) / pairSize;

        if (calculatedN < 1) {

            throw new Exception( "Page size too small for index entries");

        }

        return calculatedN;

    }


    private int getNodeHeaderSize() {

        return 12;

    }

    public void saveIndexSchemaToDisk(DataOutputStream out) throws IOException {

        out.writeInt(tableName.length());
        out.writeBytes(tableName);
        out.writeInt(columnName.length());
        out.writeBytes(columnName);
        out.writeInt(rootPageID);
        out.writeInt(n);

    }

    public static IndexSchema createIndexSchemaFromDisk(DataInputStream in) throws IOException {

        IndexSchema idx = new IndexSchema();
        byte[] tableBytes = new byte[in.readInt()];
        in.readFully(tableBytes);
        idx.tableName = new String(tableBytes);
        byte[] columnBytes = new byte[in.readInt()];
        in.readFully(columnBytes);
        idx.columnName = new String(columnBytes);
        idx.rootPageID = in.readInt();
        idx.n = in.readInt();
        return idx;

    }

}