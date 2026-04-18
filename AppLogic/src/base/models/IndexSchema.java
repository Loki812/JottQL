package base.models;

import base.buffer.BufferManager;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IndexSchema {

    public int rootPageID;
    public String tableName;
    public String columnName;
    public int n;

    private static final int POINTER_SIZE = 4;
    public IndexSchema(String tableName, String columnName) throws Exception {

        this.tableName = tableName;
        this.columnName = columnName;

        int pageSize = DataCatalog.getInstance().getPageSize();
        n = find_n(pageSize);

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

        out.writeInt(tableName.getBytes(StandardCharsets.UTF_8).length);
        byte[] tableBytes = tableName.getBytes(StandardCharsets.UTF_8);
        out.write(tableBytes);

        out.writeInt(columnName.getBytes(StandardCharsets.UTF_8).length);
        byte[] columnBytes = columnName.getBytes(StandardCharsets.UTF_8);
        out.write(columnBytes);

        out.writeInt(rootPageID);
        out.writeInt(n);

    }

    public static IndexSchema createIndexSchemaFromDisk(DataInputStream in) throws IOException {

        IndexSchema idx = new IndexSchema();

        byte[] tableBytes = new byte[in.readInt()];
        in.readFully(tableBytes);
        idx.tableName = new String(tableBytes, StandardCharsets.UTF_8);

        byte[] columnBytes = new byte[in.readInt()];
        in.readFully(columnBytes);
        idx.columnName = new String(columnBytes, StandardCharsets.UTF_8);

        idx.rootPageID = in.readInt();
        idx.n = in.readInt();

        return idx;

    }

}