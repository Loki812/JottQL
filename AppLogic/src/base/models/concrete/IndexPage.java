package base.models.concrete;

import base.models.DataCatalog;
import base.models.schemas.AttributeSchema;
import base.models.schemas.DataTypes;
import base.models.schemas.InsertionResult;
import base.models.schemas.TableSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class IndexPage implements Ipage {

    public String tableName;
    public int pageId;
    public int parentPageId;
    public boolean hasBeenModified;
    public LocalDateTime timestamp;
    public AttributeSchema searchKey;
    // values to compare to during insertion
    public ArrayList<AttributeValue> searchKeys;
    // if it is an internal node, child pointers go down a level on the
    public ArrayList<Integer> childPointers;

    /**
     * Constructor for a new index page not written to disk yet.
     *
     * @param parentPageId the ID of the page that is the parent. If there is no parentPage
     *                     parentPageId should explicitly be set to -1.
     * @param pageId the ID of the given page
     * @param tableName the name of the table this is indexed for
     */
    public IndexPage(int pageId, String tableName, int parentPageId) {
        DataCatalog dc = DataCatalog.getInstance();
        TableSchema ts = dc.getTableSchema(tableName);

        this.tableName = tableName;
        this.pageId = pageId;
        this.parentPageId = parentPageId;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
        this.childPointers = new ArrayList<>();
        this.searchKeys = new ArrayList<>();
        this.searchKey = ts.getAttributeSchemas().get(ts.primaryKey);
    }

    /**
     * Builds a IndexPage from a byteArray passed from buffermanager, to ensure proper data.
     * The byte array should be structured as such
     *
     * -- Page Type Indicator (4 bytes)
     * -- Parent Page ID (4 bytes)
     * -- Table Name Length (4 bytes)
     * -- Table Name (variable length)
     * -- number of searchKeyValues (4 bytes)
     * -- number of childPagePointers (4 bytes)
     * -- searchKeyValues (variable length)
     * -- childPagePointers (4 bytes * number of children)
     *
     * @param pageId the page you are attempting to grab
     * @param bytes the byte array passed from buffer manager
     */
    public IndexPage(int pageId, byte[] bytes) {
        DataCatalog dc = DataCatalog.getInstance();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // need to pass the INDEX_PAGE_IND in header, we don't need it
        // we are already inside of Index Page Constructor
        buffer.getInt();

        this.pageId = pageId;
        this.hasBeenModified = false;
        this.timestamp = LocalDateTime.now();
        this.searchKeys = new ArrayList<>();
        this.childPointers = new ArrayList<>();

        this.parentPageId = buffer.getInt();
        int tableNameLength = buffer.getInt();
        byte[] tableNameBytes = new byte[tableNameLength];
        buffer.get(tableNameBytes);
        this.tableName = new String(tableNameBytes, StandardCharsets.UTF_8);

        TableSchema ts = dc.getTableSchema(this.tableName);
        this.searchKey = ts.getAttributeSchemas().get(ts.primaryKey);

        int numberOfSearchKeys = buffer.getInt();
        int numberOfChildPointers = buffer.getInt();

        DataTypes dt = searchKey.getDataType();
        // load in search keys
        for (int i = 0; i < numberOfSearchKeys; i++) {
            Object value = switch (dt) {
                case INTEGER -> buffer.getInt();
                case DOUBLE -> buffer.getDouble();
                case BOOLEAN -> buffer.get() == 1; // this should never be used as a primary key lol
                case CHAR, VARCHAR -> {
                    byte[] sBytes = new byte[searchKey.getLength()];
                    buffer.get(sBytes);
                    yield new String(sBytes, StandardCharsets.UTF_8);
                }
            };
            searchKeys.add(new AttributeValue(value, searchKey.getDataType()));
        }

        // load in child pointers
        for (int i = 0; i < numberOfChildPointers; i++) {
            this.childPointers.add(buffer.getInt());
        }

    }

    /**
     *
     * @return a byte buffer of the format specified in the above constructor
     */
    public ByteBuffer toBytes() {
        DataCatalog dc = DataCatalog.getInstance();

        ByteBuffer byteBuffer = ByteBuffer.allocate(dc.getPageSize());

        byteBuffer.putInt(INDEX_PAGE_IND);

        byteBuffer.putInt(parentPageId);
        byteBuffer.putInt(tableName.length());
        byte[] tableNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
        byteBuffer.put(tableNameBytes);

        byteBuffer.putInt(searchKeys.size());
        byteBuffer.putInt(childPointers.size());

        for (AttributeValue v : searchKeys) {
            switch (searchKey.getDataType()) {
                case INTEGER -> byteBuffer.putInt((int) v.data);
                case DOUBLE -> byteBuffer.putDouble((double) v.data);
                case BOOLEAN -> byteBuffer.put((byte) ((boolean) v.data ? 1 : 0));
                case CHAR, VARCHAR -> {
                    String s = (String) v.data;
                    byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
                    byteBuffer.put(strBytes);

                    // add padding so all searchKeyValues are the same length
                    if (strBytes.length < searchKey.getLength()) {
                        int padding = searchKey.getLength() - strBytes.length;
                        byte[] paddedBytes = new byte[padding];
                        byteBuffer.put(paddedBytes);
                    }
                }
            }
        }

        for (int childPointer : childPointers) {
            byteBuffer.putInt(childPointer);
        }

        byteBuffer.flip();
        return byteBuffer;
    }

    public int getPageSize() {
        int accum = 0;
        accum += Integer.BYTES * 5; // parent page ID, pageTypeIndicator, number of searchkeys, num of children, tablename length
        accum += Integer.BYTES * childPointers.size();
        int searchKeyValuesSize = switch (searchKey.getDataType()) {
            case INTEGER -> Integer.BYTES * searchKeys.size();
            case DOUBLE -> Double.BYTES * searchKeys.size();
            case BOOLEAN -> searchKeys.size();
            case CHAR, VARCHAR -> searchKey.getLength() * searchKeys.size();
        };
        accum += searchKeyValuesSize;

        return accum;
    }

    /**
     *
     * @param record grab primary key from here to insert into B+ tree
     * @param ts the corresponding table schema
     * @param ORDERED ignored, used to match function signature on data page
     * @param DUPLICATES_ALLOWED ignored, duplicates are allowed on leaf nodes, not on internals
     */
    public void insert(Record record, TableSchema ts, boolean ORDERED, boolean DUPLICATES_ALLOWED) {

    }

    //----------
    // Getters, setters here
    //----------
    public void setTimeStamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public LocalDateTime getTimeStamp() {return this.timestamp;}

    public int getPageId() {
        return pageId;
    }

    public boolean getHasBeenModified() {
        return hasBeenModified;
    }

    @Override
    public InsertionResult tryInsert(Record record, TableSchema ts, boolean ORDERED, boolean DUPLICATES_ALLOWED) {
        //todo make this do something
        return null;
    }
}
