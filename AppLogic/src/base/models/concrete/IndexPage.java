package base.models.concrete;

import base.buffer.BufferManager;
import base.models.DataCatalog;
import base.models.schemas.IndexSchema;
import base.models.schemas.AttributeSchema;
import base.models.schemas.DataTypes;
import base.models.schemas.InsertionResult;
import base.models.schemas.TableSchema;

import java.awt.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class IndexPage implements Ipage {

    public String tableName;
    public int pageId;
    public int parentPageId;
    public int nextPageId;
    public boolean isRoot;
    public boolean isLeaf;
    public int n;
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
    public IndexPage(int pageId, String tableName, AttributeSchema searchKey, int parentPageId) {
        DataCatalog dc = DataCatalog.getInstance();
        IndexSchema is = dc.getIndexSchema(tableName, searchKey.attributeName);
        this.tableName = tableName;
        this.searchKey = searchKey;
        this.pageId = pageId;
        this.nextPageId = -1;
        this.isRoot = false;
        this.isLeaf = false;
        this.n = is.n;
        //this.n = 3;
        this.parentPageId = parentPageId;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
        this.childPointers = new ArrayList<>();
        this.searchKeys = new ArrayList<>();
    }

    /**
     * Builds a IndexPage from a byteArray passed from buffermanager, to ensure proper data.
     * The byte array should be structured as such
     *
     * -- Page Type Indicator (4 bytes)
     * -- Parent Page ID (4 bytes)
     * -- Next Page ID (4 bytes)
     * -- is Root (1 byte)
     * -- is Leaf (1 byte)
     * -- Table Name Length (4 bytes)
     * -- Table Name (variable length)
     * -- Key Name Length (4 bytes)
     * -- Key Name (variable length)
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
        this.nextPageId = buffer.getInt();
        this.isRoot = buffer.get() == 1;
        this.isLeaf = buffer.get() == 1;

        int tableNameLength = buffer.getInt();
        byte[] tableNameBytes = new byte[tableNameLength];
        buffer.get(tableNameBytes);
        this.tableName = new String(tableNameBytes, StandardCharsets.UTF_8);

        int searchKeyLength = buffer.getInt();
        byte[] SearchKeyBytes = new byte[searchKeyLength];
        buffer.get(SearchKeyBytes);
        String searchKeyName = new String(SearchKeyBytes, StandardCharsets.UTF_8);
        this.searchKey = dc.getTableSchema(this.tableName).getAttributeSchemas().get(searchKeyName);

        this.n = dc.getIndexSchema(this.tableName, searchKeyName).n;
        //this.n = 3;

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
        byteBuffer.putInt(nextPageId);
        byteBuffer.put((byte) (isRoot ? 1 : 0));
        byteBuffer.put((byte) (isLeaf ? 1 : 0));

        byteBuffer.putInt(tableName.length());
        byte[] tableNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
        byteBuffer.put(tableNameBytes);

        byteBuffer.putInt(searchKey.attributeName.length());
        byte[] searchKeyName = this.searchKey.attributeName.getBytes(StandardCharsets.UTF_8);
        byteBuffer.put(searchKeyName);

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
        // parent page ID, nextPageID, pageTypeIndicator, number of searchKeys, num of children, tableName length, search key length
        accum += Integer.BYTES * 7;
        accum += 2; // bool isLeaf, isRoot
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

    public InsertionResult tryInsert(Record record, TableSchema schema, Boolean duplicates) {
        timestamp = java.time.LocalDateTime.now();
        if(isLeaf){
            return tryInsertLeaf(record, schema, 0);
        }else {

            int attributeIndex = schema.getIndex(searchKey.attributeName);
            int i = getIndex(record, attributeIndex);
            Ipage child = BufferManager.getInstance().getPageV2(childPointers.get(i));
            InsertionResult result = child.tryInsert(record, schema, false);
            switch (result){
                case SUCCESS:
                    break;
                case NEEDS_SPLIT:
                    BufferManager bm = BufferManager.getInstance();
                    int newID = child.split();
                    Ipage newChild = bm.getPageV2(newID);
                    bm.getPageV2(this.pageId);
                    childPointers.add(i+1, newID);
                    searchKeys.add(i, newChild.getFirst(attributeIndex));
                    timestamp = java.time.LocalDateTime.now();
                    hasBeenModified = true;
                    break;
            }
        }
        if(childPointers.size() > n){
            return InsertionResult.NEEDS_SPLIT;
        }else{
            return InsertionResult.SUCCESS;
        }
    }

    public InsertionResult tryInsertLeaf(Record record, TableSchema schema, int offset) {
        BufferManager bm = BufferManager.getInstance();
        timestamp = java.time.LocalDateTime.now();
        int attributeIndex = schema.getIndex(this.searchKey.attributeName);
        int i = getIndex(record, attributeIndex);
        if(this.searchKey.attributeName.equals(schema.primaryKey)){
            Ipage child;
            if(i+offset >= childPointers.size()){
                IndexPage nextPage = (IndexPage) bm.getPageV2(nextPageId);
                child = bm.getPageV2(nextPage.childPointers.get(1));
                offset+=1;
                if(offset>n){
                    throw new RuntimeException();
                }
            }else {
                child = bm.getPageV2(childPointers.get(i + offset));
            }
            InsertionResult result = child.tryInsert(record, schema, false);
            switch (result){
                case SUCCESS:
                    hasBeenModified = true;
                    searchKeys.add(i, record.attributeList.get(attributeIndex));
                    childPointers.add(i+1, child.getPageId());
                    break;
                case NEEDS_SPLIT:
                    int newID = child.split();
                    IndexSchema Ischema = DataCatalog.getInstance().getIndexSchema(tableName, searchKey.attributeName);
                    Page newChild = (Page) bm.getPageV2(newID);
                    ArrayList<Record> records = newChild.recordList;
                    for(Record r : records) {
                        IndexPage root = (IndexPage) bm.getPageV2(Ischema.rootPageID);
                        root.update(r, schema, newID);
                    }
                    hasBeenModified = true;
                    tryInsertLeaf(record, schema, 0);
                    break;
                case NOT_IN_RANGE:
                    tryInsertLeaf(record, schema, offset+1);

            }
        }else{
            searchKeys.add(i, record.attributeList.get(attributeIndex));
        }
        if(childPointers.size() > n-1){
            return InsertionResult.NEEDS_SPLIT;
        }else{
            return InsertionResult.SUCCESS;
        }
    }

    private int getIndex(Record record, int attributeIndex) {
        int i = 0;
        while (i < searchKeys.size() && record.attributeList.get(attributeIndex).compareTo(searchKeys.get(i))>-1) {
            if(record.attributeList.get(attributeIndex).compareTo(searchKeys.get(i))==0){
                throw new RuntimeException("Unique attribute cannot have Duplicates");
            }
            i++;
        }
        return i;
    }

    /**
     * @param record record to be updated
     * @param newPageId page value is in after split
     *
     * Only call on the root to update the values of the tree after
     * a data page split
     */
    public void update(Record record, TableSchema schema, int newPageId){
        int attributeIndex = schema.getIndex(this.searchKey.attributeName);
        int i = 0;
        while (i < searchKeys.size() && record.attributeList.get(attributeIndex).compareTo(searchKeys.get(i))>-1) {
            i++;
        }
        BufferManager bm = BufferManager.getInstance();
        if(isLeaf){
            childPointers.set(i, newPageId);
            if(i == 1){
                childPointers.set(0, newPageId);
            }
        }else{
            IndexPage child = (IndexPage)bm.getPageV2(childPointers.get(i));
            child.update(record, schema, newPageId);
        }
    }


    //return the next page id
    public int split(){

        BufferManager bm = BufferManager.getInstance();
        DataCatalog catalog = DataCatalog.getInstance();

        int midIndex = n/2;
        int newPageId = catalog.getNextAvailablePageID();
        bm.createNewIndexPage(newPageId, this.tableName, this.searchKey, this.parentPageId);
        IndexPage newPageNode = (IndexPage) bm.getPageV2(newPageId);

        // Move half the keys to the new leaf node
        newPageNode.searchKeys.addAll(this.searchKeys.subList(midIndex, this.searchKeys.size()));
        this.searchKeys = new ArrayList<>(this.searchKeys.subList(0, midIndex));
        newPageNode.nextPageId = this.nextPageId;
        this.nextPageId = newPageId;
        if(isLeaf){
            newPageNode.isLeaf = true;
            if(this.searchKey.attributeName.equals(catalog.getTableSchema(tableName).primaryKey)){
                // Move half the children to the new leaf node
                newPageNode.childPointers.addAll(this.childPointers.subList(midIndex,childPointers.size()));
                this.childPointers = new ArrayList<>(this.childPointers.subList(0, midIndex+1));
            }
        } else {
            // Move half the children to the new node
            newPageNode.childPointers.addAll(this.childPointers.subList(midIndex+1,childPointers.size()));
            this.childPointers = new ArrayList<>(this.childPointers.subList(0, midIndex+1));
        }


        // If the root splits, create a new root
        if (isRoot) {
            int newRootId = catalog.getNextAvailablePageID();
            bm.createNewIndexPage(newRootId, this.tableName, this.searchKey, this.parentPageId);
            IndexPage newRoot = (IndexPage) bm.getPageV2(newRootId);

            newRoot.childPointers.add(this.pageId);
            newRoot.childPointers.add(newPageNode.pageId);
            newRoot.searchKeys.add(newPageNode.getFirst(0));
            newRoot.isRoot = true;
            isRoot = false;

            IndexSchema schema = catalog.getIndexSchema(tableName,searchKey.attributeName);
            schema.rootPageID=newRootId;




        }

        hasBeenModified = true;
        newPageNode.hasBeenModified = true;
        timestamp = java.time.LocalDateTime.now();
        newPageNode.timestamp = java.time.LocalDateTime.now();



        return newPageNode.pageId;

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
    public AttributeValue<?> getFirst(int i) {
        timestamp = java.time.LocalDateTime.now();
        if(isLeaf){
            return this.searchKeys.getFirst();
        }
        hasBeenModified = true;
        return this.searchKeys.removeFirst();
    }

    @Override
    public void changeTableName(String newTableName) {
        tableName = newTableName;

        BufferManager bm = BufferManager.getInstance();

        for (int childPointer : childPointers) {
            Ipage child = bm.getPageV2(childPointer);
            child.changeTableName(newTableName);
        }
    }

    // DO NOT CALL ON INDEX PAGE
    public int nextPageId(){
        return -1;
    }

}
