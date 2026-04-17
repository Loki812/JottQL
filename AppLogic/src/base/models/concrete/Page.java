package base.models.concrete;

import base.buffer.BufferManager;
import base.models.DataCatalog;
import base.models.schemas.AttributeSchema;
import base.models.schemas.InsertionResult;
import base.models.schemas.TableSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class Page implements Ipage {

    public String tableName;
    public int pageId;
    public int nextPageId;
    public boolean hasBeenModified;
    public LocalDateTime timestamp;
    public ArrayList<Record> recordList;


    /**
     * Basic constructor for when creating a new page, no records added
     * @param pageId the id of the page
     * @param tableName the name of the table it is associated with
     */
    public Page(int pageId, String tableName){
        this.pageId = pageId;
        this.tableName = tableName;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
        this.nextPageId = -1;
        recordList = new ArrayList<>();
    }

    /**
     * Given a byte array, converts the byte array into an instantiated java object.
     * NOTE: Assumes the byte array received is a DataPage. If it is an index page,
     * will result in junk data without an error being thrown
     *
     * @param pageId the id of the page you are grabbing
     * @param bytes the byte array containing binary information on the page
     */
    public Page(int pageId, byte[] bytes) {

        DataCatalog dc = DataCatalog.getInstance();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // need to pass the DATA_PAGE_IND in header, we don't need it
        // already inside of Data Page Constructor
        buffer.getInt();

        // initialize main fields
        this.pageId = pageId;
        this.nextPageId = buffer.getInt();
        this.hasBeenModified = false;
        this.timestamp = LocalDateTime.now();
        this.recordList = new ArrayList<>();

        int tableNameLength = buffer.getInt();
        byte[] tableNameBytes = new byte[tableNameLength];
        buffer.get(tableNameBytes);
        int numOfRecords = buffer.getInt();

        this.tableName = new String(tableNameBytes, StandardCharsets.UTF_8);

        ArrayList<AttributeSchema> attributes = new ArrayList<>(dc.getTableSchema(tableName)
                .getAttributeSchemas().sequencedValues());

        // add the records to the page object
        for (int i = 0; i < numOfRecords; i++) {
            Record record = new Record(buffer, attributes);
            this.recordList.add(record);
        }
    }


    public ByteBuffer toBytes() {

        DataCatalog dc = DataCatalog.getInstance();

        ByteBuffer byteBuffer = ByteBuffer.allocate(dc.getPageSize());

        byteBuffer.putInt(DATA_PAGE_IND); // LETS US KNOW WHEN READING PAGE THAT IT IS A DATA PAGE

        byteBuffer.putInt(this.nextPageId);
        byte[] tableNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
        byteBuffer.putInt(tableNameBytes.length);
        byteBuffer.put(tableNameBytes);

        // put in number of records, used for reading from hardware for loop counter
        byteBuffer.putInt(this.recordList.size());

        ArrayList<AttributeSchema> attributes = new ArrayList<>(dc.getTableSchema(this.tableName)
                .getAttributeSchemas().sequencedValues());

        for (Record r : this.recordList) {
            r.toBytes(byteBuffer, attributes);
        }

        byteBuffer.flip(); // resets position, clips length. prepares for it to be sent to storage manager
        return byteBuffer;
    }

    /**
     * tryInsert attempts to insert the record into the given page.
     * THIS FUNCTION ASSUMES LINEAR SEARCH AND PAGES WITH SMALLER VALUES HAVE BEEN ALREADY CHECKED
     * If this is the last page within a table, it inserts no matter what.
     * lets the caller know if the page needs to be split.
     *
     * @param record the record you are attempting to insert
     * @param schema the schema you are using for reference
     * @param duplicates if the table allows for duplicate primary key
     * @return SUCCESS if record is inserted, NOT_IN_RANGE if record primary key is not in range of page
     * NEEDS_SPLIT if the record is in range, but the page is full.
     */
    public InsertionResult tryInsert(Record record, TableSchema schema, Boolean duplicates) {
        // 1. Check if record is in range of page

        //todo find the primary key and call compareTo on that
        if(schema.primaryKey != null){
            Integer primaryKeyIndex = schema.getIndex(schema.primaryKey);
            if(primaryKeyIndex!=null){
                if (!recordList.isEmpty() && record.compareTo(recordList.getLast(), primaryKeyIndex) > 0) {
                    // If we are not on the last page, send signal to buffer manager to iterate to next page
                    if (nextPageId != -1) return InsertionResult.NOT_IN_RANGE;
                }
            }
        }


        // 2. Check if record fits in page
        int pageSize = DataCatalog.getInstance().getPageSize();
        if (getPageSize() + record.getSize() > pageSize) {
            return InsertionResult.NEEDS_SPLIT;
        }

        // 3. Insert into page if both of those pass
        insert(record, schema, true, duplicates);
        this.hasBeenModified = true;
        this.timestamp = java.time.LocalDateTime.now();
        return InsertionResult.SUCCESS;

    }

    /**
     * attempts to insert a page with no specfied order
     * NOTE: the insert on this does allow for duplicates
     * if you have a scenario where you dont want order but you also dont want duplicates,
     * please pass a DUPLICATES_ALLOWED variable to this function by editing the code
     *
     *
     * @param record the record you are attempting to insert into the page
     * @param schema the schema corresponding to the page
     * @return the result of the attempted insert
     */
    public InsertionResult tryInsertNoOrder(Record record, TableSchema schema) {
        int pageSize = DataCatalog.getInstance().getPageSize();
        if (getPageSize() + record.getSize() > pageSize) {
            if (nextPageId == -1) {
                return InsertionResult.NEEDS_SPLIT;
            }
            // cant fit on page, but we are not on the last page so do not split, iterate to next one
            return InsertionResult.NOT_IN_RANGE;
        }

        insert(record, schema, false, true);
        this.hasBeenModified = true;
        this.timestamp = java.time.LocalDateTime.now();
        return InsertionResult.SUCCESS;
    }


    /**
     * insert maintains record order on a page, inserting by linear search and using record.compareTO
     * @param record the given record you are inserting
     * @param schema the schema you are using as reference
     * @param ORDERED boolean determining if you are inserting ordered or not
     * @param DUPLICATES_ALLOWED boolean determining if you are allowing duplicate primary keys
     */
    public void insert(Record record, TableSchema schema, boolean ORDERED, boolean DUPLICATES_ALLOWED) {
        // check for duplicates while iterating over records,
        // if ORDERED, then insert while iterating
        // if unordered, after iterating over every record, insert at end of page
        if(!ORDERED){
            recordList.add(record);
            return;
        }
        for (int i = 0; i < recordList.size(); i++) {


            if(schema.primaryKey != null){
                Integer primaryKeyIndex = schema.getIndex(schema.primaryKey);
                if(primaryKeyIndex != null){
                    //todo find the primary key and call compareTo on that
                    if(record.compareTo(recordList.get(i), primaryKeyIndex) == 0 && !DUPLICATES_ALLOWED){
                        throw new RuntimeException("Duplicate primary key found while attempting insertion...");
                    }
                    //todo find the primary key and call compareTo on that
                    if (record.compareTo(recordList.get(i), primaryKeyIndex) <= 0) {
                        recordList.add(i, record);
                        return;
                    }
                }
            }


        }

        // did not run into exception, either greater than all records on page or is unordered insertion
        recordList.add(record);
        this.hasBeenModified = true;
        this.timestamp = java.time.LocalDateTime.now();
    }


    /**
     * deletes a column for this page and THIS PAGE only
     *
     * @param index the index of the given column
     */
    public void deleteColumn(int index){
        for(Record record : recordList){
            record.attributeList.remove(index);
        }
        hasBeenModified = true;
        timestamp = LocalDateTime.now();
    }

    /**
     * adds a column for this page and this page ONLY
     * @param defaultValue the default value being added to the page
     */
    public void addColumn(AttributeValue<?> defaultValue) {
        for (Record r : recordList) {
            r.attributeList.add(defaultValue);
        }
        hasBeenModified = true;
        timestamp = LocalDateTime.now();
    }


    /**
     * calculates the pages size on the fly by accumulating the size of each record.
     * @return the total size of the page
     */
    public int getPageSize() {
        int accum = 0;
        accum += Integer.BYTES * 4; // pageTypeIndicator, pageId, nextPageID, length of tableName
        accum += tableName.length();
        for (Record r : recordList) {
            accum += r.getSize();
        }
        return accum;
    }

    //-----------
    // Getters, setters, here
    //-----------
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


    public Ipage split(){
        BufferManager bm = BufferManager.getInstance();
        int page2ID = DataCatalog.getInstance().getNextAvailablePageID();
        bm.createNewDataPage(page2ID, tableName);
        // Link pages in correct order page -> page.nextPage goes to page -> page2 -> page.nextPage
        Page page2 = (Page) bm.getPageV2(page2ID);
        page2.nextPageId = nextPageId;
        nextPageId = page2ID;

        // Give Each Page half of the records
        int mid = recordList.size() / 2;
        ArrayList<Record> firstHalf = new ArrayList<>(recordList.subList(0, mid));
        ArrayList<Record> secondHalf = new ArrayList<>(recordList.subList(mid, recordList.size()));
        recordList = firstHalf;
        page2.recordList = secondHalf;

        hasBeenModified = true;
        page2.hasBeenModified = true;
        timestamp = java.time.LocalDateTime.now();
        page2.timestamp = java.time.LocalDateTime.now();

        return page2;
    }
}
