package base.buffer;
import base.models.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import base.models.Record;
import base.storage.StorageManager;

public class BufferManager {

    private static BufferManager instance = null;

    // singleton variables
    private final int maxPageCount;
    private final HashMap<Integer,Page> buffer;
    private final StorageManager storageManager;


    private final DataCatalog dataCatalog;



    /**
     * Creates a singlton instance of the bufferManager class
     *
     *
     * @param maxPageCount the maximum number of pages that can be stored in memory, as Java Objects
     *                     at one time.
     * @param directory the directory the storage.bin file is stored in.
     */
    private BufferManager(int maxPageCount, String directory)  {
        this.maxPageCount = maxPageCount;
        this.storageManager = StorageManager.buildStorageManager(directory);
        buffer = new HashMap<>();
        dataCatalog = DataCatalog.getInstance();
    }

    /**
     *
     * @param maxPageCount the maximum number of pages that can be stored in memory, as Java Objects
     *                     at one time.
     * @param directory the directory the storage.bin file is stored in.
     * @return the singleton instance of the BufferManager
     */
    public static synchronized BufferManager buildBufferManager(int maxPageCount, String directory) {
        if (instance == null) {
            instance = new BufferManager(maxPageCount, directory);
        }
        return instance;
    }

    /**
     * getInstance checks if the buffermanager has been built, then returns it or throws an error.
     *
     * @return the singleton instance of the BufferManager
     */
    public static BufferManager getInstance() {
        if (instance == null) {
            throw new RuntimeException("BufferManager not built. An error Occurred");
        }
        return instance;
    }


    /**
     * getPage first checks if the id is valid, -1 indicating that it is invalid.
     * after it checks if the page is in the buffer.
     * if it is not in the buffer, it ejects a page from the buffer, before reading
     * the page from hardware.
     *
     * @param pageId the ID of the page
     * @return A Page Object with the given id, or null dependent on ID
     */
    public Page getPage(int pageId){
        if(pageId==-1){
            return null;
        }

        if(buffer.containsKey(pageId)){
            Page page = buffer.get(pageId);
            page.timestamp = LocalDateTime.now();
            return page;
        }

        // Page not in buffer, fetch from disk.
        try{
            flushOldestIfNeeded();
            Page decodedPage = readPageFromHardwareV2(pageId);
            buffer.put(decodedPage.pageId, decodedPage);
            decodedPage.timestamp = LocalDateTime.now();
            return decodedPage;
        } catch (IOException e){
            System.err.println("Inconsistent Database");
            throw new RuntimeException(e);
        }

    }

    /**
     * createNewPage first checks if there is space in the buffer.
     * after which it creates a new page and adds it to the buffer
     *
     * @param id the id of the new table
     * @param tableName the name of table this page belongs to
     * @return the newly created page object
     */
    public  void createNewPage(int id, String tableName)  {
        try {
            flushOldestIfNeeded();
            Page page = new Page(id, tableName);
            buffer.put(page.pageId, page);

            // immediately write a blank page for edge case.
            writePageToHardwareV2(page);
        } catch (Exception e) {
            System.err.println("Failed to create page");
            throw new RuntimeException(e);
        }
    }

    /**
     * flushOldestIfNeeded checks the buffer size against the maxPageCount field.
     * if buffer is equal (or larger, which shouldn't occur), then the oldest page
     * is ejected from the buffer, clearing space
     *
     */
    public void flushOldestIfNeeded() {
        if (buffer.size()>= maxPageCount){
            Page oldestPage = Collections.min(buffer.values(), Comparator.comparing(p -> p.timestamp));
            try {
                writePageToHardwareV2(oldestPage);
                buffer.remove(oldestPage.pageId);
            } catch (Exception e) {
                System.err.println("Failed to flush page from Buffer");
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * flushBuffer should only be called when exiting the database. It writes all existing
     * pages in the dataBase back to the disk.
     */
    public void flushBuffer() {
        for(Page p : buffer.values()){
            try {
                writePageToHardwareV2(p);
            } catch (Exception e) {
                System.err.println("Failed to flush page id=" + p.pageId + " from Buffer");
                throw new RuntimeException(e);
            }

        }
    }

    /**
     * inserts a record into a given table.
     * iterates through the pages until it finds where it belongs,
     * if required to split it calls the helper function handlePageSplit()
     *
     * @param tableName the name of the table you are inserting into
     * @param record the given record you are inserting
     */
    public void insertRecordIntoTable(String tableName, Record record) {
        TableSchema ts = dataCatalog.getTableSchema(tableName);

        int currentPageId = ts.getRootPageID();

        while (currentPageId != -1) {
            Page page = getPage(currentPageId);

            InsertionResult result = page.tryInsert(record, ts);
            switch (result) {
                case SUCCESS -> {
                    return;
                }
                case NEEDS_SPLIT -> {
                    handlePageSplit(page, record, ts);
                    return;
                }
                case NOT_IN_RANGE -> currentPageId = page.nextPageId;

            }
        }
    }

    public void insertRecordIntoTableNoOrder(String tableName, Record record) throws Exception {
        TableSchema ts = dataCatalog.getTableSchema(tableName);

        int currentPageId = ts.getRootPageID();

        while (currentPageId != -1) {
            Page page = getPage(currentPageId);

            InsertionResult result = page.tryInsertNoOrder(record, ts);
            switch (result) {
                case SUCCESS -> {
                    return;
                }
                case NOT_IN_RANGE -> {
                    // cant fit on page, but we are not on the last page so do not split,
                    // iterate to next one
                    currentPageId = page.nextPageId;
                }
                case NEEDS_SPLIT -> {
                    // treat NEEDS_SPLIT as a 'cant fit in here and nextPageId is -1'
                    // make new empty page and insert record at end
                    int newId = dataCatalog.getNextAvailablePageID();
                    page.nextPageId = newId;
                    createNewPage(newId, ts.tableName);
                    Page newPage = getPage(newId);
                    // do not need to see results, will always return
                    // SUCCESS due to page being empty it will always fit
                    newPage.tryInsertNoOrder(record, ts);
                }

            }
        }
    }

    /**
     * Deletes a table by iterate going through the list of linked pages, deleting each one
     * from Disk.
     * @param tableName the name of the table you are deleting
     * @throws IOException if the storagemanager fails (bad ID or IO failure)
     */
    public void deleteTable(String tableName) throws IOException {
        // Grab table schema, iterate through pages until -1, deleting pages.
        TableSchema tableSchema = DataCatalog.getInstance().getTableSchema(tableName);
        int pageId = tableSchema.rootPageID;

        while (pageId != -1) {
            Page p = getPage(pageId);
            int nextPageId = p.nextPageId;

            buffer.remove(pageId);
            storageManager.deletePage(pageId);
            pageId = nextPageId;
        }

        // Once all pages are deleted, remove tableschema from DataCatalog
        DataCatalog.getInstance().removeTableSchema(tableName);
    }

    /**
     * Adds a column to a given table.
     * After adding a column on each page it checks if a split is required.
     *
     * @param defaultValue the default value for the new column
     * @param tableName the name of the given table.
     */
    public void addColumn(AttributeValue<?> defaultValue, String tableName) {
        TableSchema ts = DataCatalog.getInstance().getTableSchema(tableName);
        int pageSize = DataCatalog.getInstance().getPageSize();

        int pageId = ts.getRootPageID();
        while (pageId != -1) {
            Page page = getPage(pageId);

            page.addColumn(defaultValue);

            if (page.getTotalRecordsSize() > pageSize) {
                int originalNextPageId = page.nextPageId; // Save old id so we don't repeat the split pages
                handlePageSplit(page, ts);

                pageId = originalNextPageId;
            } else {
                pageId = page.nextPageId;
            }
        }
    }

    /**
     * Deletes a column on a given table by attribute name.
     * TODO: possibly look at page merging in the future for unused space?
     * @param columnName the name of the attribute you are deleting
     * @param tableName the name of the table you are deleting from
     */
    public void deleteColumn(String columnName, String tableName) {
        TableSchema ts = DataCatalog.getInstance().getTableSchema(tableName);

        int attributeIndex = ts.getIndex(columnName);

        int pageId = ts.rootPageID;
        while (pageId != -1) {
            Page page = getPage(pageId);
            page.deleteColumn(attributeIndex);
            pageId = page.nextPageId;
        }
    }

    /**
     * An updated version of readPageFromHardWare utilizing ByteBuffer functionality and cutting
     * out unnecessary calls to Array.CopyOfRange()
     * Better performance and readability.
     *
     *
     * @param pageId the ID of the page you are requesting
     * @return a page object read from disk
     * @throws IOException thrown from storage manager, since this is a private function we pass it up.
     */
    private Page readPageFromHardwareV2(int pageId) throws IOException {


        // get byte array from hardware
        byte[] encodedByteArray = storageManager.readPageV2(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(encodedByteArray);

        // instantiate page with first few fields
        int nextPageId = buffer.getInt();
        int tableNameLength = buffer.getInt();
        byte[] tableNameBytes = new byte[tableNameLength];
        buffer.get(tableNameBytes);
        int numOfRecords = buffer.getInt();

        String tableName = new String(tableNameBytes, StandardCharsets.UTF_8);
        Page p = new Page(pageId, tableName);
        p.nextPageId = nextPageId;

        ArrayList<AttributeSchema> attributes = new ArrayList<>(dataCatalog.getTableSchema(tableName)
                .getAttributeSchemas().sequencedValues());

        // process records until buffer is empty
        for (int i = 0; i < numOfRecords; i++) {
            Record record = convertBytesToRecordV2(buffer, attributes);
            p.recordList.add(record);
        }

        return p;
    }

    /**
     * An Updated Version of convertBytesToRecord that takes in a ByteBuffer instead of a
     * byte array. Greater performance and readability gains.
     *
     *
     * @param buffer A bytebuffer assumed to be at the start of the record. passed only by convertBytesToPage
     * @param attributes the list of attributes for the tableSchema
     * @return a record object
     */
    private static Record convertBytesToRecordV2(ByteBuffer buffer, ArrayList<AttributeSchema> attributes) {

        Record record = new Record();

        byte[] nullByteArray = new byte[attributes.size()];
        buffer.get(nullByteArray); // advances pointer and loads values into nullByte array

        for (int i = 0; i < nullByteArray.length; i++) {
            DataTypes d = attributes.get(i).getDataType();

            if (nullByteArray[i] == 1) {
                // if null set data field to null
                record.attributeList.add(new AttributeValue<>(null, d));
            } else {
                Object value = switch(d) {
                    case INTEGER -> buffer.getInt();
                    case DOUBLE -> buffer.getDouble();
                    case BOOLEAN -> buffer.get() == 1;
                    case CHAR, VARCHAR -> {
                        int len = buffer.getInt();
                        byte[] sBytes = new byte[len];
                        buffer.get(sBytes);
                        yield new String(sBytes, StandardCharsets.UTF_8);
                    }
                };
                record.attributeList.add(new AttributeValue<>(value, d));
            }
        }
        return record;
    }


    /**
     * Writes a page to disk, features readability and performance improvements from V1.
     *
     * @param page The page object we wish to write to disk
     */
    private void writePageToHardwareV2(Page page) {
        //prevent a page from writing if it has not been modified
        // need to add if table was recently touched as well with timestamp. for initial disk allocation.
        long secondsActive = Duration.between(page.timestamp, java.time.LocalDateTime.now()).getSeconds();
        if(!page.hasBeenModified && secondsActive > 5){
            return;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(dataCatalog.getPageSize());


        try {
            byteBuffer.putInt(page.nextPageId);
            byte[] tableNameBytes = page.tableName.getBytes(StandardCharsets.UTF_8);
            byteBuffer.putInt(tableNameBytes.length);
            byteBuffer.put(tableNameBytes);

            // put in number of records, used for reading from hardware for loop counter
            byteBuffer.putInt(page.recordList.size());

            ArrayList<AttributeSchema> attributes = new ArrayList<>(dataCatalog.getTableSchema(page.tableName)
                    .getAttributeSchemas().sequencedValues());

            for (Record r : page.recordList) {
                convertRecordToBytesV2(r, attributes, byteBuffer);
            }

            byteBuffer.flip(); // resets position, clips length. prepares for it to be sent to storage manager
            storageManager.writePage(page.pageId, byteBuffer);


        } catch (IOException e) {
            System.err.println("Error occurred while attempting to write page " + page.pageId + " to disk...");
            throw new RuntimeException(e);
        }

    }

    /**
     * Converts a Record object to bytes, to later be written to disk.
     *
     * @param r the record we wish to convert to bytes
     * @param attributes the list of attributes, and their corresponding DataTypes
     * @param buffer the bytebuffer, at a position ready for the record data to be written
     */
    private static void convertRecordToBytesV2(Record r, ArrayList<AttributeSchema> attributes, ByteBuffer buffer) {
        // write null bit array into buffer
        for (AttributeValue<?> a : r.attributeList) {
            if (a.data == null) {
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0);
            }
        }

        for (int i = 0; i < attributes.size(); i++) {
            Object value = r.attributeList.get(i).data;
            if (value != null) {
                DataTypes dataType = attributes.get(i).getDataType();
                switch (dataType) {
                    case INTEGER -> buffer.putInt((int) value);
                    case DOUBLE -> buffer.putDouble((double) value);
                    // in-case value is Boolean and not boolean, haven't checked full codebase
                    case BOOLEAN -> buffer.put((byte) ((boolean) value ? 1 : 0));
                    case CHAR, VARCHAR -> {
                        String s = (String) value;

                        byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
                        // write the length of value
                        buffer.putInt(strBytes.length);

                        buffer.put(strBytes);
                    }
                }
            }
        }
    }


    /**
     * Splits the page at the midway point, links the ids to maintain record order on primary key
     *
     * @param page the page you wish to split
     * @param ts the table schema needed for accessing metadata
     */
    private void handlePageSplit(Page page, TableSchema ts) {
        // Link pages in correct order page -> page.nextPage goes to page -> page2 -> page.nextPage
        int newId = DataCatalog.getInstance().getNextAvailablePageID();
        Page page2 = new Page(newId, ts.tableName);
        page2.nextPageId = page.nextPageId;
        page.nextPageId = newId;

        // Give Each Page half of the records
        int mid = page.recordList.size() / 2;
        ArrayList<Record> firstHalf = new ArrayList<>(page.recordList.subList(0, mid));
        ArrayList<Record> secondHalf = new ArrayList<>(page.recordList.subList(mid, page.recordList.size()));
        page.recordList = firstHalf;
        page2.recordList = secondHalf;

        page.hasBeenModified = true;
        page2.hasBeenModified = true;
        page.timestamp = java.time.LocalDateTime.now();
        page2.timestamp = java.time.LocalDateTime.now();

        buffer.put(newId, page2);
    }

    /**
     * overloaded version of handlePageSplit for when the split happens
     * due to the insertion of a new record
     *
     * @param page the page you are spliting
     * @param record the record you are looking to insert
     * @param ts the tableschema you are using as reference
     */
    private void handlePageSplit(Page page, Record record, TableSchema ts) {
        // Temporarily insert record into page before split to get in order positioning
        page.insert(record, ts, true, false);

        // Call the parent handlePageSplit function
        handlePageSplit(page, ts);
    }

    public void deleteTempTables() {
        // delete all temporary tables
    }

}