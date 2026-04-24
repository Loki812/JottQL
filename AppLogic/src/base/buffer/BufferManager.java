package base.buffer;
import base.models.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import base.models.concrete.*;
import base.models.concrete.Record;
import base.models.schemas.AttributeSchema;
import base.models.schemas.IndexSchema;
import base.models.schemas.InsertionResult;
import base.models.schemas.TableSchema;
import base.storage.StorageManager;

public class BufferManager {

    private static BufferManager instance = null;

    // singleton variables
    private final int maxPageCount;
    private final HashMap<Integer, Ipage> buffer;
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
     * Updates in V2: Returns a Ipage instead of a normal page, requires casting
     * via (DataPage) or (IndexPage) after call.
     *
     * getPage first checks if the id is valid, -1 indicating that it is invalid.
     * after it checks if the page is in the buffer.
     * if it is not in the buffer, it ejects a page from the buffer, before reading
     * the page from hardware.
     *
     * @param pageId the ID of the page
     * @return A Page Object with the given id, or null dependent on ID
     */
    public Ipage getPageV2(int pageId) {
        if(pageId == -1) {
            return null;
        }

        if(buffer.containsKey(pageId)) {
            Ipage page = buffer.get(pageId);
            page.setTimeStamp(LocalDateTime.now());
            return page;
        }

        // Page not in buffer, fetch from disk
        try{
            flushOldestIfNeeded();
            Ipage decodedPage = readPageFromHardwareV3(pageId);
            buffer.put(pageId, decodedPage);
            decodedPage.setTimeStamp(LocalDateTime.now());
            return decodedPage;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new index page, calls flushOldestFromBuffer to check if space is needed
     *
     * @param pageId the id of the new page
     * @param tableName the name of the related table
     * @param parentPageId the id of the parent to the page in the B+ tree
     */
    public void createNewIndexPage(int pageId, String tableName, AttributeSchema searchKey, int parentPageId) {
        try {
            flushOldestIfNeeded();
            IndexPage indexPage = new IndexPage(pageId, tableName, searchKey, parentPageId);
            buffer.put(pageId, indexPage);
        } catch (Exception e) {
            System.err.println("Failed to create new page");
            throw new RuntimeException(e);
        }
    }


    /**
     * createNewPage first checks if there is space in the buffer.
     * after which it creates a new page and adds it to the buffer
     *
     * @param id the id of the new table
     * @param tableName the name of table this page belongs to
     * @return the newly created data page object
     */
    public  void createNewDataPage(int id, String tableName)  {
        try {
            flushOldestIfNeeded();
            Page page = new Page(id, tableName);
            buffer.put(page.pageId, page);
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
            Ipage oldestPage = Collections.min(buffer.values(), Comparator.comparing(Ipage::getTimeStamp));
            try {
                writePageToHardwareV2(oldestPage);
                buffer.remove(oldestPage.getPageId());
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
        for(Ipage p : buffer.values()){
            try {
                writePageToHardwareV2(p);
            } catch (Exception e) {
                System.err.println("Failed to flush page id=" + p.getPageId() + " from Buffer");
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
    public void insertRecordIntoTable(String tableName, Record record, boolean duplicates) {
        TableSchema ts = dataCatalog.getTableSchema(tableName);

        ArrayList<Integer> rootPageIds = new ArrayList<>();

        if (dataCatalog.indexOn) {
            ArrayList<IndexSchema> indexes = new ArrayList<>();

            for (AttributeSchema as : ts.getAttributeSchemas().values()) {
                IndexSchema is = dataCatalog.getIndexSchema(tableName, as.attributeName);
                if (is != null) {
                    indexes.add(is);
                }
            }
            if(indexes.isEmpty()){
                rootPageIds.add(ts.getRootPageID());
            }else {
                for (IndexSchema is : indexes) {
                    Ipage ip = getPageV2(is.rootPageID);
                    rootPageIds.add(ip.getPageId());
                }
            }
        } else {
            rootPageIds.add(ts.getRootPageID());
        }

        for (int currentPageId : rootPageIds) {
            insertHelper(currentPageId, record, ts,duplicates);
        }
    }

    public void insertHelper(int currentPageId, Record record, TableSchema ts,  boolean duplicates) {
        while (currentPageId != -1) {
            Ipage page = getPageV2(currentPageId);
            InsertionResult result = page.tryInsert(record, ts, duplicates);
            switch (result) {
                case SUCCESS -> {
                    return;
                }
                case NEEDS_SPLIT -> {
                    page.split();
                    if(page instanceof Page) {
                        result = page.tryInsert(record, ts , duplicates);
                        if(result == InsertionResult.NOT_IN_RANGE) {
                            currentPageId = page.nextPageId();
                        }else{
                            return;
                        }
                    }else {
                        return;
                    }
                }
                case NOT_IN_RANGE -> currentPageId = page.nextPageId();

            }
        }
    }

    public void insertRecordIntoTableNoOrder(String tableName, Record record) throws Exception {
        TableSchema ts = dataCatalog.getTableSchema(tableName);

        int currentPageId = ts.getRootPageID();

        while (currentPageId != -1) {
            Page page = (Page) getPageV2(currentPageId);

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
                    createNewDataPage(newId, ts.tableName);
                    Page newPage = (Page) getPageV2(newId);
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

        //Remove pages of table
        while (pageId != -1) {
            Page p = (Page) getPageV2(pageId);
            int nextPageId = p.nextPageId;

            buffer.remove(pageId);
            storageManager.deletePage(pageId);
            pageId = nextPageId;
        }
        //remove tableSchema from DataCatalog
        DataCatalog.getInstance().removeTableSchema(tableName);
    }

    public void deleteIndex(String tableName) throws IOException {
        DataCatalog dc = DataCatalog.getInstance();

        ArrayList<IndexSchema> indexes = new ArrayList<>();

        TableSchema tableSchema = dc.getTableSchema(tableName);

        for (AttributeSchema as : tableSchema.getAttributeSchemas().values()) {
            IndexSchema is = dc.getIndexSchema(tableName, as.attributeName);
            if (is != null) {
                indexes.add(is);
            }
        }

        ArrayList<Integer> pageIds = new ArrayList<>();

        for (IndexSchema is : indexes) {
            pageIds.add(is.rootPageID);
        }

        deleteIndexPages(pageIds);
    }

    private void deleteIndexPages(ArrayList<Integer> pageIds) throws IOException {
        for (int pageId : pageIds) {
            IndexPage ip = (IndexPage)getPageV2(pageId);
            if (!ip.isLeaf) {
                ArrayList<Integer> children = ip.childPointers;
                buffer.remove(pageId);
                storageManager.deletePage(pageId);
                deleteIndexPages(children);
            }
        }
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
            Page page = (Page) getPageV2(pageId);

            page.addColumn(defaultValue);

            if (page.getPageSize() > pageSize) {
                int originalNextPageId = page.nextPageId; // Save old id so we don't repeat the split pages
                page.split();
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
            Page page = (Page) getPageV2(pageId);
            page.deleteColumn(attributeIndex);
            pageId = page.nextPageId;
        }
    }

    /**
     * Updates in V3: utilizes Ipage instead of Page, to create common code for
     * reading index pages and data pages
     * An updated version of readPageFromHardWare utilizing ByteBuffer functionality and cutting
     * out unnecessary calls to Array.CopyOfRange()
     * Better performance and readability.
     *
     *
     * @param pageId the ID of the page you are requesting
     * @return a page object read from disk
     * @throws IOException thrown from storage manager, since this is a private function we pass it up.
     */
    private Ipage readPageFromHardwareV3(int pageId) throws IOException {
        byte[] encodedByteArray = storageManager.readPageV2(pageId);

        return PageFactory.fromBytes(pageId, encodedByteArray);
    }

    /**
     * Updated to take Ipage
     * Writes a page to disk, features readability and performance improvements from V1.
     *
     * @param page The page object we wish to write to disk
     */
    private void writePageToHardwareV2(Ipage page) {
        //prevent a page from writing if it has not been modified
        // need to add if table was recently touched as well with timestamp. for initial disk allocation.
        long secondsActive = Duration.between(page.getTimeStamp(), java.time.LocalDateTime.now()).getSeconds();
        if(!page.getHasBeenModified() && secondsActive > 5){
            return;
        }
        try {
            ByteBuffer byteBuffer = page.toBytes();
            storageManager.writePage(page.getPageId(), byteBuffer);
        } catch (IOException e) {
            System.err.println("Error occurred while attempting to write page " + page.getPageId() + " to disk...");
            throw new RuntimeException(e);
        }

    }


    /**
     *
     * @param page the page you are spliting
     * @param record the record you are looking to insert
     * @param ts the tableschema you are using as reference
     */
    private void handlePageSplit(Page page, Record record, TableSchema ts) {
        // Temporarily insert record into page before split to get in order positioning
        page.insert(record, ts, true, true);
        page.split();
    }

}
