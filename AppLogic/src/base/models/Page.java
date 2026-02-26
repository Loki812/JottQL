package base.models;

import base.buffer.BufferManager;
import base.storage.StorageManager;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class Page {

    public final String tableName;
    public int pageId;
    public int nextPageId;
    public int currentSize;
    public boolean hasBeenModified;
    public LocalDateTime timestamp;
    private DataCatalog catalog;
    public ArrayList<Record> recordList;

    public Page(int pageId, String tableName){
        this.pageId = pageId;
        this.tableName = tableName;
        this.currentSize = 0;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
        this.catalog = DataCatalog.getInstance();
        this.nextPageId = -1;
        recordList = new ArrayList<Record>();
    }

    /**
     * Insert a record into this page.
     *
     * @param record The record to insert
     * @throws Exception If the DataCatalog instance cannot be retrieved
     */
    public void insertIntoPage(Record record) throws Exception {
        int pageSize = catalog.getPageSize();
        TableSchema schema = catalog.getTableSchema(tableName);

        if (recordList.isEmpty()) {
            recordList.addFirst(record);
            this.currentSize += record.getSize();
            this.hasBeenModified = true;
            this.timestamp = LocalDateTime.now();
            return;
        }

        for (int i = 0; i < recordList.size(); i++) {
            if (record.compareTo(recordList.get(i), schema) == 0) {
                throw new Exception("Duplicate primary key detected");
            }
            if (record.compareTo(recordList.get(i), schema) < 0) {
                // If the page is full, split it
                if (currentSize + record.getSize() > pageSize) {
                    splitPage();
                    insertIntoPage(record);
                }
                // Otherwise insert the record in the correct place
                recordList.add(i, record);
                this.currentSize += record.getSize();
                this.hasBeenModified = true;
                this.timestamp = LocalDateTime.now();
                return;
            }
        }
        if(nextPageId == -1){
            nextPageId = DataCatalog.getInstance().getNextAvailablePageID();
            BufferManager.createNewPage(nextPageId, tableName).insertIntoPage(record);
        }else {
            BufferManager.getPage(nextPageId).insertIntoPage(record);
        }
    }

    public void insertNoOrder(Record record) throws Exception {
        int pageSize = catalog.getPageSize();
        if(currentSize + record.getSize() > pageSize){
            if(nextPageId == -1) {
                nextPageId = DataCatalog.getInstance().getNextAvailablePageID();
                BufferManager.createNewPage(nextPageId, tableName).insertNoOrder(record);
            }else {
                BufferManager.getPage(nextPageId).insertNoOrder(record);
            }
        }else {
            recordList.add(record);
            this.currentSize += record.getSize();
        }
    }

    /**
     * Split a full page and insert a record into one of the pages.
     *
     * @throws Exception If the next page cannot be retrieved
     * @throws IllegalStateException If no next page is available
     */
    private void splitPage() throws IOException {
        if (nextPageId < 0) {
            throw new IllegalStateException("No next page available for split");
        }

        // Create a new page and re-order the pointers
        Page nextPage = BufferManager.createNewPage(catalog.getNextAvailablePageID(), tableName);
        nextPage.nextPageId = nextPageId;
        this.nextPageId = nextPage.pageId;

        // Split the record list into two equal halves
        int split = recordList.size() / 2;

        // Move half of the records to the next page
        nextPage.recordList.addAll(recordList.subList(split, recordList.size()));
        recordList.subList(split, recordList.size()).clear();

        // Recalculate each page's current size
        for(Record record : recordList){
            currentSize += record.getSize();
        }
        for (Record record : nextPage.recordList) {
            nextPage.currentSize += record.getSize();
        }

        // Mark both pages as having been modified
        this.hasBeenModified = true;
        nextPage.hasBeenModified = true;

        // Record timestamps
        this.timestamp = java.time.LocalDateTime.now();
        nextPage.timestamp = java.time.LocalDateTime.now();
    }

    public void deleteTable(){
        Page page = BufferManager.getPage(nextPageId);
        if(page != null){
            page.deleteTable();
        }
        StorageManager.deletePage(pageId);
        BufferManager.deletePage(pageId);
    }

    public void deleteColumn(int index){
        for(Record record : recordList){
            record.attributeList.remove(index);
        }
        Page page = BufferManager.getPage(nextPageId);
        if(page != null){
            page.deleteColumn(index);
        }
        hasBeenModified = true;
    }

    public void addColumn(AttributeValue<?> defaultValue) throws IOException {
        for(Record record : recordList){
            currentSize -= record.getSize();
            record.attributeList.add(defaultValue);
            currentSize += record.getSize();
        }
        Page page = BufferManager.getPage(nextPageId);
        if(page != null){
            page.addColumn(defaultValue);
        }
        if (currentSize > DataCatalog.getInstance().getPageSize()) {
            splitPage();
        }
    }
}
