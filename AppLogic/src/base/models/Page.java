package base.models;

import base.buffer.BufferManager;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class Page {

    public int pageId;
    public String tableName;
    public int nextPageId;
    public int currentSize;
    public boolean hasBeenModified;
    public LocalDateTime timestamp;
    public ArrayList<Record> recordList;

    public Page(int pageId, String tableName){
        this.pageId = pageId;
        this.tableName = tableName;
        this.currentSize = 0;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
        recordList = new ArrayList<Record>();
    }

    /**
     * Insert a record into this page.
     *
     * @param record The record to insert
     * @param schema The table schema
     * @throws Exception If the DataCatalog instance cannot be retrieved
     */
    public void insertIntoPage(Record record, TableSchema schema) throws Exception {
        int recordSize = schema.getRecordSize();
        int pageSize = DataCatalog.getInstance().getPageSize();

        // If the page is full, split it
        if (currentSize + recordSize > pageSize) {
            splitPage(schema);
        }

        // If the page is not full, insert the record at the correct position
        for (int i = 0; i < recordList.size(); i++) {
            if (record.compareTo(recordList.get(i), schema) < 0) {
                recordList.add(i, record);
                this.currentSize += recordSize;
                this.hasBeenModified = true;
                this.timestamp = LocalDateTime.now();
                return;
            }
        }
        // Add to end if this record is the largest
        recordList.add(record);
        this.currentSize += recordSize;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
    }

    /**
     * Split a full page and insert a record into one of the pages.
     *
     * @param schema The table schema
     * @throws Exception If the next page cannot be retrieved
     * @throws IllegalStateException If no next page is available
     */
    private void splitPage(TableSchema schema) throws Exception {
        if (nextPageId < 0) {
            throw new IllegalStateException("No next page available for split");
        }

        // Fetch the next page from the buffer
        Page nextPage = BufferManager.getPage(nextPageId);

        // Split the record list into two equal halves
        int split = recordList.size() / 2;

        // Move half of the records to the next page
        nextPage.recordList.addAll(recordList.subList(split, recordList.size()));
        recordList.subList(split, recordList.size()).clear();

        // Recalculate each page's current size
        this.currentSize = recordList.size() * schema.getRecordSize();
        nextPage.currentSize = recordList.size() * schema.getRecordSize();

        // Mark both pages as having been modified
        this.hasBeenModified = true;
        nextPage.hasBeenModified = true;

        // Record timestamps
        this.timestamp = java.time.LocalDateTime.now();
        nextPage.timestamp = java.time.LocalDateTime.now();
    }
}
