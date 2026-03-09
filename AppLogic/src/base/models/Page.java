package base.models;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class Page {

    public final String tableName;
    public int pageId;
    public int nextPageId;
    public boolean hasBeenModified;
    public LocalDateTime timestamp;
    public ArrayList<Record> recordList;

    public Page(int pageId, String tableName){
        this.pageId = pageId;
        this.tableName = tableName;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
        this.nextPageId = -1;
        recordList = new ArrayList<>();
    }


    /**
     * tryInsert attempts to insert the record into the given page.
     * THIS FUNCTION ASSUMES LINEAR SEARCH AND PAGES WITH SMALLER VALUES HAVE BEEN ALREADY CHECKED
     * If this is the last page within a table, it inserts no matter what.
     * lets the caller know if the page needs to be split.
     *
     * @param record the record you are attempting to insert
     * @param schema the schema you are using for reference
     * @return SUCCESS if record is inserted, NOT_IN_RANGE if record primary key is not in range of page
     * NEEDS_SPLIT if the record is in range, but the page is full.
     */
    public InsertionResult tryInsert(Record record, TableSchema schema) {
        // 1. Check if record is in range of page
        if (!recordList.isEmpty() && record.compareTo(recordList.getLast(), schema) > 0) {
            // If we are not on the last page, send signal to buffer manager to iterate to next page
            if (nextPageId != -1) return InsertionResult.NOT_IN_RANGE;
        }

        // 2. Check if record fits in page
        int pageSize = DataCatalog.getInstance().getPageSize();
        if (getTotalRecordsSize() + record.getSize() > pageSize) {
            return InsertionResult.NEEDS_SPLIT;
        }

        // 3. Insert into page if both of those pass
        insertSorted(record, schema);
        this.hasBeenModified = true;
        this.timestamp = java.time.LocalDateTime.now();
        return InsertionResult.SUCCESS;

    }

    /**
     * insertSorted maintains record order on a page, inserting by linear search and using record.compareTO
     * @param record the given record you are inserting
     * @param schema the schema you are using as reference
     */
    public void insertSorted(Record record, TableSchema schema) {

        for (int i = 0; i < recordList.size(); i++) {
            if (record.compareTo(recordList.get(i), schema) < 0) {
                recordList.add(i, record);
                return;
            }
        }

        // We iterated over entire record list and this record is greater than all
        recordList.add(record);
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
    public int getTotalRecordsSize() {
        int accum = 0;
        for (Record r : recordList) {
            accum += r.getSize();
        }
        return accum;
    }
}
