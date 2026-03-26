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
        insert(record, schema, true, false);
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
        if (getTotalRecordsSize() + record.getSize() > pageSize) {
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
            if(record.compareTo(recordList.get(i), schema) == 0 && !DUPLICATES_ALLOWED){
                throw new RuntimeException("Duplicate primary key found while attempting insertion...");
            }
            if (record.compareTo(recordList.get(i), schema) <= 0) {
                recordList.add(i, record);
                return;
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
    public int getTotalRecordsSize() {
        int accum = 0;
        for (Record r : recordList) {
            accum += r.getSize();
        }
        return accum;
    }
}
