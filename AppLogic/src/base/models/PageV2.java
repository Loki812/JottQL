package base.models;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class PageV2 {

    public final String tableName;
    public int pageId;
    public int nextPageId;
    public boolean hasBeenModified;
    public LocalDateTime timestamp;
    private ArrayList<Record> recordList = new ArrayList<>();

    public PageV2(int pageId, String tableName) {
        this.pageId = pageId;
        this.nextPageId = -1; // initialized to null (-1)
        this.tableName = tableName;
        this.hasBeenModified = false;
        this.timestamp = LocalDateTime.now();
    }

    public int getTotalRecordsSize() {
        int accum = 0;
        for (Record r : recordList) {
            accum += r.getSize();
        }
        return accum;
    }

    public ArrayList<Record> getRecordList() { return recordList; }

    public void addRecord(int index, Record r) {
        recordList.add(index, r);
        timestamp = LocalDateTime.now();
        hasBeenModified = true;
    }

    // TODO: for future
    public void deleteRecord() {

    }

    public int getNumOfRecords() { return recordList.size(); }

    public void setRecordList(ArrayList<Record> records) {
        this.recordList = records;
    }
}
