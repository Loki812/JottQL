package base.models;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class Page {

    public int pageId;
    public int tableId;
    public int nextPageId;
    public int currentSize;
    public boolean hasBeenModified;
    public LocalDateTime timestamp;
    public ArrayList<Record> recordList;

    public Page(int pageId){
        this.pageId = pageId;
        this.currentSize = 0;
        this.hasBeenModified = true;
        this.timestamp = LocalDateTime.now();
        recordList = new ArrayList<Record>();
    }

    public void insertIntoPage(Record record) {
        // records sorted by primary key
        // call compareTo
        // check last element in recordList to check if split needed/next page needed
    }

}
