package base.models;
import base.models.Record;

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



}
