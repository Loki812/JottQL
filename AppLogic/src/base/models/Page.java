package base.models;
import base.models.Record;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class Page {

    private int pageId;
    public LocalDateTime timestamp;
    public ArrayList<Record> recordList;

    //todo the Page is just a list of pointers to records
    public Page(int pageId){
        this.pageId = pageId;
        this.timestamp = LocalDateTime.now();
        recordList = new ArrayList<Record>();
    }

    public int getPageId(){
        return pageId;
    }

}
