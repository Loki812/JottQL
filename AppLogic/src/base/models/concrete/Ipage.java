package base.models.concrete;

import base.models.schemas.TableSchema;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public interface Ipage {

    // use indicators to determine if a page is a Index Page or Data Page
    int DATA_PAGE_IND = 1;
    int INDEX_PAGE_IND = 2;

    public ByteBuffer toBytes();

    public int getPageSize();

    public void setTimeStamp(LocalDateTime timeStamp);

    public LocalDateTime getTimeStamp();

    public int getPageId();

    public boolean getHasBeenModified();

    public void insert(Record record, TableSchema ts, boolean ORDERED, boolean DUPLICATES_ALLOWED);
}
