package base.models.concrete;

import java.nio.ByteBuffer;

public interface Ipage {

    // use indicators to determine if a page is a Index Page or Data Page
    int DATA_PAGE_IND = 1;
    int INDEX_PAGE_IND = 2;

    public ByteBuffer toBytes();

    public int getPageSize();
}
