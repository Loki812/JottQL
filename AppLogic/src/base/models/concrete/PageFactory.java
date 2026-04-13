package base.models.concrete;

import java.nio.ByteBuffer;

public class PageFactory {

    public static Ipage fromBytes(int pageId, byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int pageType = buffer.getInt();

        return switch (pageType) {
            case Ipage.DATA_PAGE_IND -> new Page(pageId, bytes);
            case Ipage.INDEX_PAGE_IND -> new IndexPage(pageId, bytes);
            default -> throw new RuntimeException("Corrupted Page, did not start with type indicator...");
        };
    }
}
