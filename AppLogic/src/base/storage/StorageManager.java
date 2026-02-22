package base.storage;
import base.models.DataCatalog;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * The StorageManager handles low-level disk I/O operations.
 */
public class StorageManager {
    private static RandomAccessFile file;
    private static DataCatalog catalog;
    private static ArrayList<Integer> freePages;
    //private static HashMap<Integer, Integer> idSizeMapper;

    /**
     * Create a new StorageManager instance.
     *
     * @param filename The database file name
     * @throws Exception If the file cannot be opened or created
     */
    public StorageManager(String filename) throws Exception {
        StorageManager.file = new RandomAccessFile(filename, "rw");
        StorageManager.catalog = DataCatalog.getInstance();
        StorageManager.freePages = new ArrayList<>();
        //idSizeMapper = new HashMap<Integer, Integer>();
    }

    /**
     * Get the ArrayList of free pages.
     *
     * @return the freePages ArrayList of Integers
     */
    public static ArrayList<Integer> getFreePages() {
        return freePages;
    }

    /**
     * Read a page from disk.
     *
     * @param pageId The ID of the page to read
     * @return A ByteBuffer object containing the page data
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the page does not exist
     */
    public static byte[] readPage(int pageId) throws IOException {
        int pageSize = catalog.getPageSize();
        long offset = (long) pageId * pageSize;

        if (offset >= file.length()) {
            throw new IllegalArgumentException("Page does not exist");
        }

        // Allocate a new buffer and read page contents into it at the page's location
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        file.seek(offset);

        byte[] byteCount = new byte[Integer.BYTES];
        for(int i =0; i<Integer.BYTES; i++){
            file.seek(offset+i);
            //System.out.println(file.readByte());
            byteCount[i] = file.readByte();
        }


        int pageByteSize = (ByteBuffer.wrap(byteCount).getInt())-Integer.BYTES;

        byte[] finalByteArray = new byte[pageByteSize];

        for(int i =0; i<pageByteSize; i++){
            file.seek(offset+i+Integer.BYTES);
            finalByteArray[i] = file.readByte();
        }

        //file.readFully(buffer.array());

        buffer.position(0); // Reset buffer cursor back to beginning

        return finalByteArray;
    }

    /**
     * Write a page to disk.
     *
     * @param pageId The ID of the page to write
     * @param pageData The page data to write to disk
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the page size does not match
     */
    public static void writePage(int pageId, ByteBuffer pageData) throws IOException {

        //todo idSizeMapper.put(pageId,pageData.array().length);

        int pageSize = catalog.getPageSize();

        if (pageData.array().length > pageSize) {
            throw new IllegalArgumentException("Page size mismatch");
        }

        // Find page location and write there
        long offset = (long) pageId * pageSize;
        file.seek(offset);
        file.write(pageData.array());

        // If writing to a previously freed page, remove it from the free pages list
        if (freePages.contains(pageId)) {
            freePages.remove(pageId);
        }
    }

    /**
     * Delete a page by clearing its contents and marking it as free.
     *
     * @param pageId The ID of the page to delete
     * @throws IllegalArgumentException If the page does not exist
     */
    public static void deletePage(int pageId) {
        try {
            int pageSize = catalog.getPageSize();
            long offset = (long) pageId * pageSize;

            if (offset >= file.length()) {
                return;
            }

            // Create an empty ByteBuffer and overwrite the page at its location
            ByteBuffer emptyPage = ByteBuffer.allocate(pageSize);
            file.seek(offset);
            file.write(emptyPage.array());
            freePages.add(pageId);  // The page becomes free, so we add it to the free pages list

        } catch (IOException e) {
            throw new RuntimeException("Failed to delete page", e);
        }
    }
}
