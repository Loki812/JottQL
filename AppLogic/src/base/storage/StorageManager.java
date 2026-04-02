package base.storage;
import base.models.DataCatalog;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * The StorageManager handles low-level disk I/O operations.
 */
public class StorageManager {
    private RandomAccessFile file;
    private ArrayList<Integer> freePages;

    // singleton class style
    private static StorageManager instance;



    /**
     * Create a new StorageManager instance.
     *
     * @param directory The database file directory
     */
    private StorageManager(String directory) {
        try {
            file = new RandomAccessFile(directory+"/storage.bin", "rw");
            freePages = new ArrayList<>();
        } catch (FileNotFoundException e) {
            System.err.println("Database File could not be created or found:" + e);
            System.err.println("Exiting Gracefully...");
            System.exit(1);
        }
    }

    public static StorageManager buildStorageManager(String directory) {
        if (instance == null) {
            instance = new StorageManager(directory);
            return instance;
        }
        return instance;
    }

    public static StorageManager getInstance() {
        if (instance == null) {
            throw new RuntimeException("Storage Manager not instantiated");
        }
        return instance;
    }



    /**
     * Version 2 of read page from disk. Instead of dynamically calculating how many bytes to pull,
     * readPageV2 simply reads all 'x' bytes from disk starting at offset. Buffermanager then uses
     * metadata from the header to determine when the real data ends and padding starts.
     *
     * @param pageId the id of the page we want
     * @return a byte array read directly from disk
     * @throws IOException if the disk fails
     */
    public byte[] readPageV2(int pageId) throws IOException {
        DataCatalog dc = DataCatalog.getInstance();

        int pageSize = dc.getPageSize();
        long offset = (long) pageId * pageSize;

        if (offset >= file.length()) {
            throw new IllegalArgumentException("Page does not exist");
        }

        // always read full page, no dynamically calculating how much to read
        // massive performance gains
        byte[] fullPage = new byte[pageSize];

        file.seek(offset);
        file.readFully(fullPage);

        return fullPage;
    }

    /**
     * Write a page to disk.
     *
     * @param pageId The ID of the page to write
     * @param pageData The page data to write to disk
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the page size does not match
     */
    public void writePage(int pageId, ByteBuffer pageData) throws IOException {

        DataCatalog dc = DataCatalog.getInstance();

        int pageSize = dc.getPageSize();

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
     */
    public void deletePage(int pageId) throws IOException {

        DataCatalog dc = DataCatalog.getInstance();

        int pageSize = dc.getPageSize();
        long offset = (long) pageId * pageSize;

        // TODO: if this is called and the page isnt found,
        // should throw and error?
        if (offset >= file.length()) {
            throw new RuntimeException("Attempted to delete page that did not exist...");
        }

        // Create an empty ByteBuffer and overwrite the page at its location
        ByteBuffer emptyPage = ByteBuffer.allocate(pageSize);
        file.seek(offset);
        file.write(emptyPage.array());
        freePages.add(pageId);  // The page becomes free, so we add it to the free pages list
    }
}
