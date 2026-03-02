package base.storage;
import base.models.DataCatalog;

import java.io.FileNotFoundException;
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

    /**
     * Create a new StorageManager instance.
     *
     * @param filename The database file name
     */
    public StorageManager(String filename) {
        try {
            StorageManager.file = new RandomAccessFile(filename, "rw");
            StorageManager.catalog = DataCatalog.getInstance();
            StorageManager.freePages = new ArrayList<>();
        } catch (FileNotFoundException e) {
            System.err.println("Database File could not be created or found:" + e);
            System.err.println("Exiting Gracefully...");
            System.exit(1);
        }
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
     * Version 2 of read page from disk. Instead of dynamically calculating how many bytes to pull,
     * readPageV2 simply reads all 'x' bytes from disk starting at offset. Buffermanager then uses
     * metadata from the header to determine when the real data ends and padding starts.
     *
     * @param pageId the id of the page we want
     * @return a byte array read directly from disk
     * @throws IOException if the disk fails
     */
    public static byte[] readPageV2(int pageId) throws IOException {
        int pageSize = catalog.getPageSize();
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
     */
    public static void deletePage(int pageId) throws IOException {
        int pageSize = catalog.getPageSize();
        long offset = (long) pageId * pageSize;

        // TODO: if this is called and the page isnt found,
        // should throw and error?
        if (offset >= file.length()) {
            return;
        }

        // Create an empty ByteBuffer and overwrite the page at its location
        ByteBuffer emptyPage = ByteBuffer.allocate(pageSize);
        file.seek(offset);
        file.write(emptyPage.array());
        freePages.add(pageId);  // The page becomes free, so we add it to the free pages list
    }
}
