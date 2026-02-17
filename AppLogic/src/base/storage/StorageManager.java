package base.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import base.models.DataCatalog;

// Insert this record into this table function

// Write page, read page, insert record into table
// Get a table schema, get record object, use tableschema.lastPage field to get page ID, go to that offset in DB file
// and write to file

// Job is to loop through all pages and find where they belong

// Delete/update functions LATER

// SPLIT PAGES

// StorageManager should be very dumb for the first phase.

/**
 * The StorageManager handles low-level disk I/O operations.
 */
public class StorageManager {
    private final RandomAccessFile file;
    private final DataCatalog catalog;

    /**
     * Creates a new StorageManager instance.
     *
     * @param filename The database file name
     * @param pageSize The size of each page (currently obtained from DataCatalog)
     * @throws Exception If the file cannot be opened or created
     */
    public StorageManager(String filename, int pageSize) throws Exception {
        this.file = new RandomAccessFile(filename, "rw");
        this.catalog = DataCatalog.getInstance();
    }

    /**
     * Reads a page from disk.
     *
     * @param pageNum The page number to read
     * @return A byte array containing the page data
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the page does not exist
     */
    public byte[] readPage(int pageNum) throws IOException {
        int pageSize = catalog.getPageSize();
        byte[] buffer = new byte[pageSize];
        long offset = (long) pageNum * pageSize;

        if (offset >= file.length()) {
            throw new IllegalArgumentException("Page does not exist");
        }

        file.seek(offset);
        file.readFully(buffer);
        return buffer;
    }

    /**
     * Writes a page to disk.
     *
     * @param pageNum The page number to write
     * @param pageData The page data to write
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the page size does not match
     */
    public void writePage(int pageNum, byte[] pageData) throws IOException {
        int pageSize = catalog.getPageSize();

        if (pageData.length != pageSize) {
            throw new IllegalArgumentException("Page size mismatch");
        }

        long offset = (long) pageNum * pageSize;
        file.seek(offset);
        file.write(pageData);
    }
}
