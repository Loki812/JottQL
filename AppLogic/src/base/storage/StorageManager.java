package base.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import base.models.DataCatalog;
import base.models.TableSchema;
import base.models.Page;
import base.models.Record;

// Insert this record into this table function

// Write page, read page, insert record into page
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
     * @throws Exception If the file cannot be opened or created
     */
    public StorageManager(String filename) throws Exception {
        this.file = new RandomAccessFile(filename, "rw");
        this.catalog = DataCatalog.getInstance();
    }

    /**
     * Reads a page from disk.
     *
     * @param page The page to read
     * @return A byte array containing the page data
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the page does not exist
     */
    public byte[] readPage(Page page) throws IOException {
        int pageID = page.getId();
        int pageSize = catalog.getPageSize();
        byte[] buffer = new byte[pageSize];
        long offset = (long) pageID * pageSize;

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
     * @param page The page to write
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the page size does not match
     */
    public void writePage(Page page) throws IOException {
        int pageID = page.getId();
        int pageSize = catalog.getPageSize();

        byte[] pageData = new byte[pageSize];

        // Wrap buffer for easy writing
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(pageData);

        buffer.putInt(pageID);
        buffer.putInt(page.getNextPageId());
        buffer.putInt(page.recordList.size());

        for (Record record : page.recordList) {
            byte[] recordBytes = record.toBytes();

            if (buffer.position() + recordBytes.length > pageSize) {
                throw new IllegalArgumentException("Page overflow");
            }

            buffer.put(recordBytes);
        }

        long offset = (long) pageID * pageSize;
        file.seek(offset);
        file.write(pageData);
    }


    /**
     * Deletes the contents of a page.
     *
     * @param page The page to delete
     * @throws IllegalArgumentException If the page size does not match
     */
    public void deletePage(Page page) {
        try {
            int pageID = page.getId();
            int pageSize = catalog.getPageSize();
            long offset = (long) pageID * pageSize;

            if (offset >= file.length()) {
                throw new IllegalArgumentException("Page does not exist");
            }

            byte[] emptyPage = new byte[pageSize];
            file.seek(offset);
            file.write(emptyPage);

        } catch (IOException e) {
            throw new RuntimeException("Failed to delete page", e);
        }
    }

    // take pageID and record, get offset
    public void insertRecordIntoPage(Page page, Record record, TableSchema schema) {
        int pageID = page.getId();
        int recordSize = schema.getRecordSize();
    }
}
