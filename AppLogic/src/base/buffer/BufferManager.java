package base.buffer;
import base.models.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;

import base.models.Record;
import base.storage.StorageManager;

public class BufferManager {

    //max number of pages that can be in buffer before flushing
    private static int maxPageCount;
    private static HashMap<Integer,Page> buffer;
    private DataCatalog dataCatalog;
    private StorageManager storageManager;

    //todo make static instance of the BufferManager similar to how the bufferManager is done
    private static BufferManager bufferManager = null;


    // TODO follow singleton pattern for storage manager,
    // no need for field that isnt used
    /**
     * Creates an instance of the bufferManager class
     *
     *
     * @param maxPageCount the maximum number of pages that can be stored in memory, as Java Objects
     *                     at one time.
     * @param directory the directory the storage.bin file is stored in.
     */
    public BufferManager(int maxPageCount, String directory)  {
        BufferManager.maxPageCount = maxPageCount;
        this.storageManager = new StorageManager(directory+"/storage.bin/");
        buffer = new HashMap<>();
    }


    /**
     * getPage first checks if the id is valid, -1 indicating that it is invalid.
     * after it checks if the page is in the buffer.
     * if it is not in the buffer, it ejects a page from the buffer, before reading
     * the page from hardware.
     *
     * @param pageId the ID of the page
     * @return A Page Object with the given id, or null dependent on ID
     */
    public static Page getPage(int pageId){
        if(pageId==-1){
            return null;
        }

        if(buffer.containsKey(pageId)){
            Page page = buffer.get(pageId);
            page.timestamp = LocalDateTime.now();
            return page;
        }

        // Page not in buffer, fetch from disk.
        // TODO figure out error handling architecture
        try{
            flushOldestIfNeeded();
            Page decodedPage = readPageFromHardwareV2(pageId);
            buffer.put(decodedPage.pageId, decodedPage);
            decodedPage.timestamp = LocalDateTime.now();
            return decodedPage;
        } catch (IOException e){
            System.err.println("Inconsistent Database");
            throw new RuntimeException(e);
        }

    }

    /**
     * createNewPage first checks if there is space in the buffer.
     * after which it creates a new page and adds it to the buffer
     *
     * @param id the id of the new table
     * @param tableName the name of table this page belongs to
     * @return the newly created page object
     */
    public static Page createNewPage(int id, String tableName)  {
        try {
            flushOldestIfNeeded();
            Page page = new Page(id, tableName);
            buffer.put(page.pageId, page);
            return page;
        } catch (Exception e) {
            System.err.println("Failed to create page");
            throw new RuntimeException(e);
        }
    }

    /**
     * flushOldestIfNeeded checks the buffer size against the maxPageCount field.
     * if buffer is equal (or larger, which shouldn't occur), then the oldest page
     * is ejected from the buffer, clearing space
     *
     */
    public static void flushOldestIfNeeded() {
        if (buffer.size()>= maxPageCount){
            Page oldestPage = Collections.min(buffer.values(), Comparator.comparing(p -> p.timestamp));
            try {
                writePageToHardwareV2(oldestPage);
                buffer.remove(oldestPage.pageId);
            } catch (Exception e) {
                System.err.println("Failed to flush page from Buffer");
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * flushBuffer should only be called when exiting the database. It writes all existing
     * pages in the dataBase back to the disk.
     */
    public static void flushBuffer() {
        for(Page p : buffer.values()){
            try {
                writePageToHardwareV2(p);
            } catch (Exception e) {
                System.err.println("Failed to flush page id=" + p.pageId + " from Buffer");
                throw new RuntimeException(e);
            }

        }
    }

    //TODO refactor, this should call storage manager.

    /**
     *
     * @param pageId
     */
    public static void deletePage(int pageId){
        buffer.remove(pageId);
    }

    /**
     * An updated version of readPageFromHardWare utilizing ByteBuffer functionality and cutting
     * out unnecessary calls to Array.CopyOfRange()
     * Better performance and readability.
     *
     *
     * @param pageId the ID of the page you are requesting
     * @return a page object read from disk
     * @throws IOException thrown from storage manager, since this is a private function we pass it up.
     */
    private static Page readPageFromHardwareV2(int pageId) throws IOException {

        // TODO: remove this once design patterns sorted
        DataCatalog dc = DataCatalog.getInstance();

        // get byte array from hardware
        byte[] encodedByteArray = StorageManager.readPageV2(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(encodedByteArray);

        // instantiate page with first few fields
        int nextPageId = buffer.getInt();
        int tableNameLength = buffer.getInt();
        byte[] tableNameBytes = new byte[tableNameLength];
        buffer.get(tableNameBytes);
        int numOfRecords = buffer.getInt();

        String tableName = new String(tableNameBytes, StandardCharsets.UTF_8);
        Page p = new Page(pageId, tableName);
        p.nextPageId = nextPageId;

        ArrayList<AttributeSchema> attributes = new ArrayList<>(dc.getTableSchema(tableName)
                .getAttributeSchemas().sequencedValues());

        // process records until buffer is empty
        for (int i = 0; i < numOfRecords; i++) {
            Record record = convertBytesToRecordV2(buffer, attributes);
            p.recordList.add(record);
        }

        return p;
    }

    /**
     * An Updated Version of convertBytesToRecord that takes in a ByteBuffer instead of a
     * byte array. Greater performance and readability gains.
     *
     *
     * @param buffer A bytebuffer assumed to be at the start of the record. passed only by convertBytesToPage
     * @param attributes the list of attributes for the tableSchema
     * @return a record object
     */
    private static Record convertBytesToRecordV2(ByteBuffer buffer, ArrayList<AttributeSchema> attributes) {

        Record record = new Record();

        byte[] nullByteArray = new byte[attributes.size()];
        buffer.get(nullByteArray); // advances pointer and loads values into nullByte array

        for (int i = 0; i < nullByteArray.length; i++) {
            DataTypes d = attributes.get(i).getDataType();

            if (nullByteArray[i] == 1) {
                // if null set data field to null
                record.attributeList.add(new AttributeValue<>(null, d));
            } else {
                Object value = switch(d) {
                    case INTEGER -> buffer.getInt();
                    case DOUBLE -> buffer.getDouble();
                    case BOOLEAN -> buffer.get() == 1;
                    case CHAR, VARCHAR -> {
                        int len = buffer.getInt();
                        byte[] sBytes = new byte[len];
                        buffer.get(sBytes);
                        yield new String(sBytes, StandardCharsets.UTF_8);
                    }
                };
                record.attributeList.add(new AttributeValue<>(value, d));
            }
        }
        return record;
    }


    /**
     * Writes a page to disk, features readability and performance improvements from V1.
     *
     * @param page The page object we wish to write to disk
     */
    private static void writePageToHardwareV2(Page page) {
        //prevent a page from writing if it has not been modified
        if(!page.hasBeenModified){
            return;
        }

        DataCatalog dc = DataCatalog.getInstance();
        ByteBuffer byteBuffer = ByteBuffer.allocate(dc.getPageSize());


        try {
            byteBuffer.putInt(page.nextPageId);
            byte[] tableNameBytes = page.tableName.getBytes(StandardCharsets.UTF_8);
            byteBuffer.putInt(tableNameBytes.length);
            byteBuffer.put(tableNameBytes);

            // put in number of records, used for reading from hardware for loop counter
            byteBuffer.putInt(page.recordList.size());

            ArrayList<AttributeSchema> attributes = new ArrayList<>(dc.getTableSchema(page.tableName)
                    .getAttributeSchemas().sequencedValues());

            for (Record r : page.recordList) {
                convertRecordToBytesV2(r, attributes, byteBuffer);
            }

            byteBuffer.flip(); // resets position, clips length. prepares for it to be sent to storage manager
            StorageManager.writePage(page.pageId, byteBuffer);


        } catch (IOException e) {
            System.err.println("Error occurred while attempting to write page " + page.pageId + " to disk...");
            throw new RuntimeException(e);
        }

    }

    /**
     * Converts a Record object to bytes, to later be written to disk.
     *
     * @param r the record we wish to convert to bytes
     * @param attributes the list of attributes, and their corresponding DataTypes
     * @param buffer the bytebuffer, at a position ready for the record data to be written
     */
    private static void convertRecordToBytesV2(Record r, ArrayList<AttributeSchema> attributes, ByteBuffer buffer) {
        // write null bit array into buffer
        for (AttributeValue a : r.attributeList) {
            if (a.data == null) {
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0);
            }
        }

        for (int i = 0; i < attributes.size(); i++) {
            Object value = r.attributeList.get(i).data;
            if (value != null) {
                DataTypes dataType = attributes.get(i).getDataType();
                switch (dataType) {
                    case INTEGER -> buffer.putInt((int) value);
                    case DOUBLE -> buffer.putDouble((double) value);
                    // in-case value is Boolean and not boolean, haven't checked full codebase
                    case BOOLEAN -> buffer.put((byte) ((boolean) value ? 1 : 0));
                    case CHAR, VARCHAR -> {
                        String s = (String) value;

                        byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
                        // write the length of value
                        buffer.putInt(strBytes.length);

                        buffer.put(strBytes);
                    }
                }
            }
        }
    }


}