package base.models;
import base.buffer.BufferManager;
import base.models.concrete.IndexPage;
import base.models.concrete.Page;
import base.models.schemas.AttributeSchema;
import base.models.schemas.IndexSchema;
import base.models.schemas.TableSchema;

import java.io.*;
import java.util.*;

public class DataCatalog {

    private static DataCatalog catalog = null;

    private String dataDirectory;

    /* ---- In order they are written to catalog.bin file */

    int MAGIC_NUMBER = 323574324; // used to verify file is a data catalog
    private int pageSize; // represent in terms of bytes?
    private int tableCount; // in header of bin file, needed for extracting
    private int nextAvailablePageID; //  if free list is empty, use this offset
    private static ArrayList<Integer> freePageList; // List of free page IDs within the DB
    private Map<String, TableSchema> tables;
    private int indexCount;
    private Map<String, IndexSchema> indexes;
    public boolean indexOn;


    private DataCatalog() {}

    public static synchronized DataCatalog getInstance() {
        if (catalog == null) {
            System.err.println("Catalog was not built before attempting use...");
            System.exit(1);
        }
        return catalog;
    }

    /**
     * Function: buildCatalog - called to instantiate the dataCatalog, must be run before
     *  any user input is allowed or calls to DataCatalog.getInstance().
     *
     * @param suggestedSize - received from CLI, denoted pageSize. Is ignored is catalog exists.
     * @param dataDirectory - the path to the catalog.bin file.
     *
     **/
    public static void buildCatalog(Integer suggestedSize, String dataDirectory, boolean indexOn) {

        catalog = new DataCatalog();
        File catalogFile = new File(dataDirectory, "catalog.bin");

        if (catalogFile.exists()) {
            catalog.dataDirectory = dataDirectory;
            // Load existing catalog
            try (DataInputStream in  = new DataInputStream(new FileInputStream(catalogFile))) {
                loadFromDisk(in);
            } catch (FileNotFoundException e) {
                System.err.println("File not found (this shouldn't happen):" + e.getMessage());
                System.exit(1);
            } catch (IOException e) {
                System.err.println("Failed to load catalog: " + e.getMessage());
                System.exit(1);
            }


        } else {
            // Build new one at given directory
            catalog.dataDirectory = dataDirectory;
            catalog.pageSize = suggestedSize;
            catalog.tableCount = 0;
            catalog.nextAvailablePageID = 0;
            catalog.freePageList = new ArrayList<>();
            catalog.tables = new HashMap<String, TableSchema>();
            catalog.indexCount = 0;
            catalog.indexes = new HashMap<String, IndexSchema>();
            catalog.indexOn = indexOn;
            saveToDisk();


        }
    }

    /**
     * Function: loadFromDisk should be called once when building the catalog
     *      upon program startup.
     *
     * @param in input data stream passed through the buildCatalog() function.
     * @throws IOException if bin file does not start with magic number, it is discarded.
     */
    private static void loadFromDisk(DataInputStream in) throws IOException {
        // Verify file using magic number
        int fileMagicNumber = in.readInt();
        if (fileMagicNumber != catalog.MAGIC_NUMBER) {
            throw new IOException("Magic number mismatch! This file is not a valid database catalog.");
        }
        catalog.pageSize = in.readInt();
        catalog.tableCount = in.readInt();
        catalog.indexCount = in.readInt();
        catalog.nextAvailablePageID = in.readInt();
        catalog.indexOn = in.readBoolean();

        freePageList = new ArrayList<>();
        int freePageListSize = in.readInt();
        for (int i = 0; i < freePageListSize; i++) {
            freePageList.add(in.readInt());
        }

        catalog.tables = new HashMap<String, TableSchema>();
        for (int i = 0; i < catalog.tableCount; i++) {
            TableSchema ts = TableSchema.createTableSchemaFromDisk(in);
            catalog.tables.put(ts.tableName, ts);
        }

        catalog.indexes = new HashMap<String, IndexSchema>();
        for (int i = 0; i < catalog.indexCount; i++) {
            IndexSchema idx = IndexSchema.createIndexSchemaFromDisk(in);
            String key = idx.tableName + "." + idx.columnName;
            catalog.indexes.put(key, idx);
        }

    }

    /**
     * Function: saveToDisk should be called whenever changes are made to the data catalog
     *      ie. dropping a table, editing a table.
     */
    public static void saveToDisk() {
        try {
            File catalogFile = new File(catalog.dataDirectory, "catalog.bin");

            DataOutputStream out = new DataOutputStream(new FileOutputStream(catalogFile));

            out.writeInt(catalog.MAGIC_NUMBER);
            out.writeInt(catalog.pageSize);
            out.writeInt(catalog.tableCount);
            out.writeInt(catalog.indexCount);
            out.writeInt(catalog.nextAvailablePageID);
            out.writeBoolean(catalog.indexOn);
            out.writeInt(freePageList.size());

            for (int i = 0; i < freePageList.size(); i++) {
                out.writeInt(freePageList.get(i));
            }

            for (TableSchema t : catalog.tables.values()) {
                t.saveTableSchemaToDisk(out);
            }

            for (IndexSchema i : catalog.indexes.values()) {
                i.saveIndexSchemaToDisk(out);
            }


        } catch (IOException e) {
            System.err.println("Error occurred while saving DataCatalog: " + e.getMessage());
        }
    }

    /* ------- HELPER FUNCTIONS -------- */

    public int getPageSize() {
        return catalog.pageSize;
    }

    public TableSchema getTableSchema(String tableName) {
        return catalog.tables.get(tableName);
    }
    public IndexSchema getIndexSchema(String tableName, String columnName) {
        String key = tableName + "." + columnName;
        return catalog.indexes.get(key);
    }

    /**
     * removes a tableschema from the database
     *
     * @param tableName the name of the table you are adding
     */
    public void removeTableSchema(String tableName) {
        catalog.tableCount -= 1;
        catalog.tables.remove(tableName);
    }

    public void removeIndexSchema(String tableName, String columnName) {
        String key = tableName + "." + columnName;
        if (catalog.indexes.remove(key) != null) {
            catalog.indexCount -= 1;
        }
    }

    /**
     *
     * @param schema the table schema you are adding to the database
     * @throws Exception if the given schema shares a name with a database already inside the disk.
     */
    public void addTableSchema(TableSchema schema) throws Exception {
        BufferManager bufferManager = BufferManager.getInstance();
        if(catalog.tables.containsKey(schema.tableName)){
            System.out.println("Table already exists: " + schema.tableName);
            throw new Exception();
        }
        schema.rootPageID = getNextAvailablePageID();
        catalog.tables.put(schema.tableName, schema);
        bufferManager.createNewDataPage(schema.rootPageID, schema.tableName);
        catalog.tableCount += 1;
    }

    public void addIndexSchema(IndexSchema schema) throws Exception {

        BufferManager bufferManager = BufferManager.getInstance();

        String key = schema.tableName + "." + schema.columnName;

        if (catalog.indexes.containsKey(key)) {
            System.out.println("Index already exists: " + key);
            throw new Exception();
        }

        schema.rootPageID = getNextAvailablePageID();

        catalog.indexes.put(key, schema);

        TableSchema ts = tables.get(schema.tableName);
        AttributeSchema searchKey = ts.getAttributeSchemas().get(schema.columnName);

        bufferManager.createNewIndexPage(
                schema.rootPageID,
                schema.tableName,
                searchKey,
                -1
        );

        IndexPage ip = (IndexPage)bufferManager.getPageV2(schema.rootPageID);

        ip.childPointers.add(ts.rootPageID);

        catalog.indexCount += 1;
    }

    /**
     *
     * @return the lowest indexed free page, first checks the free page list. if it is empty it 'allocates' a new page
     */
    public int getNextAvailablePageID() {
        if (freePageList.isEmpty()) {
            int next = catalog.nextAvailablePageID;
            catalog.nextAvailablePageID += 1;
            return next;
        } else {
            return freePageList.removeFirst();
        }
    }

    public ArrayList<String> tempTables() {
        ArrayList<String> arr = new ArrayList<>();
        for (TableSchema t : tables.values()) {
            if (t.tableName.startsWith("##temp")) {
                arr.add(t.tableName);
            }
        }
        return arr;
    }

    /**
     * used to change a temporary table into the original table
     * @param ogName the original table name
     * @param copyName the copied table name
     * @throws IOException if the ogtTable does not exist
     */
    public void changeTableName(String ogName, String copyName) throws IOException {
        if (indexOn) {
            changeIndexName(ogName, copyName);
        } else {
            BufferManager bm = BufferManager.getInstance();
            // Drop the original tableSchema
            bm.deleteTable(ogName);

            // Get the root page ID of the copy
            TableSchema copy = getTableSchema(copyName);
            int copyRootPageId = copy.getRootPageID();

            // Get the page associated with the copy's root page ID
            Page page = (Page) bm.getPageV2(copyRootPageId);

            // Change all the copy's page's tableNames to the original tableName
            while (page.nextPageId != -1) {
                page.tableName = ogName;
                page = (Page) bm.getPageV2(page.nextPageId);
            }

            // Put the copy in the tables map
            catalog.tables.put(ogName, copy);
            //remove the copy
            catalog.tables.remove(copyName);
            catalog.tableCount -= 1;
        }
    }

    public void changeIndexName(String ogName, String copyName) throws IOException {
        BufferManager bm = BufferManager.getInstance();

        // Drop original indexSchema
        bm.deleteIndex(ogName);

        ArrayList<IndexSchema> indexes = new ArrayList<>();

        TableSchema copyTs = getTableSchema(copyName);

        for (AttributeSchema as : copyTs.getAttributeSchemas().values()) {
            IndexSchema is = getIndexSchema(copyName, as.attributeName);
            if (is != null) {
                indexes.add(is);
            }
        }

        for (IndexSchema is : indexes) {
            IndexPage ip = (IndexPage)bm.getPageV2(is.rootPageID);
            ip.changeTableName(copyName);
        }
    }

}
