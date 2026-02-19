package base.models;
import java.io.*;
import java.util.*;
import java.nio.file.*;

public class DataCatalog {

    private static DataCatalog catalog = null;

    private String dataDirectory;

    /* ---- In order they are written to catalog.bin file */

    int MAGIC_NUMBER = 323574324; // used to verify file is a data catalog
    private int pageSize; // represent in terms of bytes?
    private int tableCount; // in header of bin file, needed for extracting
    private int nextAvailablePageID; //  if free list is empty, use this offset
    private List<Integer> freePageList; // List of free page IDs within the DB
    private Map<String, TableSchema> tables;


    private DataCatalog() {}

    public static synchronized DataCatalog getInstance() throws Exception {
        if (catalog == null) {
            throw new Exception("Catalog was not built before attempting use...");
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
    public static void buildCatalog(Integer suggestedSize, String dataDirectory) {

        catalog = new DataCatalog();
        File catalogFile = new File(dataDirectory, "catalog.bin");

        if (catalogFile.exists()) {
            // Load existing catalog
            try (DataInputStream in  = new DataInputStream(new FileInputStream(catalogFile))) {
                loadFromDisk(in);
            } catch (FileNotFoundException e) {
                System.err.println("File not found (this shouldn't happen):" + e.getMessage());
            } catch (IOException e) {
                System.err.println("Failed to load catalog: " + e.getMessage());
            }


        } else {
            // Build new one at given directory
            catalog.dataDirectory = dataDirectory;

            catalog.pageSize = suggestedSize;
            catalog.tableCount = 0;
            catalog.nextAvailablePageID = 0;
            catalog.freePageList = new ArrayList<>();
            catalog.tables = new HashMap<String, TableSchema>();
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
        catalog.nextAvailablePageID = in.readInt();

        catalog.freePageList = new ArrayList<>();
        int freePageListSize = in.readInt();
        for (int i = 0; i < freePageListSize; i++) {
            catalog.freePageList.add(in.readInt());
        }

        catalog.tables = new HashMap<String, TableSchema>();
        for (int i = 0; i < catalog.tableCount; i++) {
            TableSchema ts = TableSchema.createTableSchemaFromDisk(in);
            catalog.tables.put(ts.tableName, ts);
        }

    }

    /**
     * Function: saveToDisk should be called whenever changes are made to the data catalog
     *      ie. dropping a table, editing a table.
     */
    private static void saveToDisk() {
        try {
            Files.createDirectories(Paths.get(catalog.dataDirectory));
            File catalogFile = new File(catalog.dataDirectory, "catalog.bin");
            DataOutputStream out = new DataOutputStream(new FileOutputStream(catalogFile));

            out.writeInt(catalog.MAGIC_NUMBER);
            out.writeInt(catalog.pageSize);
            out.writeInt(catalog.tableCount);
            out.writeInt(catalog.nextAvailablePageID);
            out.writeInt(catalog.freePageList.size());

            for (int i = 0; i < catalog.freePageList.size(); i++) {
                out.writeInt(catalog.freePageList.get(i));
            }

            for (TableSchema t : catalog.tables.values()) {
                t.saveTableSchemaToDisk(out);
            }


        } catch (IOException e) {
            System.err.println("Error Occured while saving DataCatalog: " + e.getMessage());
        }
    }

    /* ------- HELPER FUNCTIONS -------- */

    public int getPageSize() {
        return catalog.pageSize;
    }

    public TableSchema getTableSchema(String tableName) {
        return catalog.tables.get(tableName);
    }

    public void removeTableSchema(String tableName) {
        catalog.tableCount -= 1;
        catalog.tables.remove(tableName);
        // wherever this is called, ensure saveToDisk() is called
    }

    public void addTableSchema(TableSchema schema) {
        catalog.tables.put(schema.tableName, schema);
        catalog.tableCount += 1;
    }

    /**
     *
     * @return the lowest indexed free page, first checks the free page list. if it is empty it 'allocates' a new page
     */
    public int getNextAvailablePageID() {
        if (catalog.freePageList.isEmpty()) {
            int next = catalog.nextAvailablePageID;
            catalog.nextAvailablePageID += 1;
            return next;
        } else {
            return catalog.freePageList.removeFirst();
        }
    }

}
