package base.models;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.nio.file.*;

public class DataCatalog {

    private static DataCatalog catalog = null;

    int MAGIC_NUMBER = 323574324;
    int pageSize; // represent in terms of bytes?
    int tableCount; // in header of bin file, needed for extracting
    Map<String, TableSchema> tables;
    String dataDirectory;

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
            try {
                catalog.pageSize = suggestedSize;
                catalog.dataDirectory = dataDirectory;
                catalog.tableCount = 0;
                catalog.tables = new HashMap<String, TableSchema>();

                saveToDisk();

            } catch (IOException e) {
                System.err.println("Failed to save catalog: " + e.getMessage());
            }
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
        catalog.tables = new HashMap<String, TableSchema>();

        // TODO Loop over tableschemas, attribute schemas
    }

    /**
     * Function: saveToDisk should be called whenever changes are made to the data catalog
     *      ie. dropping a table, editing a table.
     */
    private static void saveToDisk() throws IOException {
        Files.createDirectories(Paths.get(catalog.dataDirectory));
        File catalogFile = new File(catalog.dataDirectory, "catalog.bin");

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(catalogFile))) {
            out.writeInt(catalog.MAGIC_NUMBER);
            out.writeInt(catalog.pageSize);
            out.writeInt(catalog.tableCount);

            // TODO Loop over tableschemas, attribute schemas
        }
    }

}
