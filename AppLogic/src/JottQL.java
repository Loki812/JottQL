import base.buffer.BufferManager;
import base.models.DataCatalog;
import base.models.schemas.TableSchema;
import base.parse.DDL.AtlerTable;
import base.parse.DDL.CreateTable;
import base.parse.DDL.DropTable;
import base.parse.DML.ParserHelpers.UpdateTable;
import base.parse.DML.SelectTable;
import base.parse.DML.DeleteRows;

import java.io.File;

import java.util.Scanner;

public class JottQL {


    public static void parseCommand(String command) throws Exception {
        String firstWord = command.split(" ")[0].toUpperCase();

        switch (firstWord) {
            case "CREATE" -> {
                try {
                    CreateTable.execute(command);
                    System.out.println("Table Created Successfully");
                }catch (Exception e){
                    System.err.println("Table Creation Failed");
                }
            }
            case "SELECT" -> {
                try {
                    SelectTable.parse(command);
                }catch (Exception e){
                    System.err.println("Table Select Failed" + e);
                }
            }
            case "DELETE" -> {
                try {
                    DeleteRows.execute(command);
                    System.out.println("Rows Deleted Successfully");
                }catch (Exception e){
                    System.out.println("Rows Failed to Delete");
                }
            }
            case "INSERT" -> {
                try {
                    base.parse.DML.InsertTable.parse(command);
                }catch (Exception e){
                    System.err.println("Table insertion failed " + e);
                }
            }
            case "DROP" -> {
                try {
                    DropTable.execute(command);
                    System.out.println("Table Drop Successfully");
                }catch (Exception e){
                    System.out.println("Table Drop Failed");
                }
            }
            case "ALTER" -> {
                try {
                    AtlerTable.execute(command);
                    System.out.println("Table Alter Successfully");
                }catch (Exception e) {
                    System.out.println("Table Alter Failed");
                }
            }
            case "UPDATE" -> {
                try {
                    UpdateTable.parse(command);
                    System.out.println("Update Table Successfully");
                } catch (Exception e) {
                    System.out.println("Update Table Failed");
                }
            }
            default -> System.err.println("Unrecognized Query, please try again.");
        }
    }

    /**
     * Main(): Runs an infinite command loop until the user inputs the exit command
     *
     * Args: - dbDirectory - the relative or absolute path to the data directory ex. "../../Data/"
     *       - pageSize - the number of bytes allocated per page, only used upon db instantiation, else ignored
     *       - bufferSize - the number of pages that are allowed in the buffer at one point in time
     *       - indexing - will indexing be used True or False
     *
     * **/
    public static void main(String[] args) throws Exception {

        // Check if exists build File, if not create new file.
        File dbFile = new File(args[0], "catalog.bin");
        // docs say it only creates new file if file existed before
        // therefore we don't have to check if it exists beforehand
        boolean madeNew = dbFile.exists();
        if (madeNew) {
            System.out.println("Database found at " + args[0] + " initializing...");
        } else {
            System.out.println("No file found at " + args[0] + " creating new file...");
        }
        // build data catalog with page-size and data directory
        DataCatalog.buildCatalog(Integer.parseInt(args[1]), args[0], Boolean.parseBoolean(args[3]));
        DataCatalog dc = DataCatalog.getInstance();
        BufferManager bm = BufferManager.buildBufferManager(Integer.parseInt(args[2]),args[0]);

        Scanner scanner = new Scanner(System.in);

        //infinite command-line loop

        System.out.println("Welcome to JottQL!");

        while(true){

            StringBuilder fullQuery = new StringBuilder();
            String input = scanner.nextLine();

            if (input.equalsIgnoreCase("<QUIT>")) {
                TableSchema.deleteTemps();
                DataCatalog.saveToDisk();
                BufferManager.getInstance().flushBuffer();
                System.out.println("Exiting Application");
                System.exit(0);
            }

            while (!input.endsWith(";")) {
                fullQuery.append(input).append(" ");
                input = scanner.nextLine();
            }

            // grab the final string that ends with ';' from scanner
            fullQuery.append(input);

            parseCommand(fullQuery.toString().toUpperCase());

        }




    }
}