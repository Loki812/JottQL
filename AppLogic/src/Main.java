import base.models.DataCatalog;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

import java.util.Scanner;

public class Main {
    /**
     * Main(): Runs an infinite command loop until the user inputs the exit command
     *
     * Args: - dbDirectory - the relative or absolute path to the data directory ex. "../../Data/"
     *       - pageSize - the number of bytes allocated per page, only used upon db instantiation, else ignored
     *       - bufferSize - the number of pages that are allowed in the buffer at one point in time
     *       - indexing - will indexing be used True or False
     *
     * **/
    public static void main(String[] args)  {

        // Check if exists build File, if not create new file.
        try {
            File dbFile = new File(args[0] + "db.bin");
            // docs say it only creates new file if file existed before
            // therefore we don't have to check if it exists beforehand
            boolean madeNew = dbFile.createNewFile();
            if (madeNew) {
                System.out.println("Database found at " + args[0] + " initializing...");
            } else {
                System.out.println("No file found at " + args[0] + " creating new file...");
            }
        } catch (IOException e) {
            System.err.println("An error occurred while attempting to initialize database: " + e);
            System.exit(1);
        }

        // build data catalog with page-size and data directory
        DataCatalog.buildCatalog(Integer.parseInt(args[1]), args[0]);
        DataCatalog dc = DataCatalog.getInstance();

        // BufferManager.buildBuffer(Integer.parseInt(args[2]))
        // BufferManager bm = BufferManager.getInstance()

        Scanner scanner = new Scanner(System.in);

        //infinite command-line loop

        //todo call the Datacatalog.buildCatalog() function
        //todo call the BufferManager.buildBufferManager

        System.out.println("Welcome to JottQL!");

        while(true){
            String input = scanner.nextLine();

            String firstWord = input.split(" ")[0].toUpperCase();

            switch (firstWord) {
                case "CREATE" -> System.out.println("Creating Table...");
                case "SELECT" -> System.out.println("Selecting something...");
                case "INSERT" -> System.out.println("Inserting Something...");
                case "DROP" -> System.out.println("Dropping a Table...");
                case "ALTER" -> System.out.println("Altering a Table...");
                case "EXIT" -> {
                    DataCatalog.saveToDisk();
                    // BufferManager (save before exiting_)
                    System.out.println("Exiting Application");
                    System.exit(0);
                }
                default -> System.out.println("Unrecognized query, please retry.");
            }


        }




    }
}