import base.models.DataCatalog;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {




        Scanner scanner = new Scanner(System.in);

        //infinite command-line loop

        //todo call the Datacatalog.buildCatalog() function
        //todo call the BufferManager.buildBufferManager

        System.out.println("Welcome to JottQL!");
        DataCatalog.buildCatalog();

        while(true){



            /*
            todo read the first word from command-line inputs to decide
            what functions it should be sent to
             */

            //todo do not instantiate DDL and DML parsers
            //todo pass the full query-string to the DDLand DML parsers' static functions

        }




    }
}