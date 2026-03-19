package base.parse.DDL;
import base.buffer.BufferManager;
import base.models.DataCatalog;
import base.models.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;

public class CreateTable {
    public static void execute(String command) throws Exception {
        String trimmedCommand = command.trim().toUpperCase();
        if(!trimmedCommand.startsWith("CREATE TABLE ")) {
            System.out.println("Invalid CREATE TABLE Command");
            throw new Exception();
        }
        if(!trimmedCommand.endsWith(";")) {
            System.out.println("Missing Semicolon");
            throw new Exception();
        }
        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        command = trimmedCommand.substring("CREATE TABLE ".length()).trim();
        // strip command
        String table_Name = command.substring(0, command.indexOf(" ")).toUpperCase();
        if(command.charAt(command.length()-1) == ')' ){
            command = command.substring(command.indexOf(" "),command.length()-1).trim(); //remove table name
            if(!command.startsWith("(")) {
                System.out.println("Missing opening parentheses");
                throw new Exception();
            }
            command = command.replaceFirst("\\(", "");
            ArrayList<String> columns = new ArrayList<String>(Arrays.asList(command.split(",")));
            TableSchema ts = TableSchema.createTableSchemaFromQuery(table_Name, columns);
            DataCatalog dc = DataCatalog.getInstance();
            BufferManager bm = BufferManager.getInstance();
            dc.addTableSchema(ts);
            bm.createNewPage(ts.rootPageID, ts.tableName);

        }else {
            System.out.println("Missing closing parentheses");
            throw new Exception();
        }
    }
}
