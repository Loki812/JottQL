package base.parse.DDL;
import base.models.DataCatalog;
import base.models.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;

public class CreateTable {
    private static int COMMAND_LENGTH = 13;
    public static void parse(String command) throws Exception {
        command = command.substring(COMMAND_LENGTH); // strip command
        String table_Name = command.substring(0, command.indexOf(" "));
        if(command.charAt(command.length()-1) == ';' && command.charAt(command.length()-2) == ')' ){
            command = command.substring(command.indexOf(" ") + 1,command.length()-2); //remove table name
            command = command.replace("(", "");
            ArrayList<String> columns = new ArrayList<String>(Arrays.asList(command.split(",")));
            TableSchema ts = TableSchema.createTableSchemaFromQuery(table_Name, columns);
            DataCatalog dc = DataCatalog.getInstance();
            dc.addTableSchema(ts);
        }else {
            System.out.println("ending not valid");
            System.out.println(command);
            throw new Exception();
        }

    }
}
