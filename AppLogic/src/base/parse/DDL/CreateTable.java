package base.parse.DDL;
import java.util.ArrayList;
import java.util.Arrays;

public class CreateTable {
    private static int COMMAND_LENGTH = 13;
    public static void parse(String command) {
        command = command.substring(COMMAND_LENGTH);
        String table_Name = command.substring(0, command.indexOf(" "));
        command = command.substring(command.indexOf(" ") + 1);
        command = command.replace("(", "").replace(")", "").replace(";", "");
        ArrayList<String> columns = new ArrayList<String>(Arrays.asList(command.split(",")));
        for (String column : columns) {
            ArrayList<String> column_components = new ArrayList<String>(Arrays.asList(column.split(" ")));
            String attribute = column_components.removeFirst();
            String type = column_components.removeFirst();
            ArrayList<String> constraints = column_components;
            //TODO Do something with the values to make the table
        }

    }
}
