package base.parse.DDL;

import java.util.ArrayList;
import java.util.Arrays;

public class AtlerTable {
    private static int COMMAND_LENGTH = 12;
    public static void parse(String command) {
        command = command.substring(COMMAND_LENGTH);
        String table_Name = command.substring(0, command.indexOf(" "));
        command = command.substring(command.indexOf(" ") + 1);
        String operation = command.substring(0, command.indexOf(" "));
        command = command.substring(command.indexOf(" ") + 1);
        command = command.replace(";", "");
        if(operation.equals("ADD")) {
            ArrayList<String> column_components = new ArrayList<String>(Arrays.asList(command.split(" ")));
            String attribute = column_components.removeFirst();
            String type = column_components.removeFirst();
            ArrayList<String> constraints = column_components;
            //TODO Remake the table with new or altered column
        } else if(operation.equals("DROP")) {
            String attribute = command;
            //TODO remake the table with dropped column
        }
    }
}
