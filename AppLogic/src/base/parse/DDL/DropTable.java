package base.parse.DDL;
import java.util.ArrayList;
import java.util.Arrays;

public class DropTable {
    private static int COMMAND_LENGTH = 11;
    public static void parse(String command) {
        command = command.substring(COMMAND_LENGTH);
        String table_Name = command.replace(";", "");
        //TODO Drop the table

    }
}
