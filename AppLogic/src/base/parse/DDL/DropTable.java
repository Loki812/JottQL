package base.parse.DDL;
import base.models.DataCatalog;

import java.util.ArrayList;
import java.util.Arrays;

public class DropTable {
    private static int COMMAND_LENGTH = 11;
    public static void parse(String command) throws Exception {
        command = command.substring(COMMAND_LENGTH);
        String table_Name = command.replace(";", "");
        //TODO Drop the table
        DataCatalog dc = DataCatalog.getInstance();
        dc.removeTableSchema(table_Name);

    }
}
