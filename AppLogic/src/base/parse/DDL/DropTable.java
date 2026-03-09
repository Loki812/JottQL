package base.parse.DDL;
import base.buffer.BufferManager;
import base.models.DataCatalog;
import base.models.Page;
import base.models.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;

public class DropTable {
    public static void execute(String command) throws Exception {

        BufferManager bm = BufferManager.getInstance();
        DataCatalog dc = DataCatalog.getInstance();

        String trimmedCommand = command.trim();
        if(!trimmedCommand.startsWith("DROP TABLE ")) {
            System.out.println("Invalid DROP TABLE Command");
            throw new Exception();
        }
        if(!trimmedCommand.endsWith(";")) {
            System.out.println("Missing Semicolon");
            throw new Exception();
        }
        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        String table_Name = trimmedCommand.substring("DROP TABLE ".length()).trim().toUpperCase();

        dc.removeTableSchema(table_Name);
        bm.deleteTable(table_Name);
    }
}
