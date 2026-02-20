package base.parse.DDL;
import base.buffer.BufferManager;
import base.models.DataCatalog;
import base.models.Page;
import base.models.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;

public class DropTable {
    private static int COMMAND_LENGTH = 11;
    public static void parse(String command) throws Exception {
        command = command.substring(COMMAND_LENGTH);
        String table_Name = command.replace(";", "");
        DataCatalog dc = DataCatalog.getInstance();
        Page first = BufferManager.getPage(dc.getTableSchema(table_Name).getRootPageID());
        first.deleteTable();
        dc.removeTableSchema(table_Name);
    }
}
