package base.parse.DDL;

import base.models.AttributeSchema;
import base.models.DataCatalog;
import base.models.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;

public class AtlerTable {
    private static int COMMAND_LENGTH = 12;
    public static void parse(String command) throws Exception {
        command = command.substring(COMMAND_LENGTH);
        String table_Name = command.substring(0, command.indexOf(" "));
        command = command.substring(command.indexOf(" ") + 1);
        String operation = command.substring(0, command.indexOf(" "));
        command = command.substring(command.indexOf(" ") + 1);
        command = command.replace(";", "");
        DataCatalog dc = DataCatalog.getInstance();
        TableSchema ts = dc.getTableSchema(table_Name);
        if(operation.equals("ADD")) {
            AttributeSchema attribute = AttributeSchema.createAttributeSchemaFromQuery(command);
            ts.addAttributeSchema(attribute);
        } else if(operation.equals("DROP")) {
            String attribute = command;
            ts.removeAttributeSchema(attribute);
        }
    }
}
