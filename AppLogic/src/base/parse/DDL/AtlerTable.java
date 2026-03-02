package base.parse.DDL;

import base.buffer.BufferManager;
import base.models.AttributeSchema;
import base.models.DataCatalog;
import base.models.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;

public class AtlerTable {
    public static void execute(String command) throws Exception {
        String trimmedCommand = command.trim();
        if(!trimmedCommand.startsWith("ALTER TABLE ")) {
            System.out.println("Invalid ALTER TABLE Command");
            throw new Exception();
        }
        if(!trimmedCommand.endsWith(";")) {
            System.out.println("Missing Semicolon");
            throw new Exception();
        }
        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        command = trimmedCommand.substring("ALTER TABLE ".length()).trim();
        String table_Name = command.substring(0, command.indexOf(" ")).toUpperCase();
        DataCatalog dc = DataCatalog.getInstance();
        TableSchema ts = dc.getTableSchema(table_Name);
        if(ts == null) {
            System.out.println("Table " + table_Name + " not found");
            throw new Exception();
        }
        command = command.substring(command.indexOf(" ") + 1);
        String operation = command.substring(0, command.indexOf(" "));
        command = command.substring(command.indexOf(" ") + 1);

        if(operation.equals("ADD")) {
            AttributeSchema attribute = AttributeSchema.createAttributeSchemaFromQuery(command);
            ts.addAttributeSchema(attribute);
        } else if(operation.equals("DROP")) {
            String attribute = command;
            ts.removeAttributeSchema(attribute);
        } else{
            System.out.println("Invalid Operation");
            throw new Exception();
        }
    }
}
