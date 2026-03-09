package base.parse.DDL;

import base.buffer.BufferManager;
import base.models.AttributeSchema;
import base.models.AttributeValue;
import base.models.DataCatalog;
import base.models.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;

public class AtlerTable {
    public static void execute(String command) throws Exception {
        String trimmedCommand = command.trim();
        if(!trimmedCommand.startsWith("ALTER TABLE ")) {
            System.err.println("Invalid ALTER TABLE Command");
            throw new Exception();
        }
        if(!trimmedCommand.endsWith(";")) {
            System.err.println("Missing Semicolon");
            throw new Exception();
        }

        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        command = trimmedCommand.substring("ALTER TABLE ".length()).trim();
        String table_Name = command.substring(0, command.indexOf(" ")).toUpperCase();
        DataCatalog dc = DataCatalog.getInstance();
        TableSchema ts = dc.getTableSchema(table_Name);
        BufferManager bm = BufferManager.getInstance();
        if(ts == null) {
            System.err.println("Table " + table_Name + " not found");
            throw new Exception();
        }
        command = command.substring(command.indexOf(" ") + 1);
        String operation = command.substring(0, command.indexOf(" "));
        command = command.substring(command.indexOf(" ") + 1);

        if(operation.equals("ADD")) {
            // build schema and add to data catalog
            AttributeSchema attribute = AttributeSchema.createAttributeSchemaFromQuery(command);
            ts.addAttributeSchema(attribute);
            // build default value and add to buffer manager
            AttributeValue defaultValue = new AttributeValue<>(attribute.getDefaultVal(), attribute.getDataType());
            bm.addColumn(defaultValue, ts.tableName);
        } else if(operation.equals("DROP")) {
            String attribute = command;
            try {
                // remove from data catalog check if it is a valid operation
                ts.removeAttributeSchema(attribute);
                // remove column from disk lastly
                BufferManager.getInstance().deleteColumn(attribute, table_Name);
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        } else{
            System.out.println("Invalid Operation");
            throw new Exception();
        }
    }
}
