package base.parse.DDL;
import base.buffer.BufferManager;

public class DropTable {
    public static void execute(String command) throws Exception {

        BufferManager bm = BufferManager.getInstance();

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

        bm.deleteTable(table_Name);
    }
}
