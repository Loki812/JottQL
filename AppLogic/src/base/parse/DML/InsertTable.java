package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class InsertTable {

    private static AttributeValue<?> convertLiteral(String raw, AttributeSchema schema) throws Exception {

        raw = raw.trim();
        DataTypes type = schema.getDataType();
        switch (raw) {

            case "NULL" -> {
                if (schema.getNotNull()){
                    System.out.println(schema.attributeName+" is not NULL");
                }
                return new AttributeValue<>(null, type);

            }

            case "TRUE" -> {
                if(type.equals(DataTypes.BOOLEAN)) {
                    return new AttributeValue<>(true, type);
                }else {
                    System.out.println(schema.attributeName+" is not a BOOLEAN");
                    throw new Exception();
                }

            }

            case "FALSE" -> {

                if(type.equals(DataTypes.BOOLEAN)) {
                    return new AttributeValue<>(false, type);
                }else {
                    System.out.println(schema.attributeName+" is not a BOOLEAN");
                    throw new Exception();
                }

            }

        }

        if(raw.startsWith("\"") && raw.endsWith("\"") && raw.length() >= 2) {

            String s = raw.substring(1, raw.length() - 1);
            if(type == DataTypes.CHAR) {
                if(schema.getLength() == s.length()) {
                    return new AttributeValue<>(s, type);
                }else{
                    System.out.println(s+": CHAR must be of length "+schema.getLength());
                    throw new Exception();
                }
            }else if(type == DataTypes.VARCHAR) {
                if(schema.getLength() >= s.length()) {
                    return new AttributeValue<>(s, type);
                }else{
                    System.out.println(s+": VARCHAR must be within length "+schema.getLength());
                    throw new Exception();
                }
            }

            return new AttributeValue<>(s, type);

        }

        if(type == DataTypes.INTEGER) {

            try {

                return new AttributeValue<>(Integer.parseInt(raw), type);

            }

            catch (NumberFormatException e) {

                System.out.println("Invalid int" + raw);
                throw new Exception();

            }

        }

        if(type == DataTypes.DOUBLE) {

            try {

                return new AttributeValue<>(Double.parseDouble(raw), type);

            }

            catch (NumberFormatException e) {

                System.out.println("Invalid double" + raw);
                throw new Exception();

            }

        }

        System.out.println("Invalid type" + type);
        throw new Exception();

    }

    private static boolean isDuplicatePK(TableSchema tableSchema, int pkIndex, Object candidate) throws Exception {

        int pageId = tableSchema.getRootPageID();
        while (pageId >= 0) {

            Page p = BufferManager.getPage(pageId);
            for(Record r : p.recordList) {

                if(pkIndex >= r.attributeList.size()) {

                    continue;

                }

                Object exists = r.attributeList.get(pkIndex).data;
                if(exists != null && exists.equals(candidate)) {

                    return true;

                }

            }

            pageId = p.nextPageId;

        }

        return false;

    }

    public static void insertIntoLastPage(TableSchema tableSchema, Record record) throws Exception {

        int pageId = tableSchema.getRootPageID();
        Page p = BufferManager.getPage(pageId);
        while(p.nextPageId >= 0) {

            p = BufferManager.getPage(p.nextPageId);

        }

        p.insertIntoPage(record, tableSchema);

    }

    private static List<String> splitValue(String s) throws Exception {

        List<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuote = false;
        for(int i = 0; i < s.length(); i++) {

            char c = s.charAt(i);
            if(c == '"') {

                inQuote = !inQuote;
                sb.append('"');
                continue;

            }

            if(c == ',' && !inQuote) {

                list.add(sb.toString().trim());
                sb.setLength(0);
                continue;

            }

            sb.append(c);

        }

        if(inQuote) {

            System.out.println("Unterminated string in VALUES");
            throw new Exception();

        }

        String last = sb.toString().trim();
        if(!last.isEmpty()) {

            list.add(last);

        }

        return list;

    }

    public static void parse(String command) throws Exception {

        int COMMAND_LENGTH = 7;
        String trimmedCommand = command.trim();
        if(!trimmedCommand.startsWith("INSERT ")) {

            System.out.println("Invalid INSERT Command");
            throw new Exception();

        }

        if(!trimmedCommand.endsWith(";")) {

            System.out.println("Missing ';'");
            throw new Exception();

        }

        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        String remainder = trimmedCommand.substring(COMMAND_LENGTH).trim();
        int space = remainder.indexOf(' ');
        if(space < 0) {

            System.out.println("Missing table name");
            throw new Exception();

        }

        String tableName = remainder.substring(0, space).trim().toUpperCase();
        remainder = remainder.substring(space).trim();
        if(!remainder.startsWith("VALUES")) {

            System.out.println("Missing values");
            throw new Exception();

        }

        remainder = remainder.substring("VALUES".length()).trim();
        if(!remainder.startsWith("(") || !remainder.endsWith(")")) {

            System.out.println("Values missing parenthesis");
            throw new Exception();

        }

        String inParen = remainder.substring(1, remainder.length() - 1).trim();
        List<String> vals = splitValue(inParen);
        DataCatalog dataCatalog = DataCatalog.getInstance();
        TableSchema tableSchema = dataCatalog.getTableSchema(tableName);
        if(tableSchema == null) {
            System.out.println("Table " + tableName + " not found");
            throw new Exception();

        }

        List<AttributeSchema> attributeSchemas = new ArrayList<>(tableSchema.getAttributeSchemas().sequencedValues());

        if(vals.size() != attributeSchemas.size()) {

            System.out.println("Insert values count does not equal attribute count");
            throw new Exception();

        }

        Record record = new Record();
        for(int i = 0; i < attributeSchemas.size(); i++) {

            record.attributeList.add(convertLiteral(vals.get(i), attributeSchemas.get(i)));

        }
        insertIntoLastPage(tableSchema, record);

    }

}