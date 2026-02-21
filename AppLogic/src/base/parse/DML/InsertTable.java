package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;

import java.util.ArrayList;
import java.util.List;

public class InsertTable {

    private static AttributeValue<?> convertLiteral(String raw, AttributeSchema schema) throws Exception {

        raw = raw.trim();
        DataTypes type = schema.getDataType();
        switch (raw) {

            case "NULL" -> {
                if (schema.getNotNull()) {
                    System.out.println(schema.attributeName + " cannot be NULL");
                    throw new Exception("NOT NULL constraint violated for " + schema.attributeName);
                }
                return new AttributeValue<>(null, type);

            }

            case "TRUE" -> {
                if (type.equals(DataTypes.BOOLEAN)) {
                    return new AttributeValue<>(true, type);
                } else {
                    System.out.println(schema.attributeName + " is not a BOOLEAN");
                    throw new Exception();
                }

            }

            case "FALSE" -> {

                if (type.equals(DataTypes.BOOLEAN)) {
                    return new AttributeValue<>(false, type);
                } else {
                    System.out.println(schema.attributeName + " is not a BOOLEAN");
                    throw new Exception();
                }

            }

        }

        if (raw.startsWith("\"") && raw.endsWith("\"") && raw.length() >= 2) {

            String s = raw.substring(1, raw.length() - 1);
            if (type == DataTypes.CHAR) {

                if (schema.getLength() != s.length()) {

                    System.out.println(s + ": CHAR must be of length " + schema.getLength());
                    throw new Exception("CHAR length mismatch");

                }

                return new AttributeValue<>(s, type);

            }

            if (type == DataTypes.VARCHAR) {

                if (schema.getLength() < s.length()) {

                    System.out.println(s + ": VARCHAR must be within length " + schema.getLength());
                    throw new Exception("VARCHAR length overflow");

                }

                return new AttributeValue<>(s, type);

            }

            System.out.println("Type mismatch " + type);
            throw new Exception("Type mismatch");

        }

        if (type == DataTypes.INTEGER) {

            try {

                return new AttributeValue<>(Integer.parseInt(raw), type);

            } catch (NumberFormatException e) {

                System.out.println("Invalid int" + raw);
                throw new Exception();

            }

        }

        if (type == DataTypes.DOUBLE) {

            try {

                return new AttributeValue<>(Double.parseDouble(raw), type);

            } catch (NumberFormatException e) {

                System.out.println("Invalid double" + raw);
                throw new Exception();

            }

        }

        System.out.println("Invalid type" + type);
        throw new Exception();

    }

    private static List<String> splitByComma(String s) throws Exception {

        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {

            char ch = s.charAt(i);
            if (ch == '"') {

                inQuotes = !inQuotes;
                cur.append(ch);
                continue;

            }

            if (ch == ',' && !inQuotes) {

                out.add(cur.toString().trim());
                cur.setLength(0);
                continue;

            }

            cur.append(ch);

        }

        if (inQuotes) {

            System.out.println("Unterminated string literal in VALUES");
            throw new Exception();

        }

        String last = cur.toString().trim();
        if (!last.isEmpty()) {

            out.add(last);

        }


        return out;

    }

    private static List<String> splitByWhitespace(String s) throws Exception {

        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {

            char ch = s.charAt(i);
            if (ch == '"') {

                inQuotes = !inQuotes;
                cur.append(ch);
                continue;

            }

            if (Character.isWhitespace(ch) && !inQuotes) {

                if (!cur.isEmpty()) {

                    out.add(cur.toString().trim());
                    cur.setLength(0);

                }

                continue;

            }

            cur.append(ch);

        }

        if (inQuotes) {

            System.out.println("Unterminated string literal in tuple");
            throw new Exception();

        }

        if (!cur.isEmpty()) out.add(cur.toString().trim());
        return out;

    }

    public static void parse(String command) throws Exception {

        String trimmedCommand = command.trim();
        if (!trimmedCommand.startsWith("INSERT ")) {

            System.out.println("Invalid INSERT Command");
            throw new Exception();

        }
        if (!trimmedCommand.endsWith(";")) {

            System.out.println("Missing ';'");
            throw new Exception();

        }

        trimmedCommand = trimmedCommand.substring(0, trimmedCommand.length() - 1).trim();
        String afterInsert = trimmedCommand.substring("INSERT".length()).trim();
        int firstSpace = afterInsert.indexOf(' ');
        if (firstSpace < 0) {

            System.out.println("Missing table name");
            throw new Exception();

        }

        String tableName = afterInsert.substring(0, firstSpace).trim().toUpperCase();
        String remainder = afterInsert.substring(firstSpace).trim();

        if (!remainder.toUpperCase().startsWith("VALUES")) {

            System.out.println("Missing VALUES keyword");
            throw new Exception();

        }

        remainder = remainder.substring("VALUES".length()).trim();
        if (!remainder.startsWith("(") || !remainder.endsWith(")")) {

            System.out.println("VALUES must be wrapped in parentheses");
            throw new Exception();

        }

        String inParen = remainder.substring(1, remainder.length() - 1).trim();
        if (inParen.isEmpty()) {

            System.out.println("No rows provided");
            throw new Exception();

        }

        DataCatalog dataCatalog = DataCatalog.getInstance();
        TableSchema tableSchema = dataCatalog.getTableSchema(tableName);
        if (tableSchema == null) {

            System.out.println("Table " + tableName + " not found");
            throw new Exception();

        }

        List<AttributeSchema> attributeSchemas = new ArrayList<>(tableSchema.getAttributeSchemas().sequencedValues());
        List<String> strings = splitByComma(inParen);
        int inserted = 0;
        for (int idx = 0; idx < strings.size(); idx++) {

            String attr = strings.get(idx).trim();
            if (attr.isEmpty()) {

                System.out.println("Empty row at position " + (idx + 1));
                throw new Exception();

            }

            List<String> rawValues = splitByWhitespace(attr);
            if (rawValues.size() != attributeSchemas.size()) {

                System.out.println("Insert values count does not equal attribute count");
                throw new Exception();

            }

            Record record = new Record();
            for (int i = 0; i < attributeSchemas.size(); i++) {

                record.attributeList.add(convertLiteral(rawValues.get(i), attributeSchemas.get(i)));

            }

            try {

                Page p = BufferManager.getPage(tableSchema.getRootPageID());
                if (p == null) {

                    throw new Exception("Root page not found for table " + tableName);

                }

                p.insertIntoPage(record);
                inserted++;

            }

            catch (Exception e) {

                System.out.println("Insert failed on row " + (idx + 1) + ": " + e.getMessage());
                throw e;

            }

        }

        System.out.println("Inserted " + inserted + " into " + tableName);

    }

}