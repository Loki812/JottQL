package base.parse.DML.ParserHelpers;

import base.buffer.BufferManager;
import base.models.AttributeSchema;
import base.models.AttributeValue;
import base.models.DataCatalog;
import base.models.DataTypes;
import base.models.Page;
import base.models.Record;
import base.models.TableSchema;
import base.models.whereNodes.WhereTreeNode;
import base.parse.DDL.DropTable;
import static base.parse.DML.Where.buildWhereTree;
import java.util.ArrayList;

public class UpdateTable {

    private static class ResolvedOperand {
        Object value;
        DataTypes type;

        ResolvedOperand(Object value, DataTypes type) {
            this.value = value;
            this.type = type;
        }

    }

    public static void parse(String command) throws Exception {

        String trimmed = command.trim();

        if (!trimmed.toUpperCase().startsWith("UPDATE")) {
            System.err.println("Invalid UPDATE syntax");
            return;
        }

        trimmed = trimmed.substring("UPDATE".length()).trim();

        String tableName = trimmed.split(" ")[0];

        updateTable(tableName, command);

    }

    public static void updateTable(String tablename, String query) throws Exception {

        // createtable(string)
        //insert some values (optional)
        // update("update t1 set a = 5")
        // update and delete: output = "x rows updated/deleted..."
        // select * from t1, verify results with eyeballs

        if (tablename == null || query == null) {
            System.err.println("Invalid UPDATE command");
            return;
        }

        DataCatalog dataCatalog = DataCatalog.getInstance();
        BufferManager bufferManager = BufferManager.getInstance();

        TableSchema tableSchema = dataCatalog.getTableSchema(tablename);
        if (tableSchema == null) {
            System.err.println("Table does not exist");
            return;
        }

        String trimmedQuery = query.trim();

        if (trimmedQuery.endsWith(";")) {
            trimmedQuery = trimmedQuery.substring(0, trimmedQuery.length() - 1).trim();
        }

        String upperQuery = trimmedQuery.toUpperCase();
        String updatePrefix = "UPDATE " + tablename.toUpperCase();

        if (!upperQuery.startsWith(updatePrefix)) {
            System.err.println("Invalid UPDATE command");
            return;
        }

        trimmedQuery = trimmedQuery.substring(updatePrefix.length()).trim();

        if (!trimmedQuery.toUpperCase().startsWith("SET ")) {
            System.err.println("Missing SET clause");
            return;
        }

        trimmedQuery = trimmedQuery.substring(4).trim();

        int whereIndex = trimmedQuery.toUpperCase().indexOf("WHERE");
        String setPart;
        String wherePart = null;

        if (whereIndex == -1) {
            setPart = trimmedQuery;
        } else {
            setPart = trimmedQuery.substring(0, whereIndex).trim();
            wherePart = trimmedQuery.substring(whereIndex + "WHERE".length()).trim();
        }

        int equalsIndex = setPart.indexOf('=');
        if (equalsIndex == -1) {
            System.err.println("Invalid SET clause");
            return;
        }

        String targetAttr = setPart.substring(0, equalsIndex).trim();
        String valuePart = setPart.substring(equalsIndex + 1).trim();

        ArrayList<AttributeSchema> attrs =
                new ArrayList<>(tableSchema.getAttributeSchemas().sequencedValues());

        int targetIndex = -1;
        AttributeSchema targetSchema = null;

        for (int i = 0; i < attrs.size(); i++) {
            if (attrs.get(i).attributeName.equalsIgnoreCase(targetAttr)) {
                targetIndex = i;
                targetSchema = attrs.get(i);
                break;
            }
        }

        if (targetIndex == -1) {
            System.err.println("Attribute in SET clause does not exist");
            return;
        }

        WhereTreeNode whereTree = null;
        if (wherePart != null && !wherePart.isBlank()) {
            try {
                whereTree = buildWhereTree("WHERE " + wherePart, tablename);
            } catch (Exception e) {
                System.err.println("Invalid WHERE clause");
                return;
            }
        }

        TableSchema tempSchema = tableSchema.makeTempCopy(new ArrayList<>());

        String tempName = tempSchema.tableName;

        int pageId = tableSchema.getRootPageID();

        while (pageId != -1) {
            Page page = bufferManager.getPage(pageId);

            for (Record record : page.recordList) {

                Record newRecord = copyRecord(record);
                boolean matchesWhere = true;

                if (whereTree != null) {
                    matchesWhere = whereTree.eval(record, tableSchema);
                }

                if (!matchesWhere) {
                    bufferManager.insertRecordIntoTable(tempName, newRecord);
                }else {
                    ResolvedOperand resolved = evaluateSetExpression(valuePart, record, attrs);
                    if (resolved == null) {
                        return;
                    }

                    AttributeValue<?> newValue = buildUpdatedValue(resolved, targetSchema);
                    if (newValue == null) {
                        return;
                    }

                    newRecord.attributeList.set(targetIndex, newValue);
                }
                bufferManager.insertRecordIntoTable(tempName, newRecord);
            }

            pageId = page.nextPageId;
        }

        dataCatalog.changeTableName(tablename, tempName);
        System.out.println("Update Successful");
    }

    private static Record copyRecord(Record original) {

        Record r = new Record();

        for (AttributeValue<?> v : original.attributeList) {

            if (v == null) {

                r.attributeList.add(null);

            } else {

                r.attributeList.add(
                        new AttributeValue<>(v.data, v.type)
                );

            }

        }

        return r;

    }

    private static ResolvedOperand resolveSingleValue(String token, Record record, ArrayList<AttributeSchema> attrs) {

        if (token == null) {
            return null;
        }

        token = token.trim();

        if (token.equalsIgnoreCase("NULL")) {
            return new ResolvedOperand(null, null);
        }

        if ((token.startsWith("\"") && token.endsWith("\""))
                || (token.startsWith("'") && token.endsWith("'"))) {
            return new ResolvedOperand(token.substring(1, token.length() - 1), DataTypes.VARCHAR);
        }

        if (token.equalsIgnoreCase("true") || token.equalsIgnoreCase("false")) {
            return new ResolvedOperand(Boolean.parseBoolean(token), DataTypes.BOOLEAN);
        }

        for (int i = 0; i < attrs.size(); i++) {
            if (attrs.get(i).attributeName.equalsIgnoreCase(token)) {
                AttributeValue<?> sourceValue = record.attributeList.get(i);
                return new ResolvedOperand(
                        sourceValue == null ? null : sourceValue.data,
                        attrs.get(i).getDataType()
                );
            }
        }

        try {
            return new ResolvedOperand(Integer.parseInt(token), DataTypes.INTEGER);
        }
        catch (NumberFormatException e1) {
            try {
                return new ResolvedOperand(Double.parseDouble(token), DataTypes.DOUBLE);
            }
            catch (NumberFormatException e2) {
                System.err.println("Unsupported SET value");
                return null;
            }
        }
    }

    private static ResolvedOperand evaluateSetExpression(String valuePart, Record record, ArrayList<AttributeSchema> attrs) {

        String expr = valuePart.trim();
        String[] operators = {"+", "-", "*", "/"};

        int operatorCount = 0;
        String foundOperator = null;
        int foundIndex = -1;

        for (String op : operators) {
            int idx = expr.indexOf(" " + op + " ");
            if (idx != -1) {
                operatorCount++;
                foundOperator = op;
                foundIndex = idx;
            }
        }

        if (operatorCount > 1) {
            System.err.println("Compound expressions are not supported");
            return null;
        }

        if (operatorCount == 1) {

            // splits the expression into right and left
            String leftToken = expr.substring(0, foundIndex).trim();
            String rightToken = expr.substring(foundIndex + 3).trim();

            ResolvedOperand left = resolveSingleValue(leftToken, record, attrs);
            ResolvedOperand right = resolveSingleValue(rightToken, record, attrs);

            if (left == null || right == null) {
                return null;
            }

            if (left.value == null || right.value == null) {
                System.err.println("Cannot use NULL in mathematical expression");
                return null;
            }

            if (!((left.type == DataTypes.INTEGER || left.type == DataTypes.DOUBLE)
                    && (right.type == DataTypes.INTEGER || right.type == DataTypes.DOUBLE))) {
                System.err.println("Mathematical expressions require INTEGER or DOUBLE operands");
                return null;
            }

            if (left.type != right.type) {
                System.err.println("Operand types in mathematical expression must match");
                return null;
            }

            if (left.type == DataTypes.INTEGER) {
                int l = ((Number) left.value).intValue();
                int r = ((Number) right.value).intValue();

                return switch (foundOperator) {
                    case "+" -> new ResolvedOperand(l + r, DataTypes.INTEGER);
                    case "-" -> new ResolvedOperand(l - r, DataTypes.INTEGER);
                    case "*" -> new ResolvedOperand(l * r, DataTypes.INTEGER);
                    case "/" -> {
                        if (r == 0) {
                            System.err.println("Division by zero in UPDATE");
                            yield null;
                        }
                        yield new ResolvedOperand(l / r, DataTypes.INTEGER);
                    }
                    default -> null;
                };
            }
            // assume double
            else {
                double l = ((Number) left.value).doubleValue();
                double r = ((Number) right.value).doubleValue();

                return switch (foundOperator) {
                    case "+" -> new ResolvedOperand(l + r, DataTypes.DOUBLE);
                    case "-" -> new ResolvedOperand(l - r, DataTypes.DOUBLE);
                    case "*" -> new ResolvedOperand(l * r, DataTypes.DOUBLE);
                    case "/" -> {
                        if (r == 0.0) {
                            System.err.println("Division by zero in UPDATE");
                            yield null;
                        }
                        yield new ResolvedOperand(l / r, DataTypes.DOUBLE);
                    }
                    default -> null;
                };
            }
        }

        return resolveSingleValue(expr, record, attrs);
    }

    private static AttributeValue<?> buildUpdatedValue(ResolvedOperand resolved, AttributeSchema targetSchema) throws Exception {

        // same logic as before
        Object resolvedValue = resolved == null ? null : resolved.value;

        if (resolvedValue == null && targetSchema.getNotNull()) {
            System.err.println("Cannot assign NULL to NOT NULL attribute");
            throw new Exception();
        }

        switch (targetSchema.getDataType()) {
            case INTEGER -> {
                if (resolvedValue == null) {
                    return new AttributeValue<>(null, DataTypes.INTEGER);
                } else if (resolvedValue instanceof Number) {
                    return new AttributeValue<>(((Number) resolvedValue).intValue(), DataTypes.INTEGER);
                } else {
                    System.err.println("Type mismatch in UPDATE");
                    return null;
                }
            }

            case DOUBLE -> {
                if (resolvedValue == null) {
                    return new AttributeValue<>(null, DataTypes.DOUBLE);
                } else if (resolvedValue instanceof Number) {
                    return new AttributeValue<>(((Number) resolvedValue).doubleValue(), DataTypes.DOUBLE);
                } else {
                    System.err.println("Type mismatch in UPDATE");
                    return null;
                }
            }

            case BOOLEAN -> {
                if (resolvedValue == null) {
                    return new AttributeValue<>(null, DataTypes.BOOLEAN);
                } else if (resolvedValue instanceof Boolean) {
                    return new AttributeValue<>((Boolean) resolvedValue, DataTypes.BOOLEAN);
                } else {
                    System.err.println("Type mismatch in UPDATE");
                    return null;
                }
            }

            case CHAR -> {
                if (resolvedValue == null) {
                    return new AttributeValue<>(null, DataTypes.CHAR);
                } else {
                    String strVal = String.valueOf(resolvedValue);
                    if (strVal.length() != targetSchema.getLength()) {
                        System.err.println("CHAR length mismatch in UPDATE");
                        return null;
                    }
                    return new AttributeValue<>(strVal, DataTypes.CHAR);
                }
            }

            case VARCHAR -> {
                if (resolvedValue == null) {
                    return new AttributeValue<>(null, DataTypes.VARCHAR);
                } else {
                    String strVal = String.valueOf(resolvedValue);
                    if (strVal.length() > targetSchema.getLength()) {
                        System.err.println("VARCHAR length too large in UPDATE");
                        return null;
                    }
                    return new AttributeValue<>(strVal, DataTypes.VARCHAR);
                }
            }

            default -> {
                System.err.println("Unsupported attribute type");
                return null;
            }
        }
    }
}