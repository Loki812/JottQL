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

import java.time.LocalDateTime;
import java.util.ArrayList;

public class UpdateTable {

    public static void updateTable(String tablename, String query) {

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

        if (trimmedQuery.toUpperCase().startsWith("SET ")) {
            trimmedQuery = trimmedQuery.substring(4).trim();
        }

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

        int pageId = tableSchema.getRootPageID();

        while (pageId != -1) {
            Page page = bufferManager.getPage(pageId);

            // do where up here
            // because where() returns array of records

            for (Record record : page.recordList) {
                if (record == null) {
                    continue;
                }

                boolean matchesWhere = true;


                if (wherePart != null) {
                    //WhereTreeNode whereTree = buildWhere(wherePart);
                    matchesWhere = false;
                }

                if (!matchesWhere) {
                    continue;
                }

                Object resolvedValue = null;

                if (valuePart.equalsIgnoreCase("NULL")) {
                    resolvedValue = null;
                } else if ((valuePart.startsWith("\"") && valuePart.endsWith("\""))
                        || (valuePart.startsWith("'") && valuePart.endsWith("'"))) {
                    resolvedValue = valuePart.substring(1, valuePart.length() - 1);
                } else {
                    boolean foundAttributeReference = false;

                    for (int i = 0; i < attrs.size(); i++) {
                        if (attrs.get(i).attributeName.equalsIgnoreCase(valuePart)) {
                            AttributeValue<?> sourceValue = record.attributeList.get(i);
                            resolvedValue = sourceValue.data;
                            foundAttributeReference = true;
                            break;
                        }
                    }

                    if (!foundAttributeReference) {
                        try {
                            resolvedValue = Integer.parseInt(valuePart);
                        } catch (NumberFormatException e1) {
                            try {
                                resolvedValue = Double.parseDouble(valuePart);
                            } catch (NumberFormatException e2) {
                                if (valuePart.equalsIgnoreCase("true") || valuePart.equalsIgnoreCase("false")) {
                                    resolvedValue = Boolean.parseBoolean(valuePart);
                                } else {
                                    System.err.println("Unsupported SET value");
                                    return;
                                }
                            }
                        }
                    }
                }

                if (resolvedValue == null && targetSchema.getNotNull()) {
                    System.err.println("Cannot assign NULL to NOT NULL attribute");
                    return;
                }

                AttributeValue<?> newValue;

                try {
                    switch (targetSchema.getDataType()) {
                        case INTEGER -> {
                            if (resolvedValue == null) {
                                newValue = new AttributeValue<>(null, DataTypes.INTEGER);
                            } else if (resolvedValue instanceof Number) {
                                newValue = new AttributeValue<>(((Number) resolvedValue).intValue(), DataTypes.INTEGER);
                            } else {
                                System.err.println("Type mismatch in UPDATE");
                                return;
                            }
                        }

                        case DOUBLE -> {
                            if (resolvedValue == null) {
                                newValue = new AttributeValue<>(null, DataTypes.DOUBLE);
                            } else if (resolvedValue instanceof Number) {
                                newValue = new AttributeValue<>(((Number) resolvedValue).doubleValue(), DataTypes.DOUBLE);
                            } else {
                                System.err.println("Type mismatch in UPDATE");
                                return;
                            }
                        }

                        case BOOLEAN -> {
                            if (resolvedValue == null) {
                                newValue = new AttributeValue<>(null, DataTypes.BOOLEAN);
                            } else if (resolvedValue instanceof Boolean) {
                                newValue = new AttributeValue<>((Boolean) resolvedValue, DataTypes.BOOLEAN);
                            } else {
                                System.err.println("Type mismatch in UPDATE");
                                return;
                            }
                        }

                        case CHAR -> {
                            if (resolvedValue == null) {
                                newValue = new AttributeValue<>(null, DataTypes.CHAR);
                            } else {
                                String strVal = String.valueOf(resolvedValue);
                                if (strVal.length() != targetSchema.getLength()) {
                                    System.err.println("CHAR length mismatch in UPDATE");
                                    return;
                                }
                                newValue = new AttributeValue<>(strVal, DataTypes.CHAR);
                            }
                        }

                        case VARCHAR -> {
                            if (resolvedValue == null) {
                                newValue = new AttributeValue<>(null, DataTypes.VARCHAR);
                            } else {
                                String strVal = String.valueOf(resolvedValue);
                                if (strVal.length() > targetSchema.getLength()) {
                                    System.err.println("VARCHAR length too large in UPDATE");
                                    return;
                                }
                                newValue = new AttributeValue<>(strVal, DataTypes.VARCHAR);
                            }
                        }

                        default -> {
                            System.err.println("Unsupported attribute type");
                            return;
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to update value");
                    return;
                }

                record.attributeList.set(targetIndex, newValue);
                page.hasBeenModified = true;
                page.timestamp = LocalDateTime.now();
            }

            pageId = page.nextPageId;
        }
    }

}