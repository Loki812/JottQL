package base.models;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AttributeSchema {

    public String attributeName;
    private DataTypes dataType;
    private int length;
    private boolean notNull;
    private boolean primaryKey;
    private boolean unique;
    private Object defaultVal;
    public String ogTable;

    public AttributeSchema() {
        this.notNull = false;
        this.primaryKey = false;
        this.unique = false;
        this.defaultVal = null;
    }

    public AttributeSchema(AttributeSchema attributeSchema, String table){
        if(attributeSchema.attributeName.contains(".")){
            this.attributeName = attributeSchema.attributeName;
        }else{
            this.attributeName = table+"."+attributeSchema.attributeName;
        }
        this.dataType = attributeSchema.dataType;
        this.length = attributeSchema.length;
        this.notNull = attributeSchema.notNull;
        this.primaryKey = attributeSchema.primaryKey;
        this.unique = attributeSchema.unique;
        this.ogTable = attributeSchema.ogTable;
        if(this.ogTable == null){
            this.ogTable = table;
        }
    }

    public AttributeSchema copy() {
        AttributeSchema copied = new AttributeSchema();
        copied.attributeName = this.attributeName;
        copied.dataType = this.dataType;
        copied.notNull = this.notNull;
        copied.primaryKey = this.primaryKey;
        copied.unique = this.unique;
        copied.ogTable = this.ogTable;
        copied.defaultVal = this.defaultVal;
        copied.length = this.length;
        return copied;
    }

    public static AttributeSchema createAttributeSchemaFromDisk(DataInputStream in, int offSet) throws IOException {
        AttributeSchema as = new AttributeSchema();

        byte[] nameBytes = new byte[in.readInt()];
        in.readFully(nameBytes);

        as.attributeName = new String(nameBytes, StandardCharsets.UTF_8);

        // we store the ordinal value of the datatype of disk
        // ex. DataTypes.INTEGER = 0
        as.dataType = DataTypes.values()[in.readInt()];

        as.length = in.readInt();

        as.notNull = in.readBoolean();
        as.primaryKey = in.readBoolean();
        as.unique = in.readBoolean();
        return as;
    }

    public static AttributeSchema createAttributeSchemaFromQuery(String field) throws Exception {
        field = field.strip();

        // INPUT String = "ID INT PRIMARYKEY" <Identifier> <type> <constraints>
        String[] tokens = field.split(" ");

        // each column definition needs atleast <Ident> <type>
        if (tokens.length < 2) {
            System.out.println("Invalid column definition:" + field);
            throw new Exception();
        }

        /* ---------- Name -------------- */
        AttributeSchema abtSch = new AttributeSchema();
        abtSch.attributeName = tokens[0];

        /* ---------- Datatype ------------ */
        String type = tokens[1].split("\\(")[0];
        abtSch.dataType = switch (type) {
            case "INTEGER" -> DataTypes.INTEGER;
            case "DOUBLE" -> DataTypes.DOUBLE;
            case "BOOLEAN" -> DataTypes.BOOLEAN;
            case "CHAR" -> DataTypes.CHAR;
            case "VARCHAR" -> DataTypes.VARCHAR;
            default -> {
                System.out.println("Invalid Datatype Specified: " + type + " is not valid");
                throw new Exception();
            }
        };

        /*------- Length ---------*/
        abtSch.length = switch (abtSch.dataType) {
            case INTEGER -> 4;
            case DOUBLE -> 8;
            case BOOLEAN -> 1;
            case CHAR, VARCHAR -> {
                int openParen = tokens[1].indexOf("(");
                int closeParen = tokens[1].indexOf(")");

                if (closeParen != -1 && openParen != -1 && closeParen > openParen) {
                    yield Integer.parseInt(tokens[1].substring(openParen + 1, closeParen));
                } else {
                    System.out.println("Column definition was not valid");
                    throw new Exception();
                }
            }
        };

        /*------- Constraints ---------*/
        for (int i = 2; i < tokens.length; i++) {
            switch (tokens[i]) {
                case "PRIMARYKEY" -> {
                    abtSch.primaryKey = true;
                    abtSch.notNull = true;
                }
                case "NOTNULL" -> abtSch.notNull = true;
                case "UNIQUE" -> abtSch.unique = true;
                case "DEFAULT" -> {
                    i += 1; // use to grab default value
                    String defValue = tokens[i];
                    abtSch.setDefaultValue(defValue);
                }
            }
        }
        return abtSch;
    }

    public void saveAttributeSchemaToDisk(DataOutputStream out) throws IOException {
        out.writeInt(attributeName.length());

        byte[] nameBytes = attributeName.getBytes(StandardCharsets.UTF_8);
        out.write(nameBytes);

        out.writeInt(dataType.ordinal());

        out.writeInt(length);
        out.writeBoolean(notNull);
        out.writeBoolean(primaryKey);
        out.writeBoolean(unique);
    }

    public int getLength() { return length; }

    public boolean getNotNull() { return notNull; }

    public boolean isPrimaryKey() { return primaryKey; }

    public boolean isUnique() { return unique; }

    public Object getDefaultVal() { return defaultVal; }

    public DataTypes getDataType() { return dataType; }

    /**
     * setDefaultValue expects the attributeSchemas datatype to be not null
     *
     *
     * @param defValue the default value passed from a query
     */
    private void setDefaultValue(String defValue) throws Exception {
        defaultVal = switch(dataType) {
            case INTEGER -> Integer.parseInt(defValue);
            case DOUBLE -> Double.parseDouble(defValue);
            case BOOLEAN -> Boolean.parseBoolean(defValue);
            case CHAR -> {
                if (!(defValue.startsWith("\"") && defValue.endsWith("\""))) {
                    System.out.println("Default Value was not valid: " + defValue);
                    throw new Exception();
                }
                String strippedValue = defValue.substring(1, defValue.length() - 1);

                if (length != strippedValue.length()) {
                    System.out.println("Default value was not prescribed length: " + defValue);
                    throw new Exception();
                }

                yield strippedValue;
            }
            case VARCHAR -> {
                if (!(defValue.startsWith("\"") && defValue.endsWith("\""))) {
                    System.out.println("Default Value was not valid: " + defValue);
                    throw new Exception();
                }
                String strippedValue = defValue.substring(1, defValue.length() - 1);

                if (length < strippedValue.length()) {
                    System.out.println("Default value was too large to fit: " + defValue);
                    throw new Exception();
                }

                yield strippedValue;
            }
        };
    }

    public void makePrimaryKey() {
        this.primaryKey = true;
    }
}
