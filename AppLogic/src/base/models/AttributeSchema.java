package base.models;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class AttributeSchema {

    public String attributeName;
    private DataTypes dataType;
    private int length;
    private boolean notNull;
    private boolean primaryKey;
    private boolean unique;
    private Object Default = null;

    public AttributeSchema() {}

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

        return as;
    }

    public static AttributeSchema createAttributeSchemaFromQuery(String field) throws Exception {
        ArrayList<String> fields = new ArrayList<>(Arrays.asList(field.split(" ")));
        String name = fields.removeFirst();
        AttributeSchema atb = new  AttributeSchema();
        try {
            atb.attributeName = name;
            String type = fields.removeFirst();
            if(type.substring(DataTypes.valueOf("VARCHAR").toString().length()).equals("VARCHAR")) {
                atb.dataType = DataTypes.VARCHAR;
                type = type.substring(DataTypes.valueOf("VARCHAR").toString().length())
                        .replace("(", "").replace(")", "");
                atb.length = Integer.parseInt(type);
            } else if(type.substring(DataTypes.valueOf("CHAR").toString().length()).equals("VARCHAR")) {
                    atb.dataType = DataTypes.VARCHAR;
                    type = type.substring(DataTypes.valueOf("CHAR").toString().length())
                            .replace("(", "").replace(")", "");
                    atb.length = Integer.parseInt(type);
            }else{
                atb.dataType = DataTypes.valueOf(type);
                if(atb.dataType == DataTypes.INTEGER){
                    atb.length = 4;
                } else if(atb.dataType == DataTypes.DOUBLE){
                    atb.length = 8;
                } else if(atb.dataType == DataTypes.BOOLEAN){
                    atb.length = 1;
                }
            }
        }catch (IllegalArgumentException e){
            System.out.println(name + " has invalid data type");
            throw new Exception();
        }catch (NoSuchElementException e){
            System.out.println(name + " has No data type");
            throw new Exception();
        }
        while(!field.isEmpty()){
            String constraint = fields.removeFirst();
            if(constraint.equals("PRIMARYKEY")){
                atb.primaryKey = true;
                atb.unique = true;
                atb.notNull = true;
            }else if(constraint.equals("NOTNULL")) {
                atb.notNull = true;
            }else if(constraint.equals("UNIQUE")) {
                atb.unique = true;
            }else if(constraint.equals("DEFAULTVALUE")){
                atb.Default = fields.removeFirst();
            }else {
                System.out.println(constraint + " is an invalid constraint");
                throw new Exception();
            }
        }

        return atb;
    }

    public void saveAttributeSchemaToDisk(DataOutputStream out) throws IOException {
        out.writeInt(attributeName.length());

        byte[] nameBytes = attributeName.getBytes(StandardCharsets.UTF_8);
        out.write(nameBytes);

        out.writeInt(dataType.ordinal());

        out.writeInt(length);
        out.writeBoolean(notNull);
        out.writeBoolean(primaryKey);
    }

    public int getLength() { return length; }

    public boolean getNotNull() { return notNull; }

    public boolean isPrimaryKey() { return primaryKey; }

    public DataTypes getDataType() { return dataType; }

}
