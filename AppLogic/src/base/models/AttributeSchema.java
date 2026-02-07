package base.models;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AttributeSchema {

    public String attributeName;
    private DataTypes dataType;
    private int length;
    private int offSet; // offset from start of record, allows for quicker lookups.
    private boolean notNull;
    private boolean primaryKey;

    public AttributeSchema() {}

    public static AttributeSchema createAttributeSchemaFromDisk(DataInputStream in, int offSet) throws IOException {
        AttributeSchema as = new AttributeSchema();
        as.offSet = offSet;

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

    public static AttributeSchema createAttributeSchemaFromQuery(String Query) {
        // TODO
        return new AttributeSchema();
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

    public int getOffSet() { return offSet; }

    public boolean getNotNull() { return notNull; }

    public void setNotNull(boolean n) { notNull = n; }

    public boolean isPrimaryKey() { return primaryKey; }

    public DataTypes getDataType() { return dataType; }

}
