package base.models.concrete;

import base.models.schemas.AttributeSchema;
import base.models.schemas.DataTypes;
import base.models.schemas.TableSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Record {
    public ArrayList<AttributeValue> attributeList;

    public Record(){
        this.attributeList = new ArrayList<>();
    }

    /**
     * Constructor used when converting to and from byte arrays.
     * Takes a ByteBuffer, is not expected to utilize the whole thing.
     *
     * @param buffer the bytebuffer containing binary information on the record
     * @param attributes the attributes of the corresponding table
     */
    public Record(ByteBuffer buffer, ArrayList<AttributeSchema> attributes) {
        byte[] nullByteArray = new byte[attributes.size()];
        buffer.get(nullByteArray); // advances pointer and loads values into nullByte array

        this.attributeList = new ArrayList<>();

        for (int i = 0; i < nullByteArray.length; i++) {
            DataTypes d = attributes.get(i).getDataType();

            if (nullByteArray[i] == 1) {
                // if null set data field to null
                this.attributeList.add(new AttributeValue<>(null, d));
            } else {
                Object value = switch(d) {
                    case INTEGER -> buffer.getInt();
                    case DOUBLE -> buffer.getDouble();
                    case BOOLEAN -> buffer.get() == 1;
                    case CHAR, VARCHAR -> {
                        int len = buffer.getInt();
                        byte[] sBytes = new byte[len];
                        buffer.get(sBytes);
                        yield new String(sBytes, StandardCharsets.UTF_8);
                    }
                };
                this.attributeList.add(new AttributeValue<>(value, d));
            }
        }
    }

    /**
     * Writes the record to the byte buffer in binary
     * @param byteBuffer the given byteBuffer
     * @param attributeSchemas the given attributes
     */
    public void toBytes(ByteBuffer byteBuffer, ArrayList<AttributeSchema> attributeSchemas) {
        for (AttributeValue<?> a : this.attributeList) {
            if (a.data == null) {
                byteBuffer.put((byte) 1);
            } else {
                byteBuffer.put((byte) 0);
            }
        }

        for (int i = 0; i < attributeSchemas.size(); i++) {
            Object value = this.attributeList.get(i).data;
            if (value != null) {
                DataTypes dataType = attributeSchemas.get(i).getDataType();
                switch (dataType) {
                    case INTEGER -> byteBuffer.putInt((int) value);
                    case DOUBLE -> byteBuffer.putDouble((double) value);
                    // in-case value is Boolean and not boolean, haven't checked full codebase
                    case BOOLEAN -> byteBuffer.put((byte) ((boolean) value ? 1 : 0));
                    case CHAR, VARCHAR -> {
                        String s = (String) value;

                        byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
                        // write the length of value
                        byteBuffer.putInt(strBytes.length);
                        byteBuffer.put(strBytes);

                    }
                }
            }
        }
    }

    /**
     * Compare this record to another record.
     *
     * @param rec The record to compare this record to
     * @param schema The current table schema
     * @return a positive value if this record is greater than rec,
     *      negative if it is less than rec,
     *      0 if they are equal
     */
    public int compareTo(Record rec, TableSchema schema)  {

        if (schema.primaryKey == null) {
            return 0;
        }

        Integer primaryKeyIndex = schema.getIndex(schema.primaryKey);

        if (primaryKeyIndex == null) {
            return 0;
        }

        AttributeValue<?> primaryKey = attributeList.get(primaryKeyIndex);

        if (primaryKey.data == null) {
            System.out.println("Primary key is null");
            throw new RuntimeException();
        }

        return primaryKey.compareTo(rec.attributeList.get(primaryKeyIndex));
    }

    public int getSize(){
        int byteSize = 0;
        for (AttributeValue<?> a : attributeList){
            //add 1 for null bit array
            byteSize+=1;

            //add bytes based on data type
            switch (a.type){
                case DataTypes.INTEGER ->  byteSize += Integer.BYTES;
                case DataTypes.DOUBLE -> byteSize+=Double.BYTES;
                case DataTypes.BOOLEAN -> byteSize+=1;
                case DataTypes.CHAR, DataTypes.VARCHAR -> {
                    String s = (String) a.data;
                    byteSize += Integer.BYTES + s.getBytes(StandardCharsets.UTF_8).length;
                }

            }

        }
        return byteSize;
    }
}
