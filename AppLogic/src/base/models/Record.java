package base.models;

import java.util.ArrayList;

public class Record {
    public ArrayList<AttributeValue> attributeList;

    public Record(){
        this.attributeList = new ArrayList<AttributeValue>();
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
    public int compareTo(Record rec, TableSchema schema) {
        int primaryKeyIndex = schema.getPrimaryIndex();
        AttributeValue primaryKey = attributeList.get(primaryKeyIndex);
        return primaryKey.compareTo(rec.attributeList.get(primaryKeyIndex));
    }

    public int getSize(){
        int byteSize = 0;
        for (AttributeValue a : attributeList){
            //add 1 for null bit array
            byteSize+=1;

            //add bytes based on data type
            switch (a.type){
                case DataTypes.INTEGER:
                    byteSize+=Integer.BYTES;
                case DataTypes.DOUBLE:
                    byteSize+=Double.BYTES;
                case DataTypes.BOOLEAN:
                    byteSize+=1;
                    //todo char[] and varchar[]
                default:
                    break;
            }

        }
        return byteSize;
    }
}
