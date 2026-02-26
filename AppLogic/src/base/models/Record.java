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
    public int compareTo(Record rec, TableSchema schema) throws Exception {
        int primaryKeyIndex = schema.getIndex(schema.primaryKey);
        AttributeValue primaryKey = attributeList.get(primaryKeyIndex);
        if(primaryKey.data == null){
            System.out.println("Primary key is null");
            throw new Exception();
        }
        return primaryKey.compareTo(rec.attributeList.get(primaryKeyIndex));
    }

    public int getSize(){
        int byteSize = 0;
        for (AttributeValue a : attributeList){
            //add 1 for null bit array
            byteSize+=1;

            //add bytes based on data type
            switch (a.type){
                case INTEGER:
                    byteSize+=Integer.BYTES;
                case DOUBLE:
                    byteSize+=Double.BYTES;
                case BOOLEAN:
                    byteSize+=1;
                case CHAR:
                    byteSize+=a.toString().length();
                case VARCHAR:
                    byteSize+=a.toString().length();
            }

        }
        return byteSize;
    }
}
