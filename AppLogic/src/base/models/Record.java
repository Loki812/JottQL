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
}
