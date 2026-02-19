package base.models;

import java.util.ArrayList;

public class Record {
    public ArrayList<AttributeValue> byteList;

    public Record(){
        this.byteList = new ArrayList<AttributeValue>();
    }

    /**
     * Compare two records and determine if they are equal.
     *
     * @param other The other record of the comparison
     * @return true if the records are equal, false otherwise
     */
    public boolean compareTo(Record other) {


        return false;
    }
}
