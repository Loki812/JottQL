package base.models;

public class Record {
    public Byte[] byteList;

    public Record(int recordLength){
        this.byteList = new Byte[recordLength];
    }
}
