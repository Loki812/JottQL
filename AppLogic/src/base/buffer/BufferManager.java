package base.buffer;
import base.models.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import base.models.Record;
import base.storage.StorageManager;

public class BufferManager {

    //max number of pages that can be in buffer before flushing
    //todo get max page count from command line
    private final int maxPageCount;
    private static HashMap<Integer,Page> buffer;
    private DataCatalog dataCatalog;

    //todo make static instance of the BufferManager similar to how the bufferManager is done
    private static BufferManager bufferManager = null;

    public BufferManager(int maxPageCount) throws Exception {
        this.maxPageCount = maxPageCount;
        //this.dataCatalog = DataCatalog.getInstance();
    }


    //todo buffer is inside of the buffermanage (stored as Hashmap of Pages)

    //todo to get least recently used page, get the values and find the oldest timestamp

    //todo createNewPage() -> create new Page object and store it in the hashmap

    //todo if createNewPage() is called and the # of entries in the hash, eject the least recently used page to hardware.
    //todo call storage manager to save the page



    public static Page getPage(int id){
        if(buffer.containsKey(id)){
            Page page = buffer.get(id);
            page.timestamp = LocalDateTime.now();
            return page;
        } else {
            //todo actually make a proper getPage function
            Page decodedPage; // = bufferManager.readPageFromHardware(1,encodedByteArray, fakeTableSchema);
            decodedPage = new Page(id);
            buffer.put(decodedPage.pageId, decodedPage);
            decodedPage.timestamp = LocalDateTime.now();
            return decodedPage;
        }
    }

    public Page createNewPage(int id){
        Page page = new Page(id);
        buffer.put(page.pageId, page);
        page.timestamp = LocalDateTime.now();
        return page;
    }

    /*
    private Page readPageFromHardware(int pageId) throws IOException {

        //todo send pageId to storage managers -> get a ByteBuffer
        ByteBuffer pageData = StorageManager.readPage(id);
        //todo convert byteBuffers to Page -> return page

    }
     */


    //todo make this just take in an (int pageId) instead of an array of bytes
    public Page readPageFromHardware(int pageId, byte[] encodedByteArray, ArrayList<DataTypes> fakeTableSchema) throws IOException {
        int encodedIndex = 0;
        Page finalPage = new Page(pageId);
        while(encodedIndex<encodedByteArray.length){
            Record finalRecord = new Record();
            int startByte = encodedIndex;
            //null-byte array
            byte[] nullByteArray = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+fakeTableSchema.size()));
            encodedIndex+=fakeTableSchema.size();
            System.out.println("null-byte array: "+Arrays.toString(nullByteArray));
            //objects in byte array
            for(int i=0; i<nullByteArray.length; i++){
                DataTypes dataType = fakeTableSchema.get(i);
                //check that data is != null
                if(nullByteArray[i]!=1){
                    switch(dataType){
                        case DataTypes.INTEGER:
                            encodedIndex+=Integer.BYTES;
                            break;
                        default:
                            break;
                    }
                }
            }
            int endByte = encodedIndex;
            finalRecord = convertBytesToRecord(Arrays.copyOfRange(encodedByteArray,startByte,endByte), fakeTableSchema);
            finalPage.recordList.add(finalRecord);
        }

        return finalPage;
    }


    public Record convertBytesToRecord(byte[] encodedByteArray, ArrayList<DataTypes> fakeTableSchema) throws IOException {

        //todo the null byte array should be the same size as the length of characters
        //System.out.println("encoded byteArray: "+Arrays.toString(encodedByteArray));


        int encodedIndex = 0;
        //null-byte array
        byte[] nullByteArray = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+fakeTableSchema.size()));
        encodedIndex+=fakeTableSchema.size();
        //System.out.println("null-byte array: "+Arrays.toString(nullByteArray));

        //objectify byte arrays
        Record record = new Record();
        for(int i=0; i<nullByteArray.length; i++){
            AttributeValue attribute = null;
            DataTypes dataType = fakeTableSchema.get(i);
            //check if null data
            if(nullByteArray[i]==1){
                attribute = new AttributeValue<>(null, dataType);
            } else {
                switch(dataType){
                    case DataTypes.INTEGER:
                        byte[] dataSegment = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+Integer.BYTES));
                        encodedIndex+=Integer.BYTES;
                        attribute = new AttributeValue<>(ByteBuffer.wrap(dataSegment).getInt(), dataType);
                        break;
                    default:
                        break;
                }

            }

            //AttributeValue attribute = new AttributeValue<>();
            if(attribute!=null){
                record.attributeList.add(attribute);
            }
        }

        return record;

    }


    //todo make this void and send the byte array to the sotrage manager
    public byte[] writePageToHardware(Page page, ArrayList<DataTypes> fakeTableSchema){

        ArrayList<byte[]> byteLists = new ArrayList<>();
        for(Record record : page.recordList){
            byteLists.add(convertRecordToBytes(record, fakeTableSchema));
        }

        //find size of final array
        int finalByteArraySize = 0;
        for(int i=0; i<byteLists.size(); i++){
            System.out.println(Arrays.toString(byteLists.get(i)));
            finalByteArraySize+=byteLists.get(i).length;
        }

        byte[] finalByteArray = new byte[finalByteArraySize];

        //populate final array
        int finalArrayIndex = 0;
        for(byte[] list : byteLists){
            for(byte b : list){
                finalByteArray[finalArrayIndex]=b;
                finalArrayIndex++;
            }
        }


        return finalByteArray;
    }


    public byte[] convertRecordToBytes(Record record, ArrayList<DataTypes> fakeTableSchema){

        ArrayList<byte[]> byteLists = new ArrayList<>();

        //make null-bit
        byte[] nullBitArray = new byte[record.attributeList.size()];
        for(int i=0; i<record.attributeList.size(); i++){
            if(record.attributeList.get(i).data==null){
                nullBitArray[i]=1;
            } else {
                nullBitArray[i]=0;
            }
        }
        byteLists.add(nullBitArray);

        //encode values
        for(AttributeValue attributeValue : record.attributeList){

            ByteBuffer byteBuffer = null;

            System.out.println("Attribute: "+attributeValue);
            if(attributeValue.data!=null){
                switch(attributeValue.type){
                    case DataTypes.INTEGER:
                        System.out.println("Its an int!");
                        byteBuffer = ByteBuffer.allocate(Integer.BYTES);
                        byteBuffer.putInt((Integer) attributeValue.data);
                        break;
                    case DataTypes.DOUBLE:
                        System.out.println("Its a double!");
                        byteBuffer = ByteBuffer.allocate(Double.BYTES);
                        byteBuffer.putDouble((Double) attributeValue.data);
                        break;

                    default:
                        break;
                }
            }

            if(byteBuffer!=null){
                byte[] byteList = byteBuffer.array();
                byteLists.add(byteList);
            }

        }

        //find size of final array
        int finalByteArraySize = 0;
        for(int i=0; i<byteLists.size(); i++){
            System.out.println(Arrays.toString(byteLists.get(i)));
            finalByteArraySize+=byteLists.get(i).length;
        }

        byte[] finalByteArray = new byte[finalByteArraySize];

        //populate final array
        int finalArrayIndex = 0;
        for(byte[] list : byteLists){
            for(byte b : list){
                finalByteArray[finalArrayIndex]=b;
                finalArrayIndex++;
            }
        }


        return finalByteArray;

        /*
        TableSchema tableSchema = dataCatalog.getTableSchema(page.tableName);

        for(AttributeSchema value : tableSchema.attributeSchemas.values()){
            dataTypes.add(value.getDataType());
            //todo iterate through this to get datatypes of attributes and their length
        }

         */

        //todo if page has been modified:
        //todo convert pages to byteBuffer
        //todo send (page id, byteBuffer) to storage manager

    }

}


class BufferMain{
    public static void main(String[] args) throws Exception {
        BufferManager bufferManager = new BufferManager(50);

        //make a test table structure

        AttributeValue attribute1 = new AttributeValue(1, DataTypes.INTEGER);
        AttributeValue attribute2 = new AttributeValue(null, DataTypes.INTEGER);
        AttributeValue attribute3 = new AttributeValue(3, DataTypes.INTEGER);
        Record record1 = new Record();
        record1.attributeList.add(attribute1);
        record1.attributeList.add(attribute2);
        record1.attributeList.add(attribute3);


        AttributeValue attribute4 = new AttributeValue(4, DataTypes.INTEGER);
        AttributeValue attribute5 = new AttributeValue(5, DataTypes.INTEGER);
        AttributeValue attribute6 = new AttributeValue(6, DataTypes.INTEGER);
        Record record2 = new Record();
        record2.attributeList.add(attribute4);
        record2.attributeList.add(attribute5);
        record2.attributeList.add(attribute6);



        Page testPage = new Page(1);
        testPage.recordList.add(record1);
        testPage.recordList.add(record2);

        //make a test table schema
        ArrayList<DataTypes> fakeTableSchema = new ArrayList<>();
        fakeTableSchema.add(DataTypes.INTEGER);
        fakeTableSchema.add(DataTypes.INTEGER);
        fakeTableSchema.add(DataTypes.INTEGER);


        // write it
        byte[] encodedByteArray;
        encodedByteArray = bufferManager.writePageToHardware(testPage, fakeTableSchema);

        //read it
        System.out.println("Reading...");
        Page decodedPage = bufferManager.readPageFromHardware(1,encodedByteArray, fakeTableSchema);
        System.out.println("decoded record");
        for(Record record : decodedPage.recordList){
            System.out.println("next record");
            for(AttributeValue attributeValue : record.attributeList){

                System.out.println("\t"+attributeValue);

            }
        }
    }
}
