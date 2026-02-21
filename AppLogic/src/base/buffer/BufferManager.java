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
    private StorageManager storageManager;

    //todo make static instance of the BufferManager similar to how the bufferManager is done
    private static BufferManager bufferManager = null;

    public BufferManager(int maxPageCount, String filename) throws Exception {
        this.maxPageCount = maxPageCount;
        this.storageManager = new StorageManager(filename);
        //this.dataCatalog = DataCatalog.getInstance();
    }


    //todo buffer is inside of the buffermanage (stored as Hashmap of Pages)

    //todo to get least recently used page, get the values and find the oldest timestamp

    //todo createNewPage() -> create new Page object and store it in the hashmap

    //todo if createNewPage() is called and the # of entries in the hash, eject the least recently used page to hardware.
    //todo call storage manager to save the page



    public static Page getPage(int id){
        if(id==-1){
            return null;
        }
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

    public static Page createNewPage(int id){
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
    //public Page readPageFromHardware(int pageId, byte[] encodedByteArray, ArrayList<DataTypes> fakeTableSchema) throws IOException {
    public static Page readPageFromHardware(int pageId, ArrayList<DataTypes> fakeTableSchema) throws IOException {

        //get byte array from hardware
        //ByteBuffer buffer = StorageManager.readPage(pageId);
        byte[] encodedByteArray = StorageManager.readPage(pageId);

        int encodedIndex = 0;
        Page finalPage = new Page(pageId);
        while(encodedIndex<encodedByteArray.length){
            Record finalRecord = new Record();
            int startByte = encodedIndex;
            //null-byte array
            byte[] nullByteArray = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+fakeTableSchema.size()));
            encodedIndex+=fakeTableSchema.size();
            //System.out.println("null-byte array: "+Arrays.toString(nullByteArray));

            //objects in byte array
            for(int i=0; i<nullByteArray.length; i++){
                DataTypes dataType = fakeTableSchema.get(i);
                //check that data is != null
                if(nullByteArray[i]!=1){
                    switch(dataType){
                        case DataTypes.INTEGER:
                            encodedIndex+=Integer.BYTES;
                            break;
                        case DataTypes.DOUBLE:
                            encodedIndex+=Double.BYTES;
                            break;
                        case DataTypes.BOOLEAN:
                            encodedIndex+=1;
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


    public static Record convertBytesToRecord(byte[] encodedByteArray, ArrayList<DataTypes> fakeTableSchema) throws IOException {

        //todo the null byte array should be the same size as the length of characters
        System.out.println("encoded byteArray: "+Arrays.toString(encodedByteArray));


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
                byte[] dataSegment;
                switch(dataType){
                    case DataTypes.INTEGER:
                        dataSegment = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+Integer.BYTES));
                        encodedIndex+=Integer.BYTES;
                        attribute = new AttributeValue<>(ByteBuffer.wrap(dataSegment).getInt(), dataType);
                        break;
                    case DataTypes.DOUBLE:
                        dataSegment = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+Double.BYTES));
                        encodedIndex+=Double.BYTES;
                        attribute = new AttributeValue<>(ByteBuffer.wrap(dataSegment).getDouble(), dataType);
                        break;
                    case DataTypes.BOOLEAN:
                        dataSegment = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+1));
                        encodedIndex+=1;
                        Byte boolByte = ByteBuffer.wrap(dataSegment).get();
                        boolean bool;
                        if(boolByte==1){
                            bool=true;
                        }else{
                            bool=false;
                        }
                        attribute = new AttributeValue<>(bool, dataType);
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
    //public byte[] writePageToHardware(Page page, ArrayList<DataTypes> fakeTableSchema){
    public static void writePageToHardware(Page page, ArrayList<DataTypes> fakeTableSchema) throws IOException {

        ArrayList<byte[]> byteLists = new ArrayList<>();
        for(Record record : page.recordList){
            byteLists.add(convertRecordToBytes(record, fakeTableSchema));
        }

        //find size of final array
        int finalByteArraySize = 0;
        for(int i=0; i<byteLists.size(); i++){
            //System.out.println(Arrays.toString(byteLists.get(i)));
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


        //return finalByteArray;
        ByteBuffer buffer = ByteBuffer.wrap(finalByteArray);

        //todo fix this
        System.out.println("buffer lengeth: "+buffer.array().length);
        StorageManager.writePage(page.pageId, buffer);
    }


    public static byte[] convertRecordToBytes(Record record, ArrayList<DataTypes> fakeTableSchema){

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

            //System.out.println("Attribute: "+attributeValue);
            if(attributeValue.data!=null){
                switch(attributeValue.type){
                    case DataTypes.INTEGER:
                        byteBuffer = ByteBuffer.allocate(Integer.BYTES);
                        byteBuffer.putInt((Integer) attributeValue.data);
                        break;
                    case DataTypes.DOUBLE:
                        byteBuffer = ByteBuffer.allocate(Double.BYTES);
                        byteBuffer.putDouble((Double) attributeValue.data);
                        break;
                    case DataTypes.BOOLEAN:
                        System.out.println("Its a bool!");
                        byteBuffer = ByteBuffer.allocate(1);
                        boolean bool = (Boolean)attributeValue.data;
                        Byte boolByte = (byte)(bool ? 1 : 0);
                        byteBuffer.put(boolByte);
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


        System.out.println("byte array: "+Arrays.toString(finalByteArray));
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
        DataCatalog.buildCatalog(500,"data");
        BufferManager bufferManager = new BufferManager(500, "storage.bin");

        //make a test table structure

        AttributeValue attribute1 = new AttributeValue(1, DataTypes.INTEGER);
        AttributeValue attribute2 = new AttributeValue(null, DataTypes.DOUBLE);
        AttributeValue attribute3 = new AttributeValue(false, DataTypes.BOOLEAN);
        Record record1 = new Record();
        record1.attributeList.add(attribute1);
        record1.attributeList.add(attribute2);
        record1.attributeList.add(attribute3);


        AttributeValue attribute4 = new AttributeValue(4, DataTypes.INTEGER);
        AttributeValue attribute5 = new AttributeValue(5.5, DataTypes.DOUBLE);
        AttributeValue attribute6 = new AttributeValue(true, DataTypes.BOOLEAN);
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
        fakeTableSchema.add(DataTypes.DOUBLE);
        fakeTableSchema.add(DataTypes.BOOLEAN);


        // write it
        byte[] encodedByteArray;
        //ncodedByteArray = bufferManager.writePageToHardware(testPage, fakeTableSchema);
        bufferManager.writePageToHardware(testPage, fakeTableSchema);


        //read it
        System.out.println("Reading...");
        //Page decodedPage = bufferManager.readPageFromHardware(1,encodedByteArray, fakeTableSchema);
        Page decodedPage = bufferManager.readPageFromHardware(1, fakeTableSchema);
        System.out.println("decoded record");
        for(Record record : decodedPage.recordList){
            System.out.println("next record");
            for(AttributeValue attributeValue : record.attributeList){

                System.out.println("\t"+attributeValue);

            }
        }
    }
}
