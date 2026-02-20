package base.buffer;
import base.models.*;

import java.io.IOException;
import java.nio.ByteBuffer;
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

        //todo actually make a proper getPage function
        return new Page(1);

    }

    /*

    public Page createNewPage(...){

    }

    private Page readPageFromHardware(int pageId) throws IOException {



        //todo send pageId to storage managers -> get a ByteBuffer
        ByteBuffer pageData = StorageManager.readPage(id);
        //todo convert byteBuffers to Page -> return page

        //

    }



     */


    //todo     public Page readPageFromHardware(int pageId) throws IOException {
    public Record convertBytesToRecord(byte[] encodedByteArray, ArrayList<DataTypes> fakeTableSchema) throws IOException {

        //todo the null byte array should be the same size as the length of characters
        System.out.println("encoded byteArray: "+Arrays.toString(encodedByteArray));


        int encodedIndex = 0;
        //null-byte array
        byte[] nullByteArray = Arrays.copyOfRange(encodedByteArray,encodedIndex,(encodedIndex+fakeTableSchema.size()));
        encodedIndex+=fakeTableSchema.size();
        System.out.println("null-byte array: "+Arrays.toString(nullByteArray));

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
                        default:
                            break;
                    }
                }


                if(byteBuffer!=null){
                    byte[] byteList = byteBuffer.array();
                    byteLists.add(byteList);
                }


            }
        }




        //find size of final array
        int finalByteArraySize = 0;
        for(int i=0; i<byteLists.size(); i++){
            System.out.println(Arrays.toString(byteLists.get(i)));
            finalByteArraySize+=byteLists.get(i).length;
        }

        byte[] finalByteArray = new byte[finalByteArraySize];

        //todo populate final array
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
        Record testRecord = new Record();
        testRecord.attributeList.add(attribute1);
        testRecord.attributeList.add(attribute2);
        testRecord.attributeList.add(attribute3);
        Page testPage = new Page(1);
        testPage.recordList.add(testRecord);

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
        Record decodedRecord = bufferManager.convertBytesToRecord(encodedByteArray, fakeTableSchema);
        System.out.println("decoded record");
        for(AttributeValue a : decodedRecord.attributeList){
            System.out.println(a);
        }

        //print the returned page
        /*
        System.out.println("Returned Page:");
        for(Record record : decodedPage.recordList){
            for(AttributeValue attributeValue : record.attributeList){

                System.out.println(attributeValue);

            }
        }

         */


    }
}
