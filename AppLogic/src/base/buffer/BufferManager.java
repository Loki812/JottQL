package base.buffer;
import base.models.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
        this.dataCatalog = DataCatalog.getInstance();
    }


    //todo buffer is inside of the buffermanage (stored as Hashmap of Pages)

    //todo to get least recently used page, get the values and find the oldest timestamp

    //todo createNewPage() -> create new Page object and store it in the hashmap

    //todo if createNewPage() is called and the # of entries in the hash, eject the least recently used page to hardware.
    //todo call storage manager to save the page


    /*
    public Page getPage(int id){


    }

    public Page createNewPage(...){

    }

    private Page readPageFromHardware(int pageId) throws IOException {



        //todo send pageId to storage managers -> get a ByteBuffer
        ByteBuffer pageData = StorageManager.readPage(id);
        //todo convert byteBuffers to Page -> return page

        //

    }



     */
    private void writePageToHardware(Page page){

        //todo the DataCatalog should take a tableId instead of a tableName
        TableSchema tableSchema = dataCatalog.getTableSchema(page.tableId);

        for(AttributeSchema value : tableSchema.attributeSchemas.values()){
            dataTypes.add(value.getDataType());
            //todo iterate through this to get datatypes of attributes and they length
        }

        //todo if page has been modified:
        //todo convert pages to byteBuffer
        //todo send (page id, byteBuffer) to storage manager

    }

}

class BufferMain{
    public static void main(String[] args) throws Exception {
        BufferManager bufferManager = new BufferManager(50);
        Integer testInt = 5;
        AttributeValue attribute = new AttributeValue(testInt, DataTypes.INTEGER);
        Record record = new Record();
    }
}
