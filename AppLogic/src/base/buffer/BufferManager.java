//package base.buffer;
//import base.models.Page;
//
//import java.util.HashMap;
//
//
//public class BufferManager {
//
//    private const int maxPageCount = 1000;
//    private static HashMap<Page> buffer;
//
//    //todo make static instance of the BufferManager similar to how the bufferManager is done
//    private static BufferManager bufferManager = null;
//
//
//
//    //todo buffer is inside of the buffermanage (stored as Hashmap of Pages)
//
//    //todo to get least recently used page, get the values and find the oldest timestamp
//
//    //todo createNewPage() -> create new Page object and store it in the hashmap
//
//    //todo if createNewPage() is called and the # of entries in the hash, eject the least recently used page to hardware.
//    //todo call storage manager to save the page
//
//
//    public Page getPage(id){
//
//    }
//
//    public Page createNewPage(...){
//
//    }
//
//    private Page readPagefromHardware(id){
//
//    }
//
//    private void writePageToHardware(Page){}
//}
