package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.whereNodes.NotNullNode;
import base.models.whereNodes.WhereTreeNode;

import java.lang.Record;
import java.util.ArrayList;

import static base.parse.DML.Where.buildWhereTree;
import static base.parse.DML.Where.where;

public class Where {


    public static WhereTreeNode buildWhereTree(String wherePieces){

        System.out.println(wherePieces);






        //todo return root of where tree
        WhereTreeNode root = new NotNullNode("columnName");
        return root;
    }


    public static ArrayList<Record> where(Page page, String tableName, WhereTreeNode whereTree){
        //todo call buildWhereTree

        /*
        for (i in page's list of record){
            check if it should be in the new table from where-condition

            if( page's nextTableID != -1){
                do the loop again on the next page
            }

        }
         */

        //todo for temporary tables, call TableSchema's copy() function

        ArrayList<Record> finalRecordList = new ArrayList<>();
        return finalRecordList;
    }
}

class WhereTest{
    public static void main(String[] args) throws Exception {

        //make a test table structure
        AttributeValue attribute1 = new AttributeValue(1, DataTypes.INTEGER);
        AttributeValue attribute2 = new AttributeValue(null, DataTypes.DOUBLE);
        AttributeValue attribute3 = new AttributeValue(false, DataTypes.BOOLEAN);
        AttributeValue attribute4 = new AttributeValue("hello", DataTypes.CHAR);
        AttributeValue attribute5 = new AttributeValue("world", DataTypes.VARCHAR);

        base.models.Record record1 = new base.models.Record();

        record1.attributeList.add(attribute1);
        record1.attributeList.add(attribute2);
        record1.attributeList.add(attribute3);
        record1.attributeList.add(attribute4);
        record1.attributeList.add(attribute5);

        AttributeValue attribute6 = new AttributeValue(4, DataTypes.INTEGER);
        AttributeValue attribute7 = new AttributeValue(5.5, DataTypes.DOUBLE);
        AttributeValue attribute8 = new AttributeValue(true, DataTypes.BOOLEAN);
        AttributeValue attribute9 = new AttributeValue("tuple", DataTypes.CHAR);
        AttributeValue attribute10 = new AttributeValue("varvar char", DataTypes.VARCHAR);
        base.models.Record record2 = new base.models.Record();


        record2.attributeList.add(attribute6);
        record2.attributeList.add(attribute7);
        record2.attributeList.add(attribute8);
        record2.attributeList.add(attribute9);
        record2.attributeList.add(attribute10);


        Page testPage = new Page(1,"table");
        testPage.recordList.add(record1);
        testPage.recordList.add(record2);

        //make a test table schema
        TableSchema tableSchema = new TableSchema();
        tableSchema.tableName = testPage.tableName;
        DataCatalog.getInstance().addTableSchema(tableSchema);


        AttributeSchema integer = AttributeSchema.createAttributeSchemaFromQuery("a INTEGER");
        tableSchema.addAttributeSchema(integer);

        AttributeSchema dub = AttributeSchema.createAttributeSchemaFromQuery("b DOUBLE");
        tableSchema.addAttributeSchema(dub);

        AttributeSchema bool = AttributeSchema.createAttributeSchemaFromQuery("c BOOLEAN");
        tableSchema.addAttributeSchema(bool);

        AttributeSchema car = AttributeSchema.createAttributeSchemaFromQuery("d CHAR(5)");
        tableSchema.addAttributeSchema(car);

        AttributeSchema var = AttributeSchema.createAttributeSchemaFromQuery("e VARCHAR(10)");
        tableSchema.addAttributeSchema(var);

        for(AttributeSchema a : DataCatalog.getInstance().getTableSchema(testPage.tableName).getAttributeSchemas().sequencedValues()){
            System.out.println("datatype: "+a.getDataType());
        }


        /**
         * call where() function
         */
        buildWhereTree("where a < or c >= 10 and");
        //where(testPage, "table", WhereTreeNode whereTree);




    }
}