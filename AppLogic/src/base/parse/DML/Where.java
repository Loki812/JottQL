package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.whereNodes.MathewsWhereTreeNode;
import base.models.whereNodes.WhereTreeNode;

import java.lang.Record;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static base.parse.DML.Where.buildWhereTree;

public class Where {

    public static PrioritizedWherePiece getOperatorPriority(String s){
        switch (s){
            case "(":
                return new PrioritizedWherePiece(s, 0);
            case ")":
                return new PrioritizedWherePiece(s, 0);
            /*
            case "+":
                return new PrioritizedOperator(s, 1);
            case "-":
                return new PrioritizedOperator(s, 1);
            case "*":
                return new PrioritizedOperator(s, 2);
            case "/":
                return new PrioritizedOperator(s, 2);

             */
            case ">":
                return new PrioritizedWherePiece(s, 1);
            case ">=":
                return new PrioritizedWherePiece(s, 1);
            case "<":
                return new PrioritizedWherePiece(s, 1);
            case "<=":
                return new PrioritizedWherePiece(s, 1);
            case "==":
                return new PrioritizedWherePiece(s, 1);
            case "<>":
                return new PrioritizedWherePiece(s, 1);
            case "is":
                return new PrioritizedWherePiece(s, 1);
            case "and":
                return new PrioritizedWherePiece(s, 2);
            case "or":
                return new PrioritizedWherePiece(s, 3);
        }
        return new PrioritizedWherePiece(s, 0);
    }


    public static MathewsWhereTreeNode makeLeafNode(List<PrioritizedWherePiece> prioritizedWherePieces){

        if(prioritizedWherePieces.size()==1){
            return new MathewsWhereTreeNode(prioritizedWherePieces.getFirst().value);
        }

        int maxPriorityIndex = 0;
        for(int i=0; i<prioritizedWherePieces.size(); i++){
            if(prioritizedWherePieces.get(i).priority >= prioritizedWherePieces.get(maxPriorityIndex).priority){
                maxPriorityIndex = i;
            }
        }
        MathewsWhereTreeNode root = new MathewsWhereTreeNode(prioritizedWherePieces.get(maxPriorityIndex).value);
        //make left list
        List<PrioritizedWherePiece> leftList = prioritizedWherePieces.subList(0,maxPriorityIndex);
        //make right list
        List<PrioritizedWherePiece> rightList = new ArrayList<>();
        if(maxPriorityIndex<(prioritizedWherePieces.size()-1)){
            rightList = prioritizedWherePieces.subList(maxPriorityIndex+1,prioritizedWherePieces.size());
        }


        System.out.println("max value: "+prioritizedWherePieces.get(maxPriorityIndex).value);
        System.out.println("left list: ");
        for (PrioritizedWherePiece i : leftList){
            System.out.print(i.value+", ");
        }
        System.out.println();
        System.out.println("right list: ");
        for (PrioritizedWherePiece i : rightList){
            System.out.print(i.value+", ");
        }
        System.out.println("\n");


        if(!leftList.isEmpty()){
            root.setLeftChild(makeLeafNode(leftList));
        }
        if(!rightList.isEmpty()){
            root.setRightChild(makeLeafNode(rightList));
        }


        //default case
        return root;
    }

    public static MathewsWhereTreeNode buildWhereTree(String wherePieces){


        List<String> splitList = Arrays.asList(wherePieces.split("\\s+"));

        //remove the "where string and anything before it"
        int whereIndex = splitList.indexOf("where");

        ArrayList<String> pieceList;
        if(whereIndex>=0){
            splitList = splitList.subList(whereIndex, splitList.size());
        }
        pieceList = new ArrayList<>(splitList);
        pieceList.removeFirst();


        //Make a list of PrioritizedWherePieces
        ArrayList<PrioritizedWherePiece> prioritizedWherePieces = new ArrayList<>();
        for(String token : pieceList){
            PrioritizedWherePiece indexedOp = getOperatorPriority(token);
            prioritizedWherePieces.add(indexedOp);
        }

        MathewsWhereTreeNode root = makeLeafNode(prioritizedWherePieces);



        //todo use java compareTo() operators
        //todo if you do someting like bool < false, do whatever java does

        //System.out.println(Arrays.toString(pieceList));


        //todo return root of where tree
        //MathewsWhereTreeNode root = new NotNullNode("columnName");

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

    static void inOrder(MathewsWhereTreeNode node, ArrayList<String> res) {
        if (node == null)
            return;

        // Traverse the left subtree first
        inOrder(node.getLeftChild(), res);

        // Visit the current node
        res.add(node.toString());

        // Traverse the right subtree last
        inOrder(node.getRightChild(), res);
    }

    public static void main(String[] args) throws Exception {

        DataCatalog.buildCatalog(4096, "C:\\Users\\mprok\\JavaProjects\\Database Impemented Systems\\JottQL\\data");
        DataCatalog dc = DataCatalog.getInstance();
        BufferManager bm = BufferManager.buildBufferManager(10,"C:\\Users\\mprok\\JavaProjects\\Database Impemented Systems\\JottQL\\data");

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
            //todo use the following print for testing
            //System.out.println("datatype: "+a.getDataType());
        }


        /**
         * call where() function
         */
        MathewsWhereTreeNode root = buildWhereTree("where a < b or c >= 10 and d == true and d <> false");

        System.out.println("root: "+root.toString());

        ArrayList<String> res = new ArrayList<>();
        inOrder(root, res);
        System.out.println();
        for(String node : res){
            System.out.print(node+" ");
        }


        //where(testPage, "table", WhereTreeNode whereTree);




    }
}