package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.Record;
import base.models.whereNodes.ComparisonNode;
import base.models.whereNodes.ValueTreeNode;
import base.models.whereNodes.WhereTreeNode;
import base.models.whereNodes.comparison.*;
import base.models.whereNodes.logical.AndNode;
import base.models.whereNodes.logical.IsNode;
import base.models.whereNodes.logical.OrNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static base.parse.DML.Where.buildWhereTree;

public class Where {

    public static PrioritizedWherePiece getOperatorPriority(String s, String tableName) throws Exception {
        switch (s){
            case ">":
                return new PrioritizedWherePiece(s, 1, new GreaterThanNode());
            case ">=":
                return new PrioritizedWherePiece(s, 1, new GreaterThanEqualsNode());
            case "<":
                return new PrioritizedWherePiece(s, 1, new LessThanNode());
            case "<=":
                return new PrioritizedWherePiece(s, 1, new LessThanEqualsNode());
            case "=":
                return new PrioritizedWherePiece(s, 1, new EqualsNode());
            case "<>":
                return new PrioritizedWherePiece(s, 1, new NotEqualsNode());
            case "IS":
                return new PrioritizedWherePiece(s, 1, new IsNode());
            case "AND":
                return new PrioritizedWherePiece(s, 2, new AndNode());
            case "OR":
                return new PrioritizedWherePiece(s, 3, new OrNode());
            case "TRUE":
                return new PrioritizedWherePiece(s, 0, new ValueTreeNode(s));
            case "FALSE":
                return new PrioritizedWherePiece(s, 0, new ValueTreeNode(s));
            case "NULL":
                return new PrioritizedWherePiece(s, 0, new ValueTreeNode(s));
            default:
                System.out.println("token: "+s);
                //check that it's NOT a String
                if(s.charAt(0)!='\"' || s.charAt(s.length()-1)!='\"'){
                    //check that it's NOT an int
                    try{
                        //its an int
                        Integer.parseInt(s);
                    } catch(NumberFormatException intE) {
                        //its NOT an int
                        try{
                            //its a double
                            Double.parseDouble(s);
                        } catch(NumberFormatException dubE) {
                            //its NOT a double
                            //check that it's NOT an attribute
                            DataCatalog dc = DataCatalog.getInstance();
                            TableSchema tableSchema = dc.getTableSchema(tableName);
                            AttributeSchema attribute = tableSchema.getAttributeSchemas().get(s);
                            if(attribute==null){
                                //throw error
                                System.out.println("Invalid input in the WHERE clause.");
                                throw new Exception();
                            }

                        }
                    }
                }
        }
        return new PrioritizedWherePiece(s, 0, new ValueTreeNode(s));
    }


    public static WhereTreeNode makeLeafNode(List<PrioritizedWherePiece> prioritizedWherePieces){

        int maxPriorityIndex = 0;
        for(int i=0; i<prioritizedWherePieces.size(); i++){
            if(prioritizedWherePieces.get(i).priority >= prioritizedWherePieces.get(maxPriorityIndex).priority){
                maxPriorityIndex = i;
            }
        }
        WhereTreeNode root;

        root= prioritizedWherePieces.get(maxPriorityIndex).node;
        //make left list
        List<PrioritizedWherePiece> leftList = prioritizedWherePieces.subList(0,maxPriorityIndex);
        //make right list
        List<PrioritizedWherePiece> rightList = new ArrayList<>();
        if(maxPriorityIndex<(prioritizedWherePieces.size()-1)){
            rightList = prioritizedWherePieces.subList(maxPriorityIndex+1,prioritizedWherePieces.size());
        }

        /*
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
         */


        if(!leftList.isEmpty()){
            root.setLeft(makeLeafNode(leftList));
            //root.setLeftChild(makeLeafNode(leftList));
        }
        if(!rightList.isEmpty()){
            root.setRight(makeLeafNode(rightList));
            //root.setRightChild(makeLeafNode(rightList));
        }

        //set col name and const val for ComparisonNodes
        if(root instanceof ComparisonNode){
            if(root.left instanceof ValueTreeNode){
                ((ComparisonNode) root).setColumnName(((ValueTreeNode)root.left).getValue());
            }
            if(root.right instanceof ValueTreeNode){
                ((ComparisonNode) root).setConstantValue(((ValueTreeNode)root.right).getValue());
            }
        }


        //default case
        return root;
    }

    public static WhereTreeNode buildWhereTree(String wherePieces, String tableName) throws Exception {

        wherePieces = wherePieces.replace(";","");
        wherePieces = wherePieces.toUpperCase();
        List<String> splitList = Arrays.asList(wherePieces.split("\\s+"));

        //remove the "where string and anything before it"
        int whereIndex = splitList.indexOf("WHERE");

        ArrayList<String> pieceList;
        if(whereIndex>=0){
            splitList = splitList.subList(whereIndex, splitList.size());
        }
        pieceList = new ArrayList<>(splitList);
        if(whereIndex>=0){
            pieceList.removeFirst();
        }



        //Make a list of PrioritizedWherePieces
        ArrayList<PrioritizedWherePiece> prioritizedWherePieces = new ArrayList<>();
        for(String token : pieceList){
            PrioritizedWherePiece indexedOp = getOperatorPriority(token, tableName);
            prioritizedWherePieces.add(indexedOp);
        }

        //make the Mathew-tree
        WhereTreeNode root = makeLeafNode(prioritizedWherePieces);


        //return root of where tree
        return root;

    }


    public static String executeWhere(String tableName, WhereTreeNode whereTree) throws Exception {

        // Get instances of the DataCatalog and BufferManager
        DataCatalog dc = DataCatalog.getInstance();
        BufferManager bm = BufferManager.getInstance();

        // Duplicate the table schema
        TableSchema tableSchema = dc.getTableSchema(tableName);
        TableSchema copy = tableSchema.makeTempCopy(new ArrayList<>());


        // Go through the table's pages and insert each record into the table
        int pageId = tableSchema.rootPageID;
        while (pageId != -1) {
            Page page = bm.getPage(pageId);
            for (Record r : page.recordList) {
                if(whereTree.eval(r, tableSchema)){
                    bm.insertRecordIntoTableAllowDuplicates(copy.tableName, r);
                }
            }

            pageId = page.nextPageId;
        }

        return copy.tableName;

    }
}










class WhereTest{

    static void inOrder(WhereTreeNode node, ArrayList<String> res) {
        if (node == null)
            return;

        // Traverse the left subtree first
        inOrder(node.getLeft(), res);

        // Visit the current node
        res.add(node.toString());

        // Traverse the right subtree last
        inOrder(node.getRight(), res);
    }

    public static void main(String[] args) throws Exception {

        DataCatalog.buildCatalog(4096, "C:\\Users\\mprok\\JavaProjects\\Database Impemented Systems\\JottQL\\data");
        DataCatalog dc = DataCatalog.getInstance();
        BufferManager bm = BufferManager.buildBufferManager(10,"C:\\Users\\mprok\\JavaProjects\\Database Impemented Systems\\JottQL\\data");

        //make a test table structure
        AttributeValue attribute1 = new AttributeValue(5, DataTypes.INTEGER);
        AttributeValue attribute2 = new AttributeValue(4, DataTypes.INTEGER);
        AttributeValue attribute3 = new AttributeValue(3, DataTypes.INTEGER);
        //AttributeValue attribute4 = new AttributeValue("hello", DataTypes.BOOLEAN);


        base.models.Record record1 = new base.models.Record();

        record1.attributeList.add(attribute1);
        record1.attributeList.add(attribute2);
        record1.attributeList.add(attribute3);
        //record1.attributeList.add(attribute4);

        AttributeValue attribute5 = new AttributeValue(5, DataTypes.INTEGER);
        AttributeValue attribute6 = new AttributeValue(6, DataTypes.INTEGER);
        AttributeValue attribute7 = new AttributeValue(7, DataTypes.INTEGER);
        //AttributeValue attribute8 = new AttributeValue(true, DataTypes.BOOLEAN);
        base.models.Record record2 = new base.models.Record();


        record2.attributeList.add(attribute5);
        record2.attributeList.add(attribute6);
        record2.attributeList.add(attribute7);
        //record2.attributeList.add(attribute8);
        //record2.attributeList.add(attribute9);
        //record2.attributeList.add(attribute10);


        Page testPage = new Page(1,"table");
        testPage.recordList.add(record1);
        testPage.recordList.add(record2);

        //make a test table schema
        TableSchema tableSchema = new TableSchema();
        tableSchema.tableName = testPage.tableName;
        DataCatalog.getInstance().addTableSchema(tableSchema);


        AttributeSchema int1 = AttributeSchema.createAttributeSchemaFromQuery("a INTEGER");
        tableSchema.addAttributeSchema(int1);

        AttributeSchema int2 = AttributeSchema.createAttributeSchemaFromQuery("b DOUBLE");
        tableSchema.addAttributeSchema(int2);

        AttributeSchema int3 = AttributeSchema.createAttributeSchemaFromQuery("c BOOLEAN");
        tableSchema.addAttributeSchema(int3);


        /*
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
         */

        //todo use the following print for testing
        /*
        for(AttributeSchema a : DataCatalog.getInstance().getTableSchema(testPage.tableName).getAttributeSchemas().sequencedValues()){
            System.out.println("datatype: "+a.getDataType());
        }
         */


        /**
         * call where() function
         */
        WhereTreeNode root = buildWhereTree("where a = 5 or c <= 6", testPage.tableName);

        System.out.println("root: "+root.toString());

        ArrayList<String> res = new ArrayList<>();
        inOrder(root, res);
        System.out.println();
        for(String node : res){
            System.out.print(node+" ");
        }
        System.out.println();

        /*
        TableSchema tempTableName = where(tableSchema.tableName, root);
        Page tempPage = bm.getPage(tempTableName.rootPageID);
        for(Record r : tempPage.recordList){
            System.out.println(r.toString());
        }
         */


        //where(testPage, "table", WhereTreeNode whereTree);




    }
}