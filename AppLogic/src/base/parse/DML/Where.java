package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.whereNodes.MathewsWhereTreeNode;
import base.models.whereNodes.NotNullNode;
import base.models.whereNodes.WhereTreeNode;

import java.lang.Record;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import static base.parse.DML.Where.buildWhereTree;

public class Where {

    public static PrioritizedOperator getOperatorPriority(String s){
        switch (s){
            case "(":
                return new PrioritizedOperator(s, 0);
            case ")":
                return new PrioritizedOperator(s, 0);
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
                return new PrioritizedOperator(s, 1);
            case ">=":
                return new PrioritizedOperator(s, 1);
            case "<":
                return new PrioritizedOperator(s, 1);
            case "<=":
                return new PrioritizedOperator(s, 1);
            case "==":
                return new PrioritizedOperator(s, 1);
            case "<>":
                return new PrioritizedOperator(s, 1);
            case "is":
                return new PrioritizedOperator(s, 1);
            case "and":
                return new PrioritizedOperator(s, 2);
            case "or":
                return new PrioritizedOperator(s, 3);
        }
        return null;
    }

    public static MathewsWhereTreeNode buildWhereTree(String wherePieces){


        List<String> splitList = Arrays.asList(wherePieces.split("\\s+"));

        //remove the "where string and anything before it"
        int whereIndex = splitList.indexOf("where");

        System.out.println("where index: "+whereIndex);

        ArrayList<String> pieceList;
        if(whereIndex>=0){
            splitList = splitList.subList(whereIndex, splitList.size());
        }
        pieceList = new ArrayList<>(splitList);
        pieceList.removeFirst();




        //Shunting Yard algorithm
        Stack<MathewsWhereTreeNode> nodeStack = new Stack<MathewsWhereTreeNode>();

        Stack<PrioritizedOperator> operandStack = new Stack<>();
        MathewsWhereTreeNode t, t1, t2;

        for(String token : pieceList){

            PrioritizedOperator indexedOp = getOperatorPriority(token);

            //if the token is not an operator, make a node for it and put it in the stack
            if(indexedOp==null){
                MathewsWhereTreeNode node = new MathewsWhereTreeNode(token);
                nodeStack.add(node);
            } else if(indexedOp.priority>0){
                //If an operator with lower or same associativity appears
                while(!operandStack.isEmpty() && indexedOp.priority<=operandStack.peek().priority && !(operandStack.peek().operator.equals("("))){
                    //Get and remove the top element from the opStack
                    t = new MathewsWhereTreeNode(operandStack.peek().operator);
                    operandStack.pop();

                    //Get and remove the t1
                    t1 = (MathewsWhereTreeNode) nodeStack.peek();
                    nodeStack.pop();

                    //Get and remove the t2
                    t2 = (MathewsWhereTreeNode) nodeStack.peek();
                    nodeStack.pop();

                    //set t's children
                    t.setLeftChild(t2);
                    t.setRightChild(t1);

                    //push the node to the node stack
                    nodeStack.push(t);

                }

                operandStack.push(indexedOp);

            } else if (token.equals(")")){
                while(!operandStack.isEmpty() && !(operandStack.peek().operator.equals("("))){
                    t = new MathewsWhereTreeNode(operandStack.peek().operator);
                    operandStack.pop();
                    t1 = nodeStack.peek();
                    nodeStack.pop();
                    t2 = nodeStack.peek();
                    nodeStack.pop();
                    t.setLeftChild(t2);
                    t.setRightChild(t1);
                    nodeStack.add(t);

                }
                operandStack.pop();
            }

        }
        t = nodeStack.peek();
        return t;

        //todo use java compareTo() operators
        //todo if you do someting like bool < false, do whatever java does

        //System.out.println(Arrays.toString(pieceList));


        //todo return root of where tree
        //MathewsWhereTreeNode root = new NotNullNode("columnName");

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
        inOrder(node.getLeftChild(), res);
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
            System.out.println("datatype: "+a.getDataType());
        }


        /**
         * call where() function
         */
        MathewsWhereTreeNode root = buildWhereTree("where a < b or c >= 10 and d == true and d <> false");

        ArrayList<String> res = new ArrayList<>();
        inOrder(root, res);
        System.out.println();
        for(String node : res){
            System.out.print(node+" ");
        }


        //where(testPage, "table", WhereTreeNode whereTree);




    }
}