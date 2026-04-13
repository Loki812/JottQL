package base.parse.DML;

import base.buffer.BufferManager;
import base.models.*;
import base.models.concrete.Record;
import base.models.concrete.Page;
import base.models.schemas.AttributeSchema;
import base.models.schemas.TableSchema;
import base.models.whereNodes.ComparisonNode;
import base.models.whereNodes.ValueTreeNode;
import base.models.whereNodes.WhereTreeNode;
import base.models.whereNodes.comparison.*;
import base.models.whereNodes.logical.AndNode;
import base.models.whereNodes.comparison.IsNode;
import base.models.whereNodes.logical.OrNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
                //check that it's NOT a String
                if(s.charAt(0)!='\"' || s.charAt(s.length()-1)!='\"'){
                    //check that it's NOT an int
                    try{
                        //it's an int
                        Integer.parseInt(s);
                    } catch(NumberFormatException intE) {
                        //it's NOT an int
                        try{
                            //it's a double
                            Double.parseDouble(s);
                        } catch(NumberFormatException dubE) {
                            //it's NOT a double
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
