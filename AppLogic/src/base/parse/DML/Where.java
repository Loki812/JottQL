package base.parse.DML;

import base.models.Page;
import base.models.whereNodes.WhereTreeNode;

import java.util.ArrayList;

public class Where {

    public WhereTreeNode buildWhereTree(String wherePieces){
        //todo return root of where tree
    }

    public ArrayList<Record> where(Page page, String tableName, String wherePieces){
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
    }
}
