package base.models.whereNodes.logical;

import base.models.concrete.Record;
import base.models.schemas.TableSchema;
import base.models.whereNodes.LogicNode;
import base.models.whereNodes.WhereTreeNode;

public class AndNode extends LogicNode {

    public AndNode(){

    }

    public AndNode (WhereTreeNode left, WhereTreeNode right) {
        super(left, right);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) throws Exception {
        return left.eval(record, schema) && right.eval(record, schema);
    }

    @Override
    public String toString() {
        //return left + " AND " + right;
        return " AND ";
    }
}
