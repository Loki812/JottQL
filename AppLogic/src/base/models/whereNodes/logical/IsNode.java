package base.models.whereNodes.logical;

import base.models.Record;
import base.models.TableSchema;
import base.models.whereNodes.LogicNode;
import base.models.whereNodes.WhereTreeNode;

public class IsNode extends LogicNode {

    private String columnName;

    public IsNode(){}

    public IsNode(WhereTreeNode left, WhereTreeNode right) {
        super(left, right);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) {
        return (left).equals(right);
    }

    @Override
    public String toString() {
        //return left + " IS " + right;
        return " IS ";
    }
}
