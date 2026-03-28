package base.models.whereNodes.logical;

import base.models.Record;
import base.models.TableSchema;
import base.models.whereNodes.LogicNode;
import base.models.whereNodes.WhereTreeNode;

public class OrNode extends LogicNode {

    public OrNode(){

    }

    public OrNode(WhereTreeNode left, WhereTreeNode right) {
        super(left, right);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) {
        return left.eval(record, schema) || right.eval(record, schema);
    }

    @Override
    public String toString() {
        //return left + " OR " + right;
        return " OR ";
    }
}
