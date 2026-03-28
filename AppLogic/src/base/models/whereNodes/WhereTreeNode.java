package base.models.whereNodes;

import base.models.Record;
import base.models.TableSchema;


public abstract class WhereTreeNode {

    public WhereTreeNode left;
    public WhereTreeNode right;

    public void setLeft(WhereTreeNode left) {
        this.left = left;
    }

    public void setRight(WhereTreeNode right) {
        this.right = right;
    }

    public WhereTreeNode getLeft() {
        return left;
    }

    public WhereTreeNode getRight() {
        return right;
    }

    public abstract boolean eval(Record record, TableSchema schema) throws Exception;
}
