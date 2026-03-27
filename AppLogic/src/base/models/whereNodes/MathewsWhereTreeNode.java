package base.models.whereNodes;

import base.models.Record;
import base.models.TableSchema;

public class MathewsWhereTreeNode extends WhereTreeNode{

    String value;

    MathewsWhereTreeNode leftChild;
    MathewsWhereTreeNode rightChild;

    public MathewsWhereTreeNode(String value){
        this.value = value;
    }

    public void setRightChild(MathewsWhereTreeNode r){
        this.rightChild = r;
    }

    public void setLeftChild(MathewsWhereTreeNode l){
        this.leftChild = l;
    }

    public MathewsWhereTreeNode getLeftChild() {
        return leftChild;
    }

    public MathewsWhereTreeNode getRightChild() {
        return rightChild;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean eval(Record record, TableSchema schema) {
        return true;
    }
}
