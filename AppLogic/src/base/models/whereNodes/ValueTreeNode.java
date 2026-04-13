package base.models.whereNodes;

import base.models.concrete.Record;
import base.models.schemas.TableSchema;

public class ValueTreeNode extends WhereTreeNode{

    String value;

    ValueTreeNode leftChild;
    ValueTreeNode rightChild;

    public ValueTreeNode(String value){
        this.value = value;
    }

    public void setRightChild(ValueTreeNode r){
        this.rightChild = r;
    }

    public void setLeftChild(ValueTreeNode l){
        this.leftChild = l;
    }

    public ValueTreeNode getLeftChild() {
        return leftChild;
    }

    public ValueTreeNode getRightChild() {
        return rightChild;
    }

    public String getValue(){
        return value;
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
