package base.models.whereNodes;

import base.models.Record;
import base.models.TableSchema;

public abstract class ComparisonNode extends WhereTreeNode {
    protected String columnName;
    protected Object constantValue;

    public ComparisonNode(){

    }

    public ComparisonNode(String columnName, Object constantValue) {

        // TODO add type checking between attribute schema datatype and constant value?
        this.columnName = columnName;
        this.constantValue = constantValue;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setConstantValue(Object constantValue) {
        this.constantValue = constantValue;
    }

    public abstract boolean eval(Record record, TableSchema schema);
}
