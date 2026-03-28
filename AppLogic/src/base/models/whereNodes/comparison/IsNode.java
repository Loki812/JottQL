package base.models.whereNodes.comparison;

import base.models.AttributeSchema;
import base.models.DataTypes;
import base.models.Record;
import base.models.TableSchema;
import base.models.whereNodes.ComparisonNode;
import base.models.whereNodes.LogicNode;
import base.models.whereNodes.ValueTreeNode;
import base.models.whereNodes.WhereTreeNode;

public class IsNode extends ComparisonNode {

    public IsNode(){

    }

    public IsNode(String columnName, Object constantValue) {
        super(columnName, constantValue);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) throws Exception {
        AttributeSchema aSchema = schema.getAttributeSchemas().get(columnName);
        int index = schema.getIndex(columnName);

        String recordData;
        if(record.attributeList.get(index).data == null){
            recordData = null;
        } else {
            recordData = record.attributeList.get(index).data.toString();
        }
        //String constData = constantValue.toString();

        return recordData==null;

    }

    @Override
    public String toString() {
        //return (columnName + " = " + constantValue);
        return (" IS ");
    }
}
