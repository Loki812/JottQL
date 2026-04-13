package base.models.whereNodes.comparison;

import base.models.schemas.AttributeSchema;
import base.models.concrete.Record;
import base.models.schemas.TableSchema;
import base.models.whereNodes.ComparisonNode;

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
