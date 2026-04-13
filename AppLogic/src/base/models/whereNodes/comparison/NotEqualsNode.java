package base.models.whereNodes.comparison;

import base.models.schemas.AttributeSchema;
import base.models.concrete.Record;
import base.models.schemas.TableSchema;
import base.models.whereNodes.ComparisonNode;

import java.util.Objects;

public class NotEqualsNode extends ComparisonNode {

    public NotEqualsNode(){

    }

    public NotEqualsNode(String columnName, Object constantValue) {
        super(columnName, constantValue);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) throws Exception {
        AttributeSchema aSchema = schema.getAttributeSchemas().get(columnName);
        int index = schema.getIndex(columnName);

        if(record.attributeList.get(index).data==null || constantValue==null){
            return false;
        }
        return !(Objects.equals(record.attributeList.get(index).data, constantValue));
    }

    @Override
    public String toString() {
        //return (columnName + " <> " + constantValue);
        return (" <> ");
    }
}
