package base.models.whereNodes.comparison;

import base.models.AttributeSchema;
import base.models.Record;
import base.models.TableSchema;
import base.models.whereNodes.ComparisonNode;

import java.util.Objects;

public class EqualsNode extends ComparisonNode {

    public EqualsNode(String columnName, Object constantValue) {
        super(columnName, constantValue);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) {
        AttributeSchema aSchema = schema.getAttributeSchemas().get(columnName);
        int index = schema.getIndex(columnName);

        // Object.equals is indiscriminate towards types, would always return false
        // TODO add type checking between attribute schema datatype and constant value?
        return Objects.equals(record.attributeList.get(index).data, constantValue);
    }

    @Override
    public String toString() {
        return (columnName + " > " + constantValue);
    }
}
