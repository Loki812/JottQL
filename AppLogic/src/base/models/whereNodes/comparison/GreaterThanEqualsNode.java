package base.models.whereNodes.comparison;

import base.models.AttributeSchema;
import base.models.Record;
import base.models.TableSchema;
import base.models.whereNodes.ComparisonNode;

public class GreaterThanEqualsNode extends ComparisonNode {

    public GreaterThanEqualsNode(){

    }

    public GreaterThanEqualsNode(String columnName, Object constantValue) {
        super(columnName, constantValue);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) {
        AttributeSchema aSchema = schema.getAttributeSchemas().get(columnName);
        int index = schema.getIndex(columnName);

        switch (aSchema.getDataType()) {
            case DOUBLE -> {
                return ((Double) record.attributeList.get(index).data) >= ((Double) constantValue);
            }
            case INTEGER -> {
                return ((Integer) record.attributeList.get(index).data) >= ((Integer) constantValue);
            }
            default -> throw new RuntimeException("Datatype " + aSchema.getDataType() + " not valid for > operator");

        }
    }

    @Override
    public String toString() {
        //return (columnName + " >= " + constantValue);
        return (" >= ");
    }
}
