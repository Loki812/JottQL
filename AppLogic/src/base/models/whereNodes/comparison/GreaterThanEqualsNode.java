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
            /*
            case DOUBLE -> {
                return ((Double) record.attributeList.get(index).data) >= ((Double) constantValue);
            }
            case INTEGER -> {
                return ((Integer) record.attributeList.get(index).data) >= ((Integer) constantValue);
            }
             */
            case DOUBLE -> {
                Double recordData;
                Double constData;
                if(record.attributeList.get(index).data instanceof String){
                    recordData = Double.parseDouble((String) record.attributeList.get(index).data);
                } else {
                    recordData = ((Double) record.attributeList.get(index).data);
                }

                if(constantValue instanceof String){
                    constData = Double.parseDouble((String) constantValue);
                } else {
                    constData = (Double) constantValue;
                }
                return recordData >= constData;
            }
            case INTEGER -> {
                Integer recordData;
                Integer constData;
                if(record.attributeList.get(index).data instanceof String){
                    recordData = Integer.parseInt((String) record.attributeList.get(index).data);
                } else {
                    recordData = ((Integer) record.attributeList.get(index).data);
                }

                if(constantValue instanceof String){
                    constData = Integer.parseInt((String) constantValue);
                } else {
                    constData = (Integer) constantValue;
                }
                return recordData >= constData;
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
