package base.models.whereNodes.comparison;

import base.models.schemas.AttributeSchema;
import base.models.concrete.Record;
import base.models.schemas.TableSchema;
import base.models.whereNodes.ComparisonNode;

public class LessThanNode extends ComparisonNode {

    public LessThanNode(){

    }

    public LessThanNode(String columnName, Object constantValue) {
        super(columnName, constantValue);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) throws Exception {

         AttributeSchema aSchema = schema.getAttributeSchemas().get(columnName);
         int index = schema.getIndex(columnName);

         switch (aSchema.getDataType()) {
             /*
             case DOUBLE -> {
                 return ((Double) record.attributeList.get(index).data) < ((Double) constantValue);
             }
             case INTEGER -> {
                 return ((Integer) record.attributeList.get(index).data) < ((Integer) constantValue);
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
                 if(recordData==null || constData==null){
                     return false;
                 }
                 return recordData < constData;
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
                 if(recordData==null || constData==null){
                     return false;
                 }
                 return recordData < constData;
             }
             default -> throw new RuntimeException("Datatype " + aSchema.getDataType() + " not valid for < operator");

         }
    }

    @Override
    public String toString() {
        //return (columnName + " < " + constantValue);
        return (" < ");
    }
}
