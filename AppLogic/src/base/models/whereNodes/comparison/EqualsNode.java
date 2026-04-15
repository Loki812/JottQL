package base.models.whereNodes.comparison;

import base.models.schemas.AttributeSchema;
import base.models.schemas.DataTypes;
import base.models.concrete.Record;
import base.models.schemas.TableSchema;
import base.models.whereNodes.ComparisonNode;

public class EqualsNode extends ComparisonNode {

    public EqualsNode(){

    }

    public EqualsNode(String columnName, Object constantValue) {
        super(columnName, constantValue);
    }

    @Override
    public boolean eval(Record record, TableSchema schema) throws Exception {
        AttributeSchema aSchema = schema.getAttributeSchemas().get(columnName);
        int index = schema.getIndex(columnName);

        if(record.attributeList.get(index).data==null){
            return false;
        }
        String recordData = record.attributeList.get(index).data.toString();
        String constData = constantValue.toString();
        if(record.attributeList.get(index).type== DataTypes.VARCHAR || record.attributeList.get(index).type== DataTypes.CHAR){
            if((constData.charAt(0)=='\"' && constData.charAt(constData.length()-1)=='\"') || constData.equals("null")){
                constData=constData.replace("\"","");
            } else {
                System.out.println("Only Strings can be compared with String type.");
                throw new Exception();
            }

        }

        if(recordData==null || constData==null){
            return false;
        }
        return constData.equalsIgnoreCase(recordData);

    }

    @Override
    public String toString() {
        //return (columnName + " = " + constantValue);
        return (" = ");
    }
}
