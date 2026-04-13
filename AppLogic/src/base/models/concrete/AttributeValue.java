package base.models.concrete;

import base.models.schemas.DataTypes;

public class AttributeValue<T> {
    public T data;
    public DataTypes type;

    public AttributeValue(T data, DataTypes type){
        this.data = data;
        this.type = type;
    }


    public int compareTo(AttributeValue<T> attribute){
        try {
            return Double.compare(Double.parseDouble(data.toString()), Double.parseDouble(attribute.data.toString()));
        } catch (NumberFormatException e) {
            return this.toString().compareTo(attribute.toString());
        }
    }

    @Override
    public String toString() {
        if(data!=null){
            return data.toString();
        }else{
            return "null";
        }
    }
}
