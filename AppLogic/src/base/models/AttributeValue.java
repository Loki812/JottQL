package base.models;

public class AttributeValue<T> {
    public T data;
    public DataTypes type;

    public AttributeValue(T data, DataTypes type){
        this.data = data;
        this.type = type;
    }


    public int compareTo(AttributeValue<T> attribute){
        return this.toString().compareTo(attribute.toString());
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
