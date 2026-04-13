package base.models.whereNodes;

import base.models.concrete.Record;
import base.models.schemas.TableSchema;

public class NotNullNode extends WhereTreeNode {

    private final String columnName;

    public NotNullNode(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public boolean eval(Record record, TableSchema schema) {
        int index = schema.getIndex(columnName);

        return record.attributeList.get(index).data == null;
    }

    @Override
    public String toString() {
        return columnName + " NOT NULL";
    }
}
