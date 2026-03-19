package base.models.whereNodes;

import base.models.Record;
import base.models.TableSchema;


public abstract class WhereTreeNode {
    public abstract boolean eval(Record record, TableSchema schema);
}
