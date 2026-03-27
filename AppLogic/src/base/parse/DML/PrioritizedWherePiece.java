package base.parse.DML;

import base.models.whereNodes.WhereTreeNode;

public class PrioritizedWherePiece {

    String value;
    int priority;
    WhereTreeNode node;

    public PrioritizedWherePiece(String op, int p, WhereTreeNode node){
        this.value = op;
        this.priority = p;
        this.node = node;
    }
}
