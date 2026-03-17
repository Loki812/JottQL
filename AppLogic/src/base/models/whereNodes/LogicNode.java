package base.models.whereNodes;

public abstract class LogicNode extends WhereTreeNode {
    protected WhereTreeNode left;
    protected WhereTreeNode right;

    public LogicNode(WhereTreeNode left, WhereTreeNode right) {
        this.left = left;
        this.right = right;
    }
}
