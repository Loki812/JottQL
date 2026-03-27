package base.parse.DML;

public class PrioritizedWherePiece {

    String value;
    int priority;

    public PrioritizedWherePiece(String op, int p){
        this.value = op;
        this.priority = p;
    }
}
