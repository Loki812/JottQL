package base.parse.DML;

public class PrioritizedOperator{

    String operator;
    int priority;

    public PrioritizedOperator(String op, int p){
        this.operator = op;
        this.priority = p;
    }
}
