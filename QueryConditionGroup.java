package edu.uob;

import java.util.ArrayList;

public class QueryConditionGroup {

    public String[] queryAttributes;
    public ArrayList<QueryCondition> queryConditions;
    public String[] queryConditionOperators;
    public ArrayList<Integer> operationalPrecedences;

    public QueryConditionGroup(String[] attributes, ArrayList<QueryCondition> conditions, String[] operators, ArrayList<Integer> precedences) {
        queryAttributes = attributes;
        queryConditions = conditions;
        queryConditionOperators = operators;
        operationalPrecedences = precedences;
    }


}
