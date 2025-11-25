package edu.uob;

import java.util.ArrayList;
import java.util.HashMap;

public class QueryCondition {

    String attribute;
    String operator;
    String testValue;

    public QueryCondition(String attribute, String operator, String value) {
        this.attribute = attribute;
        this.operator = operator;
        this.testValue = value;
    }

    public static boolean evaluateConditions(QueryConditionGroup queryData, HashMap<String, String> row) {
       if(queryData.queryConditions == null || queryData.queryConditions .isEmpty()) return true;
       boolean[] results = new boolean[queryData.queryConditions .size()];
       int count = 0;
       for(QueryCondition condition : queryData.queryConditions ) {
           String attribute = row.get(condition.attribute.toLowerCase());
           results[count] = condition.checkConditionForValue(attribute);
           count++;
        }
        if(queryData.queryConditionOperators == null || queryData.operationalPrecedences.isEmpty()) return results[0];
        Integer highest = queryData.operationalPrecedences.stream().max(Integer ::compare).get();
        boolean finalResult = false;
        for(int operationPrecedence = highest;  operationPrecedence >= 0; operationPrecedence--) {
                int index = 0;
                for(String join : queryData.queryConditionOperators) {
                    if(queryData.operationalPrecedences.get(index) == operationPrecedence){
                        boolean operand_a = results[index];
                        boolean operand_b = results[index + 1];
                        if (join.equalsIgnoreCase("AND")) finalResult = (operand_a && operand_b);
                        else if (join.equalsIgnoreCase("OR")) finalResult = (operand_a || operand_b);
                        results[index] = finalResult;
                        results[index + 1] = finalResult;
                    }
                    index++;
                }
            }
        return finalResult;
    }

    public boolean checkConditionForValue(String value) {
        float attributeFloat = 0;
        float valueFloat = 0;
        boolean numeric;
        try {
            attributeFloat = Float.parseFloat(testValue);
            valueFloat = Float.parseFloat(value);
            numeric = true;
        } catch (Exception e) {
            numeric = false;
        }
        switch (this.operator) {
            case "==":
                if(numeric) return attributeFloat == valueFloat;
                if(testValue.equalsIgnoreCase("TRUE") || testValue.equalsIgnoreCase("FALSE")){
                    return testValue.equalsIgnoreCase(value);
                }
                return testValue.equals(value);
            case "!=":
                return !(testValue.equals(value));
            case ">":
                if (numeric) { return valueFloat > attributeFloat;}
                break;
            case "<":
                if (numeric) {return valueFloat < attributeFloat;}
                break;
            case ">=":
                if (numeric) {return valueFloat >= attributeFloat;}
                break;
            case "<=":
                if (numeric) {return valueFloat <= attributeFloat;}
                break;
            case "LIKE":
                return value.contains(testValue);
        }
        return false;
    }
}