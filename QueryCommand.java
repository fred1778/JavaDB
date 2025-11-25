package edu.uob;

import java.util.ArrayList;

public class QueryCommand {

    private CommandTypes commandType;
    private String dbName;
    private String tableName;
    private String joinTableName;

    private ArrayList<String[]> conditionSets = new ArrayList<>();
    private ArrayList<String> conditionalJoins = new ArrayList<>();
    private ArrayList<Integer> joinPrecedances = new ArrayList<>();
    private ArrayList<String> valList = new ArrayList<>();
    private ArrayList<String> attributeList = new ArrayList<>();

    public boolean isConditional = false;

    private boolean alterationIsAdd;
    private ResponseStatus status;
    private String responseMessage;
    private String returnData;
    public ArrayList<QueryCondition> conditions = new ArrayList<>();


    public ArrayList<Integer> getPrecedence() {
        if (joinPrecedances.isEmpty()) return null;
        return joinPrecedances;
    }

    public void addPrecedence(int precedence){
        joinPrecedances.add(precedence);
    }

    public ArrayList<QueryCondition> getConditions(){
        return this.conditions;
    }

    public void buildConditions(){
        if(!conditionSets.isEmpty()) {
            for (String[] condition : conditionSets) {
                QueryCondition newCondition = new QueryCondition(condition[0], condition[1], condition[2]);
                this.conditions.add(newCondition);
            }
            this.isConditional = true;
        }
    }

    public String[] getConditionalJoin(){
        if(conditionalJoins.isEmpty())return null;
        String[] conditionalJoinsArray =  conditionalJoins.toArray(new String[conditionalJoins.size()]);
        conditionalJoins.toArray(conditionalJoinsArray);
        return conditionalJoinsArray;
    }

    public void setAlterationType(String alterationType){
        alterationIsAdd = alterationType.equalsIgnoreCase("add");
    }
    public boolean getAlterationType(){
        return alterationIsAdd;
    }
    public void addConditionJoin(String conditionJoin){
        this.conditionalJoins.add(conditionJoin);
    }
    public void addConditionSet(String[] conditionSet){
        conditionSets.add(conditionSet);
    }
    public String getJoinTableName() {
        return joinTableName;
    }

    public  void  setJoinTableName (String tableName){
        this.joinTableName = tableName;
    }

    public  void  setReturnData(String returnData){
        this.returnData = returnData;
    }
    public  void setResponseMessage(String responseMessage) {
        this.responseMessage = responseMessage;
    }
    public void setCommandType(CommandTypes comType){
        this.commandType = comType;
    }

    public void setDbName(String dbName){
        this.dbName = dbName;
    }

    public void setTableName(String tableName){
        this.tableName = tableName;
    }

    public void addToValList(String value){
        this.valList.add(value);
    }

    public void addToAttributeList(String value){
        this.attributeList.add(value);
    }

    public void setStatus(ResponseStatus status){
        this.status = status;
        switch (status){
            case SUCCESS -> this.responseMessage = "[OK] : ";
            case FAILURE -> this.responseMessage = "[ERROR] : ";
        }
    }

    public String generateOutput(){
        if(status==ResponseStatus.SUCCESS && returnData!=null){
            return getStatusResponse() + "\n" + returnData;
        }
        return getStatusResponse();
    }

    public String getStatusResponse(){
        if(this.status == ResponseStatus.SUCCESS) {
            String action = switch (commandType) {
                case USE -> "Switched Database to " + dbName;
                case CREATEDB -> "Created Database " + dbName;
                case INSERTROW -> "Row to" + tableName;
                case CREATETABLE -> "Created Table " + tableName;
                case DROPDB -> "Dropped Database " + dbName;
                case DROPTABLE -> "Dropped Table " + tableName;
                case SELECT -> "Selected data from " + tableName;
                case ALTER -> "Altered Table " + tableName;
                case DELETE -> "Deleted data from " + tableName;
                case UPDATE -> "Updated data in " + tableName;
                case JOIN -> "Joined tables";
            };
            return "[OK] : "  + action;
        } else {
           return "[ERROR] : " + this.responseMessage;
        }
    }

    public ArrayList<String> getAttributeList(){
        return this.attributeList;
    }

    public CommandTypes getCommandType(){
        return this.commandType;
    }

    public String getDbName(){
        return this.dbName;
    }

    public String getTableName(){
        return this.tableName.toLowerCase();
    }

    public ArrayList<String> getValList(){
        return this.valList;
    }
}
