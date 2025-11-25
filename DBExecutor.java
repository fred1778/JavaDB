package edu.uob;
// Class responsibilities: maintaining loaded data across requests, including manifest and table data, taking in commands and
// executing them with this data and by using the DBManager. Responsible for 'query' errors e.g. db does not exist,  etc.
import java.util.ArrayList;
import java.util.Arrays;

public class DBExecutor {
    static ArrayList<String> reservedWords = new ArrayList<>(Arrays.asList(
            "TRUE", "DELETE", "ALTER", "JOIN",
            "UPDATE", "ADD", "VALUES", "SET", "FALSE", "AND",
            "OR", "DROP", "USE", "SELECT", "TABLE", "DATABASE",
            "WHERE", "FROM", "LIKE", "ID", "ON", "CREATE"));

    private DatabaseManager databaseManager;
    private QueryCommand db_command;
    private String[] executionAttributes;
    private String[] conditionAttributes;
    private String[] executionValues;
    private TableData tableData;
    private QueryConditionGroup queryConditionData;

    public  DBExecutor(DatabaseManager databaseManager, QueryCommand db_command) {
        this.databaseManager = databaseManager;
        this.db_command = db_command;
        this.db_command.buildConditions();

        this.executionValues = new String[db_command.getValList().size()];
        this.executionAttributes = new String[db_command.getAttributeList().size()];
        this.conditionAttributes = new String[this.db_command.getConditions().size()];
        this.executionAttributes = db_command.getAttributeList().toArray(executionAttributes);
        this.executionValues = db_command.getValList().toArray(executionValues);


        this.queryConditionData = new QueryConditionGroup(executionAttributes,
                db_command.getConditions(),
                db_command.getConditionalJoin(),
                db_command.getPrecedence());


        int conditionIndex = 0;
        for(QueryCondition condition :this.db_command.getConditions()){
            conditionAttributes[conditionIndex] = condition.attribute;
            conditionIndex++;
        }
    }

    private boolean attributeChecks(Boolean shouldFail){
        return this.attributeExistsCheck(shouldFail, false) && this.attributeExistsCheck(shouldFail, true);
    }

    private boolean checkForDuplicates(){
        if(this.executionAttributes.length != Arrays.stream(this.executionAttributes).distinct().count()){
            db_command.setStatus(ResponseStatus.FAILURE);
            db_command.setResponseMessage("Duplicate attributes");
            return false;
        }
        return true;
    }

    private boolean screenName(String candidateName){
        if(reservedWords.contains(candidateName.toUpperCase())){
            db_command.setStatus(ResponseStatus.FAILURE);
            db_command.setResponseMessage("Name not permitted");
            return false;
        }
        return true;
    }

    private boolean screenAttributes() {
        for (String attribute : this.executionAttributes) {
            if(reservedWords.contains(attribute.toUpperCase()) || attribute.equalsIgnoreCase("id")){
                db_command.setStatus(ResponseStatus.FAILURE);
                db_command.setResponseMessage("Attribute name(s) not permitted");
                return false;
            }
        }
        return true;
    }

    private boolean attributeExistsCheck(Boolean failCommand,Boolean conditions){
        String[] toCheck = conditions ? this.conditionAttributes : this.executionAttributes;
        if(!this.tableData.verifyColumns(toCheck, null)) {
           if(failCommand) {
               db_command.setStatus(ResponseStatus.FAILURE);
               db_command.setResponseMessage("Invalid column names");
           }
           return false;
       }
       return true;
    }

    private void loadTableData() throws Exception{
        if(this.db_command.getTableName() != null){
            this.tableData = this.databaseManager.loadTable(db_command.getTableName());
        }
    }

    public void executeCommand(){
        this.db_command.setStatus(ResponseStatus.SUCCESS);
        try {
            switch (db_command.getCommandType()) {
               case USE:
                     DatabaseManager.setTargetDatabase(db_command.getDbName()); break;
               case CREATEDB:
                   if(screenName(db_command.getDbName())) this.databaseManager.createNewDatabase(db_command.getDbName()); break;
               case CREATETABLE:
                   if(screenName(db_command.getTableName())){
                   if(screenAttributes() && checkForDuplicates()) {
                       this.databaseManager.createNewTable(db_command.getTableName(), this.executionAttributes);}
                   } break;
               case INSERTROW:
                   executeInsert(); break;
               case SELECT:
                   loadTableData();
                   if(this.attributeChecks(true)) {
                   this.db_command.setReturnData(tableData.getDataForConditions(this.queryConditionData));
                     }break;
               case ALTER :
                   executeAlter(); break;
               case DROPTABLE:
                   this.databaseManager.dropTable(db_command.getTableName());break;
               case DROPDB:
                   this.databaseManager.dropDatabase(db_command.getDbName());break;
               case DELETE:
                   executeDelete(); break;
               case UPDATE:
                   executeUpdate(); break;
               case JOIN:
                   executeJoin(); break;
           }
       } catch (Exception error){
           this.db_command.setStatus(ResponseStatus.FAILURE);
           this.db_command.setResponseMessage(error.getMessage());
       }
    }

    private void executeInsert() throws Exception{

        loadTableData();
        if(tableData.headers.size() - 1 == executionValues.length) {
            this.databaseManager.insertToTable(db_command.getTableName(), executionValues);
        } else {
            db_command.setStatus(ResponseStatus.FAILURE);
            db_command.setResponseMessage(" Cannot insert row: invalid number of columns");
        }

    }

    private void executeJoin() throws Exception{
        TableData table_A = this.databaseManager.loadTable(db_command.getTableName());
        TableData table_B = this.databaseManager.loadTable(db_command.getJoinTableName());
        this.db_command.setReturnData(table_A.joinTable(table_B, this.executionAttributes));
    }



    private void executeDelete() throws Exception{
        loadTableData();
        if(this.attributeChecks(true)){
            tableData.deleteDataFromTable(this.queryConditionData);
            databaseManager.writeTableDataToDisk(tableData);
        }
    }

    private void executeUpdate() throws Exception{
        loadTableData();
        if(screenAttributes() && attributeChecks(true)) {
            tableData = this.databaseManager.loadTable(db_command.getTableName());
            tableData.updateValues(this.executionAttributes, executionValues, this.queryConditionData);
            databaseManager.writeTableDataToDisk(tableData);
        }
    }

    private void executeAlter() throws Exception{
        loadTableData();
        if(this.screenAttributes()) {
            if(db_command.getAlterationType() == this.attributeChecks(false)) {
                db_command.setStatus(ResponseStatus.FAILURE);
                db_command.setResponseMessage(" Cannot alter table:  invalid column names");
            }
            tableData.processAlteration(db_command.getAlterationType(), this.executionAttributes[0]);
            databaseManager.writeTableDataToDisk(tableData);
        }
    }
}

