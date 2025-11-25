package edu.uob;
import java.util.ArrayList;
import java.util.Arrays;

public class SQLParser {
    
    static ArrayList<String> comparatorList = new ArrayList<>(Arrays.asList("==", ">", "<", ">=",
            "<=", "!=", "LIKE", "like"));

    static char[] symbols = {'!', '#', '$', '%', '&', '(', ')', '*', '+', ',', '-',
            '.', '/', ':', ';', '>', '=', '<', '?', '@',
            '[', '\\', ']', '^', '_', '`', '{', '{', '~'};

    ArrayList<String> tokensToParse;
    ArrayList<String> conditionalTokens = new ArrayList<>();
    
    boolean conditionalMode = false;
    String focusToken;
    String nextToken;
    int focusIndex;
    QueryCommand command = new QueryCommand();

    private void switchToConditionalMode() {
        focusIndex = 0;
        focusToken = conditionalTokens.get(focusIndex);
        conditionalMode = true;
    }

    public  SQLParser(ArrayList<String> command_tokens) {
        this.tokensToParse = command_tokens;
        this.focusToken = command_tokens.get(0);
        this.nextToken = command_tokens.get(1);
        this.focusIndex = 0;
    }

    public QueryCommand parseSQLInput() throws DBException {
        if(this.parseCommand()) return this.command;
        return null;
    }
    private void updateFocusToken(){
        this.focusIndex += 1;
        if(!conditionalMode) {
            this.focusToken = this.tokensToParse.get(this.focusIndex);
            if (this.focusIndex < tokensToParse.size() - 1)
                this.nextToken = this.tokensToParse.get(this.focusIndex + 1);
        } else {
            this.focusToken = this.conditionalTokens.get(this.focusIndex);
            if (this.focusIndex < conditionalTokens.size() - 1)
                this.nextToken = this.conditionalTokens.get(this.focusIndex + 1);
        }
    }

    private boolean parseCommand() throws DBException{
        if(tokensToParse.get(tokensToParse.size() - 1).equals(";")){
            tokensToParse.remove(tokensToParse.size()-1);
            return parseCommandType();
        } else {
            throw new DBException("Invalid query format");
        }
    }

    private boolean parseCommandType() throws DBException{
        boolean matched;
        matched = parseUse();
        if(!matched) matched = parseCreate();
        if(!matched) matched = parseDrop();
        if(!matched) matched = parseInsert();
        if(!matched) matched = parseAlter();
        if(!matched) matched = parseSelect();
        if(!matched) matched = parseDelete();
        if(!matched) matched = parseUpdate();
        if(!matched) matched = parseJoin();
        return matched;
    }


    private boolean parseJoin() throws DBException{
        if(!focusToken.equalsIgnoreCase("JOIN")) return false;
        command.setCommandType(CommandTypes.JOIN);
        updateFocusToken();
        if(!parsePlainText()) return false;
        command.setTableName(focusToken);
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("AND")) return false;
        updateFocusToken();
        if(!parsePlainText()) return false;
        command.setJoinTableName(focusToken);
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("ON")) return false;
        updateFocusToken();
        if(!parseAttributeName()) return false;
        command.addToAttributeList(focusToken);
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("AND")) return false;
        updateFocusToken();
        if(parseAttributeName() && queryTerminatesNextToken()){
            command.addToAttributeList(focusToken.toLowerCase());
            return true;
        }
        return false;
    }

    private boolean parseUpdate() {
        if(!focusToken.equalsIgnoreCase("UPDATE")) return false;
        updateFocusToken();
        if(!parsePlainText()) return false;
        command.setTableName(focusToken);
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("SET")) return false;
        updateFocusToken();
        if(!parseNameValueList()) return false;
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("WHERE")) return false;
        updateFocusToken();
        command.setCommandType(CommandTypes.UPDATE);
        return conditionScan() && queryTerminatesNextToken();
    }


    private String cleanValue(String value){
        if(value.charAt(0) == '\'') return value.substring(1, value.length()-1);
        return value;
    }

    private boolean parseNameValueList(){
        boolean check = true;
        boolean seperator = false;
        while(check){
            if(!seperator){
                check = parseNameValuePair();
                if(nextToken.equalsIgnoreCase("WHERE")) return check;
            } else{
                check = focusToken.equals(",");
            }
            seperator =!seperator;
            updateFocusToken();
            if(!check) return false;
        }
        return false;
    }

    private boolean breakdownNameValuePair(){
        if(!focusToken.contains("=")) return false;
        String[] parts = focusToken.split("=");
        if(parts.length == 2){
            focusToken = parts[0];
            if(parseAttributeName()){
                command.addToAttributeList(focusToken.toLowerCase());
                focusToken = parts[1];
                if(parseValue()){
                    focusToken = tokensToParse.get(focusIndex);
                    return true;
                }
            }
            focusToken = tokensToParse.get(focusIndex);
        } else if (parts.length == 1){
            if(focusToken.endsWith("=")){
                focusToken = parts[0];
                if(parseAttributeName()){
                    command.addToAttributeList(focusToken.toLowerCase());
                    updateFocusToken();
                    return parseValue();
                }
            }
        }
        return false;
    }

    private boolean parseNameValuePair(){
       if(breakdownNameValuePair()) return true;
       if(!parseAttributeName()) return false;
       command.addToAttributeList(focusToken.toLowerCase());
       updateFocusToken();
       if(focusToken.equals("=")){
           updateFocusToken();
           return parseValue();
       } else if (focusToken.startsWith("=")){
           focusToken = focusToken.substring(1);
           if(parseValue()){
               focusToken = tokensToParse.get(focusIndex);
               return true;
            }
        }
       return false;
    }

    private boolean parseDelete() throws DBException{
        if(!focusToken.equalsIgnoreCase("DELETE")) return false;
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("FROM")) return false;
        updateFocusToken();
        if(!parsePlainText()) return false;
        command.setTableName(focusToken);
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("WHERE")) return false;
        updateFocusToken();
        if(conditionScan()){
            command.setCommandType(CommandTypes.DELETE);
            return true;
        }
        return false;
    }


    private boolean parseSelect(){
        if(!focusToken.equalsIgnoreCase("SELECT")) return false;
        command.setCommandType(CommandTypes.SELECT);
        updateFocusToken();
        if(!(parseWildAttributeList())) return false;
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("FROM")) return false;
        updateFocusToken();
        if(parsePlainText()){
            command.setTableName(focusToken);
            if(queryTerminatesNextToken()) return true;
        } else return false;
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("WHERE")) return false;
        updateFocusToken();
      return this.conditionScan();
    }

    private boolean parseComparator(){
        return comparatorList.contains(focusToken);
    }

    private boolean parseBooleanOperator(String token){
        String tokenToUse = token;
        if(token == null) tokenToUse = focusToken;
        if(tokenToUse.equalsIgnoreCase("AND") || tokenToUse.equalsIgnoreCase("OR")){
            return true;
        }
        return false;
    }

    private boolean parseWildAttributeList(){
        if(focusToken.equals("*")) return true;
        return parseAttributeList(false);
    }

    private boolean parseAlter(){
        if(!focusToken.equalsIgnoreCase("ALTER")) return false;
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("TABLE")) return false;
        updateFocusToken();
        if (!parsePlainText()) return false;
        command.setTableName(focusToken);
        updateFocusToken();
        if(focusToken.equalsIgnoreCase("DROP") || focusToken.equalsIgnoreCase("ADD")){
            command.setAlterationType(focusToken);
            command.setCommandType(CommandTypes.ALTER);
            updateFocusToken();
            String alterAttribute = focusToken;
            if (parseAttributeName() && queryTerminatesNextToken()) command.addToAttributeList(alterAttribute);
            return true;
        }
        return false;
    }

    private boolean parseInsert() throws DBException{
        if(!focusToken.equalsIgnoreCase("INSERT")) return false;
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("INTO")) return false;
        updateFocusToken();
        if(!parsePlainText()) return false;
        command.setTableName(focusToken);
        updateFocusToken();
        if(!focusToken.equalsIgnoreCase("VALUES")) return false;
        updateFocusToken();
        if(!focusToken.equals("(")) return false;
        updateFocusToken();

        if(!parseValueList()) return false;
        if(queryTerminatesNextToken()){
              command.setCommandType(CommandTypes.INSERTROW);
              return true;
          }
          return false;
    }

    private boolean parseValueList() throws DBException{
        boolean seperator = false;
        boolean check = true;
        while(check){
            if(!seperator){
                check = parseValue();
            } else{
                check = focusToken.equals(",");
                if(focusToken.equals(")")){
                    return true;
                }
            }
            if(!check) throw new DBException(" Invalid value in values list");
            seperator = !seperator;
            updateFocusToken();
        }
        return false;
    }

    private boolean parseValue(){
        int tokenMaxIndex = focusToken.length() - 1;
        boolean outcome;
        if(focusToken.charAt(0) == '\'' && focusToken.charAt(tokenMaxIndex) == '\'') {
            return parseStringLiteral();
        }
        outcome = parseBooleanLiteral();
        if(!outcome) outcome = parseIntegerLit();
        if(!outcome) outcome = parseFloatLiteral();
        if(!outcome) outcome = (focusToken.equalsIgnoreCase("NULL"));
        return outcome;
    }

    private boolean parseFloatLiteral(){
        String[] parts = focusToken.split("\\.");
        boolean beforeDot;
        boolean afterDot;
        if(parts.length != 2) return false;
        if(parts[0].charAt(0) == '+' || parts[0].charAt(0) == '-'){
           beforeDot =  parseDigitSeq(true, parts[0]);
        } else beforeDot = parseDigitSeq(false, parts[0]);
        afterDot = parseDigitSeq(false, parts[1]);
        if(beforeDot && afterDot) {
            command.addToValList(focusToken);
            return true;
        };
        return false;
    }

    private boolean parseStringLiteral(){
        if(focusToken.isEmpty()) return true;
        int charCount = 0;
        int stringLength = focusToken.length();
        for(char c : focusToken.toCharArray()){
            if(!((charCount == 0 || charCount == stringLength - 1))) {
                if(!parseCharLiteral(c)) return false;
            }
            charCount++;
        }
        command.addToValList(cleanValue(focusToken));
        return true;
    }

    private boolean parseCharLiteral(char c){
        return parseLetter(c) || parseDigit(c) || parseSymbol(c) || parseSpace(c);
    }

    private boolean parseSpace(char c){ return c == ' '; }

    private boolean parseIntegerLit(){
        if(focusToken.charAt(0) == '-' || focusToken.charAt(0) == '+'){
            if(parseDigitSeq(true, null)){
                command.addToValList(focusToken);
                return true;
            }
        }
        if(parseDigitSeq(false, null)){
            command.addToValList(focusToken);
            return true;
        }
        return false;
    }

    private boolean parseDigitSeq(boolean includeSignOffset, String customSource){
        String sequence;
        if(customSource == null){
            sequence = focusToken;
        } else sequence = customSource;
        boolean isFirst = includeSignOffset;
        for(char c : sequence.toCharArray()){
            if(!isFirst) {
                if(!parseDigit(c)) return false;
            }
            isFirst = false;
        }
        return true;
    }

    private boolean parseBooleanLiteral(){
        if(focusToken.equalsIgnoreCase("TRUE") || focusToken.equalsIgnoreCase("FALSE")){
            command.addToValList(cleanValue(focusToken));
            return true;
        }
        return false;
    }

    private boolean parseSymbol(char c){
        for(char symbol : symbols){
            if(symbol == c) return true;
        }
        return false;
    }

    private DomainLevel determineDomainLevel() throws DBException{
        DomainLevel level;
        try {
            level = DomainLevel.valueOf(focusToken.toUpperCase());
            return level;
        }catch(IllegalArgumentException e){
            throw new DBException("Expected 'TABLE'/'DATABASE' after DROP");
        }
    }

    private boolean evaluateDrop() throws DBException{
        DomainLevel level = determineDomainLevel();
        if(level == null) {
            throw new DBException("Expected database/table in DROP command");
        }
        this.updateFocusToken();
        if(parsePlainText() && queryTerminatesNextToken()){
            if(level == DomainLevel.TABLE){
                command.setCommandType(CommandTypes.DROPTABLE);
                command.setTableName(focusToken);
            } else{
                command.setCommandType(CommandTypes.DROPDB);
                command.setDbName(focusToken);
            }
            return true;
        }
        return false;
    }

    private boolean parseDrop() throws DBException{
        if(focusToken.equalsIgnoreCase("DROP")) {
            this.updateFocusToken();
            return evaluateDrop();
        }
        return false;
    }

    private boolean parseCreateDatabase(){
        if(focusToken.equalsIgnoreCase("DATABASE")){
            this.updateFocusToken();
            if(parsePlainText() && this.queryTerminatesNextToken()){
                command.setCommandType(CommandTypes.CREATEDB);
                command.setDbName(focusToken);
                return true;
            }
        }
        return false;
    }

    private boolean parseAttributeList(boolean bracketTerminal){
        boolean seperator = false;
        boolean check = true;
        while(check){
            if(seperator) {
                check = focusToken.equals(",");
                if(focusToken.equals(")") && bracketTerminal) return true;
            } else {
                check = parseAttributeName();
                if(check) command.addToAttributeList(focusToken.toLowerCase());
                if(check && nextToken.equalsIgnoreCase("FROM") && !bracketTerminal) return true;
            }
            seperator = !seperator;
            if(!check) return false;
            updateFocusToken();
        }
        return true;
    }

    private boolean parseAttributeName(){ return parsePlainText(); }

    private boolean parseCreateTable(){
        if(focusToken.equalsIgnoreCase("TABLE")) {
            updateFocusToken();
            if (!parsePlainText()) return false;
            command.setTableName(focusToken);
            command.setCommandType(CommandTypes.CREATETABLE);
            if(queryTerminatesNextToken()) return true;
            updateFocusToken();
            if(!focusToken.equals("(")) return false;
            updateFocusToken();
            if(!parseAttributeList(true))  return  false;
            return queryTerminatesNextToken();
        };
        return false;
    }

    private boolean parseCreate(){
        if(tokensToParse.get(0).equalsIgnoreCase("CREATE")) {
            updateFocusToken();
            return parseCreateDatabase() || parseCreateTable();
        }
        return false;
    }

    private boolean parseUse(){
        if(focusToken.equalsIgnoreCase("USE")){
            updateFocusToken();
            if(parsePlainText() && this.queryTerminatesNextToken()){
                command.setCommandType(CommandTypes.USE);
                command.setDbName(focusToken);
                return true;
            }
        }
        return false;
    }

    private boolean parseDigit(char c){
            if(c >= '0' && c <= '9'){
                return true;
            };
        return false;
    }

    private  boolean parseLetter(char letter){
        return letter <= 'z' && letter >= 'A';
    }

    private boolean queryTerminatesNextToken(){
       if(!conditionalMode) {return focusIndex == tokensToParse.size() - 1;
       }else{ return focusIndex == conditionalTokens.size() - 1;}
    }

     private boolean parsePlainText() {
        for(char letter : focusToken.toCharArray()){
            if(!(parseLetter(letter) || parseDigit(letter))) return false;
        }
         return  true;
     }

     private boolean conditionPaddingScan(ArrayList<Integer[]> conditionIndexes){
        ArrayList<Integer> combined = new ArrayList<>();
        for(Integer[] index : conditionIndexes){
           combined.addAll(Arrays.asList(index));
        }
        int tokenCount = 0;
        int parenthesesLevel = 0;
        String lastToken = "";
        for(String token : conditionalTokens){
            if(!combined.contains(tokenCount)){
                if(!(parseBooleanOperator(token) || token.equals("(") || token.equals(")"))) return false;
                if(parseBooleanOperator(token)){
                    command.addConditionJoin(token);
                    command.addPrecedence(parenthesesLevel);
                }
                if(token.equalsIgnoreCase("(")) parenthesesLevel++;
                if(token.equalsIgnoreCase(")")){
                    parenthesesLevel--;
                    if(lastToken.equalsIgnoreCase("(")) return false;
                }
            }
            lastToken = token;
            tokenCount++;
        }

         return parenthesesLevel == 0;
     }

     private boolean conditionScan(){
        prepareConditionTokens();
        this.switchToConditionalMode();
        int conditionSegment = 0;
        ArrayList<Integer[]> consumedIndexes = new ArrayList<>();
        boolean check = true;
        while(check) {
            if (parseAttributeName() && conditionSegment == 0) conditionSegment++;
            else if (parseComparator() && conditionSegment == 1) conditionSegment++;
            else if (parseValue() && conditionSegment == 2) {

                int attrib_index = focusIndex - 2;
                int comp_index = focusIndex - 1;
                int value_index = focusIndex;

                Integer[] conditionIndexes = {attrib_index, comp_index, value_index};
                consumedIndexes.add(conditionIndexes);
                String[] newCondition = {conditionalTokens.get(attrib_index), conditionalTokens.get(comp_index), cleanValue(conditionalTokens.get(value_index))};
                command.addConditionSet(newCondition);
                conditionSegment = 0;
            }
            if(focusIndex == conditionalTokens.size()-1) check = false;
            else updateFocusToken();
         }
       return queryTerminatesNextToken() && conditionPaddingScan(consumedIndexes);
     }

    private void prepareConditionTokens(){
        for(int tokenIndex = focusIndex; tokenIndex < tokensToParse.size();tokenIndex++){
            String conditionToken = tokensToParse.get(tokenIndex);
            boolean breakup = false;
            for(String c : comparatorList){
                if(conditionToken.contains(c) && conditionToken.length() > c.length()){
                    String[] p = conditionToken.split(c);
                        String component = conditionToken.replaceFirst(c, "");
                        if(conditionToken.startsWith(c)) {
                            this.conditionalTokens.add(c);
                            this.conditionalTokens.add(component);
                        } else if
                        (conditionToken.endsWith(c)) {conditionalTokens.add(component);
                        this.conditionalTokens.add(c);
                    } else  if(p.length == 2) {
                        this.conditionalTokens.add(p[0]);
                        this.conditionalTokens.add(c);
                        this.conditionalTokens.add(p[1]);
                    }
                        breakup = true;
                }
            }
            if(!breakup){ this.conditionalTokens.add(conditionToken); }
        }
    }
 }