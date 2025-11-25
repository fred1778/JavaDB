
package edu.uob;
import java.util.ArrayList;
import java.util.Arrays;

public class QueryLexer {
    private  ArrayList<String> tokens = new ArrayList<String>();
    private  String[] specialCharacters = {"(", ")", ",", ";"};
    private  String command;

    public QueryLexer(String inputCommand)
    {
        this.command = inputCommand;
    }
    public  ArrayList<String> getTokens(){
        return tokens;
    }
    public  void parseCommand() {
        splitQuery();
    }


    private  void tokeniseString(String query) {
        for(int i = 0; i < specialCharacters.length; i++) {
            query = query.replace(specialCharacters[i], " " + specialCharacters[i] + " ");
        }
        while (query.contains("  ")) query = query.replace("  ", " "); // Replace two spaces by one
        query = query.trim();
        tokens.addAll(Arrays.asList(query.split(" ")));

    }

    private  void splitQuery() {

        String[] fragments = this.command.split("'");
        for (int index=0; index<fragments.length; index++) {
            if (index%2 != 0) tokens.add("'" + fragments[index] + "'");
            else {
                tokeniseString(fragments[index]);
            }
        }

    }

}