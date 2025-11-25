package edu.uob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;

/** This class implements the DB server. */
public class DBServer {
    private static final char END_OF_TRANSMISSION = 4;
    private String storageFolderPath;
    private DatabaseManager dbManager;

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
    * KEEP this signature otherwise we won't be able to mark your submission correctly.
    */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));

        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }

        // Set up processing classes....
        DBManifest.shared.provisionManifest();
        this.dbManager = new DatabaseManager(storageFolderPath);

    }

    /**
    * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
    * able to mark your submission correctly.
    *
    * <p>This method handles all incoming DB commands and carries out the required actions.
    */
    public String handleCommand(String command) {
        // TODO implement your server logic here
        QueryLexer queryLexer = new QueryLexer(command);
        QueryCommand queryCommand = new QueryCommand();
        // Step 1 - use lexer to generate tokens
        ArrayList<String> tokens;
        try{
            queryLexer.parseCommand();
            tokens = queryLexer.getTokens();
            if(tokens.size() < 2) { throw new DBException("Invalid command"); }
        } catch (Exception error){ return "[ERROR] : Unable to process command " + command;}

        // Step 2 - use tokens to parse SQL:
        try {
            SQLParser sqlParser = new SQLParser(tokens);
            queryCommand = sqlParser.parseSQLInput();
        }catch(Exception exception) {
            // capture exception messages thrown from SQLParser for details...
            return "[ERROR] : " + exception.getMessage();
        }
        // Step 3 - Pass queryCommand to executor, errors will be handled by executor so just need to print message:
        if(queryCommand != null) {
            DBExecutor dbExecutor = new DBExecutor(dbManager, queryCommand);
            dbExecutor.executeCommand();
            return queryCommand.generateOutput();
        } else {
            return "[ERROR] : No command found";
        }
    }




    //  === Methods below handle networking aspects of the project - you will not need to change these ! ===

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }
}
