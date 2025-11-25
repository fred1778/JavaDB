package edu.uob;
import java.io.*;
import java.util.ArrayList;

// Helper class with  methods to read/write to filesystem

public class DatabaseManager {

    private static String storageRootPath;
    private static String targetDatabase;

    public DatabaseManager(String path) {
        DatabaseManager.storageRootPath = path;
    }

    private static boolean validateDatabase(String dbName) {
        File db = new File( DatabaseManager.storageRootPath + File.separator + dbName);
        return db.exists();
    }

    public void dropDatabase(String dbName) throws IOException {
        if(!validateDatabase(dbName)) throw new DBException("Database not found");
        DatabaseManager.targetDatabase = dbName;
        String dbRootPath =  DatabaseManager.storageRootPath + File.separator + dbName;
        File database = new File(dbRootPath);
        File[] tablesInDB = database.listFiles();
        if(tablesInDB != null) {
            for (File table : tablesInDB) {
                dropTable(table.getName());
            }
        }
        if(!database.delete()) throw new IOException("Could not delete " + dbName);
    }

    public void dropTable(String table) throws IOException {
        String path = this.getTablePath(table);
        File toDelete = new File(path);
        if(!toDelete.delete()) throw new IOException("Could not delete table");
    }

    public static void setTargetDatabase(String targetDB) throws DBException {
        if(validateDatabase(targetDB)) {
            DatabaseManager.targetDatabase = targetDB;
        } else {
            throw new DBException("Database not found");
        }
    }

    public void insertToTable(String table, String[] newRow) throws IOException {
        File tableToUpdate = new File(getTablePath(table));
        if(tableToUpdate.exists()) {
            TableData thisTable;
            int newID;
            thisTable = this.loadTable(table);
            newID = thisTable.getNextTableID();
            addRowToTable(tableToUpdate, TableData.renderTabRow(newRow, false, newID));
            thisTable.updateIDCount(newID);
        }
    }

    private void clearTable(File table) throws IOException {
        FileWriter emptier;
        emptier = new FileWriter(table, false);
        emptier.write("");
        emptier.flush();
        emptier.close();
    }

    private void addRowToTable(File table, String dataRow) throws IOException {
        FileWriter rowWriter;
        rowWriter = new FileWriter(table, true);
        rowWriter.write(dataRow + "\n");
        rowWriter.flush();
        rowWriter.close();
    }

    public void createNewTable(String tableName, String[] columns) throws IOException {
        if(DatabaseManager.targetDatabase == null) throw new DBException("Table not found");
        String newPath = this.getTablePath(tableName.toLowerCase());
        File newTableFile = new File(newPath);
        if (!newTableFile.exists()) {
            boolean fileCreated = newTableFile.createNewFile();
               if(!fileCreated) throw new DBException("Unable to create new table");
               DBManifest.shared.registerTable(DatabaseManager.targetDatabase, tableName);
           if (columns != null) addRowToTable(newTableFile, TableData.renderTabRow(columns, true, 0));
        } else {
            throw new DBException("Table already exists");
        }
    }

    public void createNewDatabase(String name) throws DBException {
        if(validateDatabase(name)) { throw new DBException("Database already exists"); }
        File newDB = new File( DatabaseManager.storageRootPath + File.separator + name.toLowerCase());
        if(!newDB.mkdir()){
            throw new DBException("Unable to create database");
        }
    }

    private String getTablePath(String tableName) {
        return  DatabaseManager.storageRootPath + File.separator +
                DatabaseManager.targetDatabase + File.separator +  tableName.toLowerCase();
    }

    public void writeTableDataToDisk(TableData tableData) throws IOException {
        File tableToRewrite = new File(getTablePath(tableData.tableName));
        if(!tableToRewrite.exists()) throw new DBException("Table not found");
        clearTable(tableToRewrite);
        for(String row : tableData.convertToTabRows()){
            addRowToTable(tableToRewrite, row);
        }
    }

    public TableData loadTable(String tableName) throws IOException {
        File table = new File(getTablePath(tableName));
           if(table.exists()) {
                   FileReader reader = new FileReader(table);
                   BufferedReader bufferedReader = new BufferedReader(reader);
                   ArrayList<String> rows = new ArrayList<>();
                   while(bufferedReader.ready()) {
                       String line = bufferedReader.readLine();
                        rows.add(line);
                   }
                   bufferedReader.close();
                   return new TableData(tableName, rows, DatabaseManager.targetDatabase);
           } else {
               throw new DBException("Error loading table");
           }
    }
}
