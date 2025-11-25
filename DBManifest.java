package edu.uob;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;


// Responsible for keeping track of tables and their id counts in a quick-and-easy way

public class DBManifest {

    public static DBManifest shared = new DBManifest();
    private static String manifestPath = Paths.get("databases").toAbsolutePath() + File.separator + "manifest.txt";
    public  HashMap<String, Integer> tables = new HashMap<String, Integer>();
    private File manifestFile;

    public void updateTableIDCount(String database, String tableName, int count) {
        this.tables.remove(database + File.separator + tableName);
        this.tables.put(database + File.separator + tableName, count);
        saveTablesToManifest();
    }

    public void saveTablesToManifest() {
        FileWriter manifestWriter;
        try {
            manifestWriter = new FileWriter(manifestFile, false);
            for (String key : this.tables.keySet()) {
                manifestWriter.write(key + " " + tables.get(key) + "\n");
            }
            manifestWriter.flush();
            manifestWriter.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void registerTable(String database, String table) {
        FileWriter manifestWriter;
        String newEntry = database + File.separator + table + " " + "0";
        try {
            manifestWriter = new FileWriter(manifestFile, true);
            manifestWriter.write( newEntry+ "\n");
            manifestWriter.flush();
            manifestWriter.close();
        }catch(IOException error){
            System.out.println(error.getMessage());
        }
        loadManifest();
    }

    public int getTableIDCount(String database, String table) {
        return tables.get(database + File.separator + table);
    }

    private void loadManifest() {
        try {
            FileReader reader = new FileReader(manifestFile);
            BufferedReader bufferedReader = new BufferedReader(reader);
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                String[] parts = line.split(" ");
                tables.put(parts[0], Integer.parseInt(parts[1]));
            }
            bufferedReader.close();
        } catch (Exception error) {
            System.out.println(error.getMessage());
        }
    }

    public void provisionManifest(){
         manifestFile = new File(manifestPath);
        if(!manifestFile.exists()){
            try {
                manifestFile.createNewFile();
            }catch (Exception e){
                System.out.println("Error creating manifest file");
            }
        }
        loadManifest();
    }
}
