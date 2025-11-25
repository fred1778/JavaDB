package edu.uob;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TableData {

    public String tableName;
    public ArrayList<String> headers = new ArrayList<String>();
    public ArrayList<HashMap<String, String>> rowData = new ArrayList<>();
    private String parentDB;
    private int idCount;

    public int getNextTableID() {
        return this.idCount += 1;
    }

    public TableData(String name, ArrayList<String> rows, String database) throws IOException {
        this.tableName = name;
        this.getHeaders(rows.get(0));
        this.buildData(rows);
        this.parentDB = database;
        this.idCount = getLatestID(this.parentDB);
    }

    private ArrayList<String> getLowercaseColumns(){
        ArrayList<String> lowerCase = new ArrayList<>();
        for(String column : this.headers){
            lowerCase.add(column.toLowerCase());
        }
        return lowerCase;
    }

    private String getDisplayHeader(String header){
       for(String column : this.headers){
           if(column.equalsIgnoreCase(header)) return column;
       }
       return header;
    }

    public boolean verifyColumns(String[] cols, String column) {
        if (cols != null) {
            for (String col : cols) {
                if (!this.getLowercaseColumns().contains(col.toLowerCase())) return false;
            }
            return true;
        } else {
            return this.getLowercaseColumns().contains(column.toLowerCase());
        }
    }

    public void processAlteration(boolean add, String attribute) throws Exception {
        if (!add && !headers.contains(attribute)) throw new Exception();
        if (add && headers.contains(attribute)) throw new Exception();
        if (add) headers.add(attribute);
        else headers.remove(attribute);
        for (HashMap<String, String> row : rowData) {
            if (add) {
                row.remove(attribute);
            } else {
                row.put(attribute.toLowerCase(), "");
            }
        }
    }

    public String getDataForConditions(QueryConditionGroup queryData) {
        StringBuilder output = new StringBuilder("\n");
        ArrayList<String> toInclude = new ArrayList<>();
        if (queryData.queryAttributes.length > 0) {
            for(String col : queryData.queryAttributes){
                toInclude.add(this.getDisplayHeader(col));
            }
        } else {
            toInclude = headers;
        }
        for (String col : toInclude) {
            output.append(col);
            output.append("\t");
        }
        output.append("\n -------------------------------------------\n");

        for (HashMap<String, String> row : rowData) {
            if (QueryCondition.evaluateConditions(queryData, row)) {
                for (String column : toInclude) {
                    output.append(row.get(column));
                    output.append("\t");
                }
                output.append("\n");
            }
        }
        return output.toString();
    }

    public void updateIDCount(int latestID) {
        DBManifest.shared.updateTableIDCount(this.parentDB, this.tableName, latestID);
    }

    private int getLatestID(String db) {
        return DBManifest.shared.getTableIDCount(db, this.tableName);
    }

    public static String renderTabRow(String[] fields, boolean isHeader, int count) {
        StringBuilder row = new StringBuilder();
        if (isHeader) row.append("id");
        else row.append(count);
        for (String field : fields) {
            row.append("\t");
            row.append(field);
        }
        return row.toString();
    }

    public void deleteDataFromTable(QueryConditionGroup queryData) {
         rowData.removeIf(row -> QueryCondition.evaluateConditions(queryData,row));
    }

    public void updateValues(String[] attributes, String[] values, QueryConditionGroup conditions) {
        int indexCounter = 0;
        for (String attribute : attributes) {
            for (HashMap<String, String> row : rowData) {
                if (QueryCondition.evaluateConditions(conditions, row)) {
                    row.put(attribute, values[indexCounter]);
                }
            }
            indexCounter++;
        }
    }

    public ArrayList<String> convertToTabRows() {
        ArrayList<String> tableWritableData = new ArrayList<>();
        StringBuilder headerRow = new StringBuilder();
        for (String header : headers) {
            headerRow.append(header);
            headerRow.append("\t");
        }
        tableWritableData.add(headerRow.toString());

        for (HashMap<String, String> row : rowData) {
            StringBuilder rowBuilder = new StringBuilder();
            for (String column : headers) {
                rowBuilder.append(row.get(column));
                rowBuilder.append("\t");
            }
            tableWritableData.add(rowBuilder.toString());
        }
        return tableWritableData;
    }

    private void getHeaders(String headerLine) {
        this.headers.addAll(List.of(headerLine.split("\t")));
    }

    private void buildData(ArrayList<String> rows) throws IOException {
        for (int i = 1; i < rows.size(); i++) {
            this.rowData.add(convertRowStringToData(rows.get(i)));
        }
    }

    private HashMap<String, String> convertRowStringToData(String row) throws DBException {
        String[] fields = row.split("\t");
        if (fields.length != headers.size()) {
            throw new DBException("Invalid number of fields in row");
        }
        HashMap<String, String> rowData = new HashMap<>();
        for (int i = 0; i < this.headers.size(); i++) {
            rowData.put(this.headers.get(i).toLowerCase(), fields[i]);
        }
        return rowData;
    }

    private String buildJoinTableHeader(TableData secondTable, String[] joinAttributes) {
        StringBuilder joinHeader = new StringBuilder();
        joinHeader.append("id");
        joinHeader.append("\t");
        for (String header : this.headers) {
            if (!joinAttributes[0].equals(header) && !header.equals("id")) {
                joinHeader.append(tableName);
                joinHeader.append(".");
                joinHeader.append(header);
                joinHeader.append("\t");
            }
        }
        for (String header : secondTable.headers) {
            if (!joinAttributes[1].equals(header) && !header.equals("id")) {
                joinHeader.append(secondTable.tableName);
                joinHeader.append(".");
                joinHeader.append(header);
                joinHeader.append("\t");
            }
        }
        return joinHeader.toString();
    }

    private ArrayList<String> findJoiningRow(HashMap<String, String> row, String[] attributes, TableData secondTable, int sequence) {
        ArrayList<String> newRows = new ArrayList<>();
        int counter = 0;
        String testAttribute = row.get(attributes[0]);
        for (HashMap<String, String> secondTableRow : secondTable.rowData) {
            StringBuilder joinRow = new StringBuilder();
            if (secondTableRow.get(attributes[1]).equals(testAttribute)) {
                joinRow.append(sequence + counter);
                joinRow.append("\t");
                for (String t1Header : this.headers) {
                    if (!t1Header.equals(attributes[0]) && !t1Header.equals("id")) joinRow.append(row.get(t1Header));
                    joinRow.append("\t");
                }
                for (String t2Header : secondTable.headers) {
                    if (!t2Header.equals(attributes[1]) && !t2Header.equals("id"))
                        joinRow.append(secondTableRow.get(t2Header));
                    joinRow.append("\t");
                }
                counter++;
                joinRow.append("\n");
                newRows.add(joinRow.toString());
            }
        }
        if (!newRows.isEmpty()) return newRows;
        return null;
    }

    public String joinTable(TableData tableB, String[] attributes) throws DBException {
        if (!(this.verifyColumns(null, attributes[0]) && tableB.verifyColumns(null, attributes[1])))
            throw new DBException("Invalid columns for join operation");
        StringBuilder output = new StringBuilder();
        output.append("\n");
        output.append(buildJoinTableHeader(tableB, attributes));
        output.append("\n");
        int id_count = 1;
        for (HashMap<String, String> row : rowData) {
            ArrayList<String> newRows = this.findJoiningRow(row, attributes, tableB, id_count);
            if (newRows != null) {
                for(String newRow : newRows) {
                    output.append(newRow);
                    id_count++;
                }
            }
        }
        return output.toString();
    }
}