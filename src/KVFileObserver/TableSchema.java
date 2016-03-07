/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class TableSchema {
    private String tableName;
    private int tableId;
    private Vector<String> colnames;
    
    public void readFromFile(String name) {
//        setTableName();
//        setTableID();
//        setColNames();
    }
    public void setTableName(String name) {
        this.tableName = name;
    }
    public void setTableID(int id) {
        this.tableId = id;
    }
    public void setColNames(String [] names) {
        for (String name:names)
            this.colnames.addElement(name);
    }
    public String getTableName() {
        return this.tableName;
    }
    public int getTableID() {
        return this.tableId;
    }
    public Vector<String> getColNames() {
        return this.colnames;
    }
    
    public static void main(String args[]) {
        TableSchema schema = new TableSchema();
        String filename = "";
        schema.readFromFile(filename);
    }
}
