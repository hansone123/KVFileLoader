/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class TableSchema {
    private String tableName;
    private int tableId;
    private int columnNum;
    private Vector<String> colnames;
    private Vector<String> coltypes;
    
    public TableSchema() {
        this.columnNum = 0;
        this.colnames = new Vector<String>();
        this.coltypes = new Vector<String>();
    }
    public boolean readFromFile(String filePath) {
        try{
            BufferedReader fstream = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
            String temp;            
            if ( (temp = fstream.readLine()) == null ) {
                return false;
            }
            
            int tableID = Integer.valueOf(temp);
            this.setTableID(tableID);
            
            if ( (temp = fstream.readLine()) == null ) {
                return false;
            }
            this.setTableName(temp);
            
            while((temp = fstream.readLine()) != null ) {                
                StringTokenizer st = new StringTokenizer(temp,",");                 
                String columnName = st.nextToken();
                String ColumnType = st.nextToken();
                this.addColumn(columnName, ColumnType);
            }       
            
            if (this.getColumnNum() < 1) {
                return false;
            }
        }catch (IOException io) {
            return false;
        }catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
    private void setTableName(String name) {
        this.tableName = name;
    }
    public void setTableID(int id) {
        this.tableId = id;
    }
    private void addColumn(String colName, String colType) {
        this.columnNum += 1;
        this.colnames.addElement(colName);
        this.coltypes.addElement(colType);
    }
    public String getTableName() {
        return this.tableName;
    }
    public int getTableID() {
        return this.tableId;
    }
    public int getColumnNum() {
        return this.columnNum;
    }
    public String getColName(int index) {
        if (index<this.colnames.size())
            return this.colnames.get(index);
        return "";
    }
    public String getColType(int index) {
        if (index<this.coltypes.size())
            return this.coltypes.get(index);
        return "";
    }
    public void show() {
        System.out.println("name: " + this.getTableName());
        System.out.println("ID: " + this.getTableID());
        System.out.println("Col number:" + this.getColumnNum());
        for (int i=0; i<this.getColumnNum(); i++) {
            System.out.print(this.getColName(i) +"\t");
        }
        System.out.println();
        for (int i=0; i<this.getColumnNum(); i++) {
            System.out.print(this.getColType(i) +"\t");
        }
        System.out.println();
    }
    public static void main(String args[]) {
        TableSchema schema = new TableSchema();
        String filename = "4";
        schema.readFromFile(filename);
        schema.show();
    }
}
