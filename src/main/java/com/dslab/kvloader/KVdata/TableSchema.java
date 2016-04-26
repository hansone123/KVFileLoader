/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvloader.KVdata;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.JDBCType;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author hansone123
 */
public class TableSchema {
    private String tableName;
    private int tableId;
    private int columnNum;
    private ArrayList<String> colnames;
    private ArrayList<JDBCType> coltypes;
    
    public TableSchema() {
        this.columnNum = 0;
        this.colnames = new ArrayList();
        this.coltypes = new ArrayList();
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
                JDBCType ColumnType = this.toJDBCType(st.nextToken());
                if (ColumnType == JDBCType.OTHER) {
                    System.out.println("Schema is not fitting the JDBC format.");
                    return false;
                }
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
    private void addColumn(String colName, JDBCType colType) {
        this.columnNum += 1;
        this.colnames.add(colName);
        this.coltypes.add(colType);
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
    public List<String> getColsName() {
        return this.colnames;
    }
    public List<JDBCType> getColsType() {
        return this.coltypes;
    }
    
    private JDBCType toJDBCType(String type) {
        
        switch(type) {
             case "int":
             case "INT":
                return JDBCType.INTEGER;
            case "float":
            case "FLOAT":
                return JDBCType.FLOAT;
            case "DOUBLE":
            case "double":
                return JDBCType.DOUBLE;
            case "VARCHAR":
            case "varchar":
                return JDBCType.VARCHAR;
            case "BLOB":
            case "blob":
                return JDBCType.BLOB;
            default:
                return JDBCType.OTHER;
        }
    }
    public void show() {
        System.out.println("-------------------------------------");
        System.out.println("ID: " + this.getTableID());
        System.out.println("name: " + this.getTableName());        
        System.out.println("Col number:" + this.getColumnNum());
        for(String name:this.getColsName()) {
            System.out.print(name +"\t");
        }
        System.out.println();
        for(JDBCType type:this.getColsType()) {
            System.out.print(type.toString() +"\t");
        }
        System.out.println();
        System.out.println("-------------------------------------");
        
    }
    
}
