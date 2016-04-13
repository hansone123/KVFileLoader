package Test;


import KVFileObserver.TableSchema;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hansone123
 */
public class testTableSchema {
    public static void main(String args[]) {
        TableSchema schema = new TableSchema();
        String filename = "schema/2";
        if (schema.readFromFile(filename)) {
            schema.show();
        }        
        filename = "schema/wrongSchema";
        if (schema.readFromFile(filename)) {
            schema.show();
        } 
    }
}
