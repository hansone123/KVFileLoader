/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import FileObserver.FileObserver;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class KVFileObserver extends FileObserver{
    
    public KVFile readAndRenderKVFile(String fileName) throws FileNotFoundException, IOException {
           
        BufferedInputStream file = new BufferedInputStream(new FileInputStream(fileName));
        byte[] buf = new byte[file.available()];
        file.read(buf, 0, file.available());
        KVFile kvfile = new KVFile(fileName, buf);   
        
        System.out.println("Open file: " + kvfile.getName());
        System.out.println("file size: " + kvfile.getSize());
        
        return kvfile;
    }
    public int pushToPhoenix(KVFile kvfile) {
        
        for (Sqlite4Record record:kvfile.toSqlite4Records()) {
            this.sendAQuery(record);
            
        }
        int SuccessedNumberOfRecord = 0;
        return SuccessedNumberOfRecord;
    }
    public boolean sendAQuery(Sqlite4Record record) {
        System.out.println("table id: " + record.getTableID());
        for (Sqlite4Col col : record.getColumns()) {
            col.show();
            Sqlite4Decoder decoder = new Sqlite4Decoder();
            System.out.println("  value: " + decoder.fromColToString(col));
        }
        return true;
    }
    @Override
    public void doJob(String Filename ) {
        try{
                KVFile kvfile = new KVFile(Filename);
                kvfile.readAndRenderKVFile(Filename);                
                this.pushToPhoenix(kvfile);
                
            } catch(IOException e) {
                System.out.println("Do Job failed!");
            }
        System.out.println("Do Job successed!");
    }
    
    
    
    
}
