/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package FileObserver.Job;

import FileObserver.FileObserver;
import FileObserver.Job.Job;
import KVFileObserver.KVFile;
import KVFileObserver.Sqlite4Col;
import KVFileObserver.Sqlite4Decoder;
import KVFileObserver.Sqlite4Record;
import KVFileObserver.TableSchema;
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
public class KVFileLoaderJob implements Job{
    
    private String fileName;
    private Vector<TableSchema> schemas;
    public KVFileLoaderJob() {
        this.schemas = new Vector();
    }
    public KVFile readAndRenderKVFile(String fileName) throws FileNotFoundException, IOException {
           
        BufferedInputStream file = new BufferedInputStream(new FileInputStream(fileName));
        byte[] buf = new byte[file.available()];
        file.read(buf, 0, file.available());
        KVFile kvfile = new KVFile(fileName, buf);   
        
        System.out.println("Open file: " + kvfile.getName());
        System.out.println("file size: " + kvfile.getSize());
        
        return kvfile;
    }
    public void loadTableSchema(String dirPath) {
        
        File directory = new File(dirPath);
        for (String file:directory.list()) {
            TableSchema schema = new TableSchema();
            if (schema.readFromFile(dirPath + "//" + file)) {
                schema.show();
                this.schemas.add(schema);
            }
        }
        
    }
    public boolean pushToPhoenix(KVFile kvfile) {
        
        int SuccessedNumberOfRecord = 0;
        int totalRecord = 0;
        Vector<Sqlite4Record> records = kvfile.toSqlite4Records();
        if (records.size() == 0) {
            return false;
        }
        
        for (Sqlite4Record record:records) {
            totalRecord++;
            if ( this.sendAQuery(record) ) {
                SuccessedNumberOfRecord++;
            }
        }
        
        return (SuccessedNumberOfRecord == totalRecord);
    }
    
    public boolean sendAQuery(Sqlite4Record record) {
        
        
        for (Sqlite4Col col : record.getColumns()) {
            col.show();
            Sqlite4Decoder decoder = new Sqlite4Decoder();
            System.out.println("  value: " + decoder.fromColToString(col));
        }
        return true;
    }
    private void deleteFile() {
        File file = new File(this.fileName);
        file.delete();
    }
    @Override
    public void execute(String fileName) {
        this.fileName = fileName;
        KVFile kvfile = new KVFile(this.fileName);        
        kvfile.readAndRenderKVFile();
        
        if (!this.pushToPhoenix(kvfile)) {
            System.out.println("pushToPhoenix failed."); 
            this.deleteFile();
            return;
        }
        this.deleteFile();
    }
    
}
