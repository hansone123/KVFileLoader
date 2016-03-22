/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class KVFile {
    private byte[] data;
    private String name;
    
    public KVFile() {
    }
    public KVFile(String name, byte[] KVFileData) {
        this.name = name;     
        this.data = KVFileData.clone();
    }
    public KVFile(String name) {
        this.name = name;     
    }
    public void readAndRenderKVFile() {
        BufferedInputStream file;
        try{   
            file = new BufferedInputStream(new FileInputStream(this.name));
            byte[] buf = new byte[file.available()];
            file.read(buf, 0, file.available());
            file.close();
            this.setData(buf);   
        } catch(Exception e) {
            System.out.println("KVFile read failed.");
            return;
        }
        
        System.out.print("Open file: " + this.getName());
        System.out.println("   size: " + this.getSize());
    }
    private void setData(byte[] kvFileData) {
        this.data = kvFileData.clone();
    }
    
    public byte[] getData() {
        return this.data;
    }
    public int getSize() {
        return this.data.length;
    }
    public String getName() {
        return this.name;
    }
    private Vector<KVpair> toKVPairs() {
        
        Vector kvpairs = new Vector();
        int ofst = 0;
        int fileSize = this.getSize();
        while (ofst < fileSize) {
//            System.out.println("ofst:" + ofst);
            KVpair kvpair = new KVpair();
            //read key
            Varint keyHeader = new Varint(Arrays.copyOfRange(this.data, ofst, ofst + 4));
            ofst += keyHeader.getSize();
            kvpair.setKey(Arrays.copyOfRange(this.data, ofst, ofst + keyHeader.getValue()));
            ofst += kvpair.getKeySize();        
            //read value
            Varint valueHeader = new Varint(Arrays.copyOfRange(this.data, ofst, ofst + 4));
//            valueHeader.show();
            ofst += valueHeader.getSize();
            kvpair.setValue(Arrays.copyOfRange(this.data, ofst, ofst + valueHeader.getValue()));
            ofst += kvpair.getValueSize();
            
            //add pair in kvpairs
            if (kvpair.isValid())
                kvpairs.addElement(kvpair);
        }
//        for ()
        return kvpairs;
    }
    public Vector<Sqlite4Record> toSqlite4Records() {        
//        System.out.println("Get " + kvpairs.size() + " KVpairs in file.");
        Vector<Sqlite4Record> records = new Vector<Sqlite4Record>();
        for (KVpair pair:this.toKVPairs()) {
            records.addElement(pair.toSqlite4Record());
        }
        return records;
    }
    public static void main(String args[]) {
        byte[] test = {0x16, 0x17, 0x18};
        KVFile kvf = new KVFile("test", test);
        System.out.println("KVFile length: " + kvf.getSize());
        System.out.print("KVFile content: ");
        for(int i=0; i<kvf.getSize(); i++)
            System.out.print(kvf.getData()[i] + " ");
    }
}
