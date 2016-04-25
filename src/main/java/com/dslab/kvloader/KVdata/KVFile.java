/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvloader.KVdata;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class KVFile {
    private byte[] data;
    
    public KVFile() {
    }
    public KVFile(byte[] KVFileData) {  
        this.data = KVFileData.clone();
    }
    public void readAndRenderKVFile(String filePath) {
        BufferedInputStream file;
        try{   
            file = new BufferedInputStream(new FileInputStream(filePath));
            byte[] buf = new byte[file.available()];
            file.read(buf, 0, file.available());
            file.close();
            this.setData(buf);   
        } catch(Exception e) {
            System.out.println("KVFile read failed.");
            return;
        }
        
        System.out.print("Open file: " + filePath);
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
    private ArrayList<KVpair> toKVPairs() {
        
        ArrayList<KVpair> kvpairs = new ArrayList();
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
                kvpairs.add(kvpair);
        }
//        for ()
        return kvpairs;
    }
    public List<Sqlite4Record> toSqlite4Records() {        
//        System.out.println("Get " + kvpairs.size() + " KVpairs in file.");
        ArrayList<Sqlite4Record> records = new ArrayList();
        for (KVpair pair:this.toKVPairs()) {
            records.add(pair.toSqlite4Record());
        }
        return records;
    }
    
}
