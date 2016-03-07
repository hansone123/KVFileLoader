/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import static KVFileObserver.Sqlite4ColumnType.*;
import java.util.Arrays;
import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class KVpair {
    private byte[] key;
    private byte[] value;
    public KVpair(){}
    public void setKey(byte[] key) {
        this.key = key.clone();
                
    }
    public void setValue(byte[] value) {
        this.value = value.clone();
    }
    public int getKeySize() {
        return this.key.length;
    }
    public int getValueSize() {
        return this.value.length;
    }
    public byte[] getKeyBytes() {
        return this.key;
    }
    public byte[] getValueBytes() {
        return this.value;
    }
    public boolean isValid() {
        if (key == null || value == null)
            return false;
        return true;
    }
    public Sqlite4Record toSqlite4Record () {
        
        Sqlite4Record result = new Sqlite4Record();
        result.setTableID(this.decodeTableID());
        //Get Schema 
//        TableSchema schema = new TableSchema();
//        schema.readFromFile();
        //set Table Name
//        result.setTableName(schema.getTableName());
        //set colnames
        
        //decode the header
        Vector<HeaderOfKValue> hdrs = this.getHdrsOfColumns(this.value);
        
        //decode the value
        int i=0;
        for (HeaderOfKValue hdr:hdrs) {
//            hdr.show();
            Sqlite4Col col = new Sqlite4Col();
            col.SetColumnTypeAndValue(hdr, this.value);
            result.addColumn(col);
        }
        
        return result;
    }
    private Vector<HeaderOfKValue> getHdrsOfColumns(byte[] KVpairValueArray) {
        Varint firstHdr = new Varint(KVpairValueArray);
        int headerStart = firstHdr.getSize();  
        int headerEnd = headerStart + firstHdr.getValue() -1;
        int dataOfst = firstHdr.getSize() + firstHdr.getValue();
        int hdrOfst = headerStart;
        Vector<HeaderOfKValue> hdrs = new Vector<HeaderOfKValue>();
        while( hdrOfst <= headerEnd ) {
            Varint hdr = new Varint(Arrays.copyOfRange(KVpairValueArray, hdrOfst, hdrOfst + 4));
            HeaderOfKValue kvhdr = new HeaderOfKValue();
            kvhdr.setTypeAndSize(hdr.getValue());
            kvhdr.setOfstOfValue(dataOfst);
                    
            dataOfst += kvhdr.sizeOfValue;
            hdrOfst += hdr.getSize();
            hdrs.addElement(kvhdr);
        }
        return hdrs;
    }
    private int decodeTableID() {
        Varint tableID = new Varint();
        tableID.set(this.key);
        
        return tableID.getValue();
    }
    
    
}
