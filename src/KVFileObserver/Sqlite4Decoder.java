/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import static KVFileObserver.Sqlite4ColumnType.*;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hansone123
 */
public class Sqlite4Decoder {
    
    
    public double fromBytestoReal  (byte[] input) {
        
        Varint firstVarint = new Varint(input);
        Varint secondVarint = new Varint(Arrays.copyOfRange(input,firstVarint.getSize() , input.length));
        int e = firstVarint.getValue();
        int m = secondVarint.getValue();   
        int sign = 0;
        if( (e & 0x02) > 0 ) {
            e = (e >> 2);
            e = -1 * e;
        }
        if( (e & 0x01) > 0 ) {
            e = (e >> 2);
            sign = -1;
        }
        double result = 0.0;
        result = sign*m*Math.pow(10, e);
//        Double.
        return result;
    }
    
    public long fromBytestoInteger64(byte[] input) {
        if (!(input == null) || input.length==0)
            return 0;
        long result = input[0];
        for (int iByte=1; iByte<input.length; iByte++) {
            result = result*256 + input[iByte];
        }
        return result;
    }
    public String charsetProcess(byte[] inputStr,Sqlite4ColumnType type) {
        try {
            switch(type) {
                case UTF8:
                    return new String(inputStr, "UTF-8");
                case UTF16LE:
                    return new String(inputStr, "UTF-16LE");
                case UTF16BE:
                    return new String(inputStr, "UTF-16BE");
            }
              
        } catch (UnsupportedEncodingException ex) {
            return "";
        }
        return "";
    }
    public String fromColToString(Sqlite4Col col)  {
        
        switch(col.getType()) {
            case UTF8:
            case UTF16LE:
            case UTF16BE:
                return this.charsetProcess(col.getValue(), col.getType());
            case BLOB:
                String output = "";
                for (byte b:col.getValue())
                    output += Byte.toString(b);
                return output;
            case REAL:
                return Double.toString(this.fromBytestoReal(col.getValue()));
            case INT:
                return Long.toString(this.fromBytestoInteger64(col.getValue()));                
            case ZERO:
                return "ZERO";
            case ONE:
                return "ONE";
            case NULL:
                return "NULL";
            default:
                return "";
        }
    }
    public static void main(String args[]) {
        int m = 10;
        double b = 0.01;
        byte[] a = {(byte)0x1f, (byte)0x00};
        int num = a[0]*256 +a[1];
        System.out.println(num);
        
           
    }
}
