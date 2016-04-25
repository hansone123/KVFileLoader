/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvloader.KVdata;

import static com.dslab.kvloader.KVdata.Sqlite4ColumnType.*;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 *
 * @author hansone123
 */
public class Sqlite4Decoder {
    
    
    public static double fromBytestoReal  (byte[] input) {
        
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
    
    public static long fromBytestoInteger64(byte[] input) {
        if ((input == null) || input.length==0)
            return 0;
        long result = input[0];
        for (int iByte=1; iByte<input.length; iByte++) {
            result = result*256 + input[iByte];
        }
        
        return result;
    }
    public static String charsetProcess(byte[] inputStr,Sqlite4ColumnType type) {
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
    
    public static void main(String args[]) {
        int m = 10;
        double b = 0.01;
        byte[] a = {(byte)0x1f, (byte)0x00};
        int num = a[0]*256 +a[1];
        System.out.println(num);
        
           
    }
}
