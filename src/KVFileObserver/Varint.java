package KVFileObserver;


import java.math.BigInteger;
import java.util.Map;
import java.util.Vector;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Hanson
 */
public class Varint {
    public int value;
    private int size;
    public Varint(){
        this.size = 0;
        this.value = 0;
    }
    public Varint(byte[] buf) {
        this.size = 0;
        this.value = 0;
        this.set(buf);
    }
    public void set(byte[] buf) {
        if (buf.length<1)
            return;
        int firstByte = Byte.toUnsignedInt(buf[0]);
        
        if( firstByte <= 240 && buf.length >=1 ){
          this.value = Byte.toUnsignedInt(buf[0]);
          this.size = 1;
        }
        else if( firstByte <= 248 && buf.length >= 2){
          this.value = (Byte.toUnsignedInt(buf[0])-241)*256 + Byte.toUnsignedInt(buf[1]) + 240;
          this.size = 2;
        }
        else if( firstByte == 249 && buf.length >= 3){
          this.value = 2288 + 256*Byte.toUnsignedInt(buf[1]) + Byte.toUnsignedInt(buf[2]);
          this.size = 3;
        }
        else if( firstByte == 250 && buf.length >= 4){
          this.value = (Byte.toUnsignedInt(buf[1])<<16) + (Byte.toUnsignedInt(buf[2])<<8) + Byte.toUnsignedInt(buf[3]);
          this.size = 4;
        }
        else{
            this.size = 0;
            this.value = 0;
        }
    }
    public int getValue(){
        return this.value;
    }
    public int getSize(){
        return this.size;
    }
    public void show(){
        
        System.out.println("Varint value:" + this.getValue());
        System.out.println("Varint size:" + this.getSize());
    }
    public static void main(String[] args) {
//        //*test long range :**/
//        long num;
//        long num2;
//        num = Long.MAX_VALUE;
//        System.out.println("MAX:" + num);
//        num2 = Long.MIN_VALUE;
//        System.out.println("MIN:" + num2);
        
        byte[] num1 = {(byte)248, (byte)255};
        byte[] num2 = {(byte)249, (byte)255, (byte)255};
        byte[] num3 = {(byte)250, (byte)255, (byte)255, (byte)255};
        Varint varint = new Varint();
        varint.set(num1);
        varint.show();
        varint.set(num2);
        varint.show();
        varint.set(num3);
        varint.show();
        
    }
}


