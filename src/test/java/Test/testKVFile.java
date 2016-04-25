/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Test;

import com.dslab.kvloader.KVdata.KVFile;

/**
 *
 * @author hansone123
 */
public class testKVFile {
    public static void main(String args[]) {
        byte[] test = {0x16, 0x17, 0x18};
        KVFile kvf = new KVFile(test);
        System.out.println("KVFile length: " + kvf.getSize());
        System.out.print("KVFile content: ");
        for(int i=0; i<kvf.getSize(); i++)
            System.out.print(kvf.getData()[i] + " ");
    }
}
