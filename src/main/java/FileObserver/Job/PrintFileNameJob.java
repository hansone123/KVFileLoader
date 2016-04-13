/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package FileObserver.Job;

import java.io.File;

/**
 *
 * @author Hanson
 */
public class PrintFileNameJob implements Job {
    String fileName;
    public PrintFileNameJob() {
    }
    private void setName(String name) {
        this.fileName = name;
    }
    private void printFileName() {
        System.out.println(this.fileName);
    }
    private void deleteFile() {
        File file = new File(this.fileName);
        file.delete();
    }
    @Override
    public void execute(String fileName) {
        System.out.print("Print name : ");
        this.setName(fileName);
        this.printFileName();
        this.deleteFile();
    }
}
