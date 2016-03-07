/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package FileObserver;

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
public class FileObserver {
    
    public String directoryPath;
    public String fileExtension;
    
    public FileObserver() {
        this.directoryPath = "";
        this.fileExtension = "";
    }
    public void setFileExtension(String extension) {
        this.fileExtension = extension;
    }
    public boolean setValidDirectoryPath(String dirPath) {
        
        this.directoryPath = dirPath;    
        return this.dirIsExisted(this.directoryPath);
    }
    
    private boolean dirIsExisted(String dir) {
        
        File directory = new File(dir);
        if ((directory.exists()  && directory.isDirectory())) {
            System.out.println("FileObserver directory path set to: " + this.directoryPath);
            return true;
        }
        return false;
    }
    
    private String chooseFile() {
        String result = "";
        if (this.dirIsEmpty()) {
//            System.out.println("Directory is empty");
            return result;
        }
        
        Vector<String> files = this.getMatchedFiles();
        
        if (files.isEmpty())
            return null;
        
        result = files.firstElement();
        for (String file : files) {
            if (result.compareTo(file) > 0)
                result = file;
        }
        result = this.directoryPath + "//" + result;
        System.out.println("Choose file: " + result);
        
        return result;
    }
    private Vector<String> getMatchedFiles() {
        
        File directory = new File(this.directoryPath);
        Vector<String> matchedFiles  = new Vector<String>();
        
        for (String fileName:directory.list()) {            
            if (this.fileExtension.equals("")) {
                if (!fileName.contains(this.fileExtension)) {
                    continue;
                }
            }
            matchedFiles.addElement(fileName);
        }
        return matchedFiles;
    }
    private boolean dirIsEmpty() {
        File directory = new File(this.directoryPath);
        String[] allFiles = directory.list();
        return allFiles.length < 1;
    }
    public void doJob(String Filename ) {
        System.out.println("Do the job .");
    }
    public void keepWatchOnDirectoryAndDoJob() {
        
        while(true) {
            String fileName = this.chooseFile();
            
            if (fileName != "") {
                this.doJob(fileName);
                this.deleteFile(fileName);
            }
            
            
        }
    }
//    public String observeDirectory() {
//        
//    }
    
    public boolean deleteFile(String fileName) {
        
        File file = new File(fileName);
        if (file.delete()) {
            System.out.println(fileName + " is deleted .");
            return true;
        } 
        
        System.out.println(fileName + "deletion failed .");
        return false;
    }
    
            
    
    
    /**
     * @param args the command line arguments
     * args[0]: is the file extension we watch
     */
    public static void main(String[] args) throws IOException {
        
        FileObserver fileObserver = new FileObserver();
        if( !fileObserver.setValidDirectoryPath("/tmp/KVoutput") )  {            
            return;
        }
        
        if (args.length >0) {
            String fileExtesion = args[0];
            fileObserver.setFileExtension(fileExtesion);            
        }
        
        fileObserver.setFileExtension("");
        fileObserver.keepWatchOnDirectoryAndDoJob();
        
    }
    
}
