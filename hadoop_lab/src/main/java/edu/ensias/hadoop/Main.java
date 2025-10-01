package edu.ensias.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author alami
 */
public class Main {

    public static void getHdfsFileStatus(FileSystem fs, String[] args) throws IOException {
        // Placeholder for HDFS file status logic
        Path path = new Path(args[1], args[2]);
            if (!fs.exists(path)) {
                System.out.println("File does not exist");
                System.exit(1);
            } 
            FileStatus status = fs.getFileStatus(path);
            System.out.println("File name: " + path.getName());    
            System.out.println("File size: " + status.getLen());    
            System.out.println("File owner: " + status.getOwner());
            System.out.println("File group: " + status.getGroup());
            System.out.println("File permission: " + status.getPermission());
            System.out.println("File modification time: " + status.getModificationTime());
            System.out.println("File access time: " + status.getAccessTime());
            fs.close();
            
    }
    
    public static void readHDFSFile(FileSystem fs, String[] args) throws IOException {
        // Placeholder for HDFS file reading logic
        Path path = new Path(args[1], args[2]);
            if (!fs.exists(path)) {
                System.out.println("File does not exist");
                System.exit(1);
            } 
            FSDataInputStream inputStream = fs.open(path);
            InputStreamReader isr = new InputStreamReader(inputStream);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            br.close();
            isr.close();    
            inputStream.close();
            fs.close();
    }
    public static void getHdfsFileInfo(FileSystem fs, String[] args) throws IOException {
        // Placeholder for HDFS file info logic
        Path path = new Path(args[1], args[2]);
            if (!fs.exists(path)) {
                System.out.println("File does not exist");
                System.exit(1);
            } 
            FileStatus status = fs.getFileStatus(path);
            System.out.println("File name: " + path.getName());    
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for(BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            fs.close();
    }
    public static void WriteHDFSFile(FileSystem fs, String[] args) throws IOException {
        // Placeholder for HDFS file writing logic
        Path path = new Path(args[1], args[2]);
            if (fs.exists(path)) {
                System.out.println("File already exists");
                System.exit(1);
            } 
            //FSDataInputStream inputStream = fs.open(new Path(args[3]));
            FSDataOutputStream outputStream = fs.create(path);
            outputStream.writeUTF("Hello world ");
            outputStream.close();
            fs.close();
    }
    public static void main(String[] args) throws IOException {
        
        System.out.println("Hello World!");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs =FileSystem.get(conf);
        String s="info";
        switch (s) { //args[0]
            case "status":
                System.out.println("Fetching HDFS file status...");
                getHdfsFileStatus(fs, args);
                break;
            case "info":
                System.out.println("Fetching HDFS file info...");
                
                getHdfsFileInfo(fs, new String[] {"info","/user/root/input","alice.txt"});
                break;
            case "read":
                System.out.println("Reading HDFS file...");
                readHDFSFile(fs, args);

                break;
            case "write":
                WriteHDFSFile(fs, args);
                break;
            default:
                System.out.println("Unknown command! Available commands: status, read, write");
        }
        
        
    }
}
    