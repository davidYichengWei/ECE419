package testing;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
public class PerformanceTest {
    private KVStore kvClient;
    Map<String, String> test_pairs;
    private static Logger logger = Logger.getRootLogger();
    public PerformanceTest() {
        
    }
    // public void performancePutShort(int numGet, int numPut){
    //     long  startTime = System.currentTimeMillis(); 
    //     try {
    //         for(int i=0;i<numPut;i++){
    //             server.putKV(Integer.toString(i), Integer.toString(i));
    //         }
            
    //     } catch (Exception e) {
            
    //     }
        

    //     long endTime = System.currentTimeMillis();

    //     System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    // }
    // public void performancePutMedium(int numGet, int numPut){
    //     long  startTime = System.currentTimeMillis(); 
    //     try {
    //         for(int i=0;i<numPut;i++){
    //             server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO");
    //         }
            
    //     } catch (Exception e) {
            
    //     }
        

    //     long endTime = System.currentTimeMillis();

    //     System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    // }
    // public void performancePutLarge(int numGet, int numPut){
    //     long  startTime = System.currentTimeMillis(); 
    //     try {
    //         for(int i=0;i<numPut;i++){
    //             server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLO");
    //         }
            
    //     } catch (Exception e) {
            
    //     }
        

    //     long endTime = System.currentTimeMillis();

    //     System.out.println("Number of large puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    // }
    // public void performance80Put20Get(int numGet, int numPut){
    //     long  startTime = System.currentTimeMillis(); 
    //     try {
    //         for(int i=0;i<numPut;i++){
    //             // server.putKV(Integer.toString(i), Integer.toString(i));

    //             // server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO");

    //             server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLO");
    //         }

    //         for(int i=0;i<numGet;i++){
    //             server.getKV(Integer.toString(i));
    //         }
    //     } catch (Exception e) {
            
    //     }
        

    //     long endTime = System.currentTimeMillis();

    //     System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    // }
    // public void performance20Put80Get(int numGet, int numPut){
    //     long  startTime = System.currentTimeMillis(); 
    //     try {
    //         for(int i=0;i<numPut;i++){
    //             // server.putKV(Integer.toString(i), Integer.toString(i));

    //             // server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO");

    //             server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
    //             "HELLOHELLOHELLOHELLOHELLOHELLOHELLO");
    //         }

    //         for(int i=0;i<8;i++){
    //             for(int j=0;j<100;j++){
    //                 server.getKV(Integer.toString(j));
    //             }
                
    //         }
    //     } catch (Exception e) {
            
    //     }
        

    //     long endTime = System.currentTimeMillis();

    //     System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    // }
    public void scan_folder(String path){
        File f = new File("/Users/beale/Desktop/maildir/allen-p/sent"); 
        Map<String, String> pairs = new HashMap<String, String>();
        File fileList[] = f.listFiles();
        try{
            for (File file : fileList) {
                System.out.println(file);
                String one_file = "";          
                InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
                BufferedReader br = new BufferedReader(reader);
                String stringLine;
                while ((stringLine=br.readLine())!=null) {
                    // System.out.println(stringLine);
                    one_file+=stringLine;
                }
                pairs.put(file.getName(), one_file);
                
                // System.out.println(one_file);
            }
            this.test_pairs = pairs;
            
        }

        catch(Exception e){
        }
    }
    public void run_tests(){
        try {
            
            
            
            // Start 2 new servers

            KVServer server2 = new KVServer("localhost", 50001, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server3 = new KVServer("localhost", 50002, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server4 = new KVServer("localhost", 50003, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server5 = new KVServer("localhost", 50004, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server6 = new KVServer("localhost", 50005, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server7 = new KVServer("localhost", 50006, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server8 = new KVServer("localhost", 50007, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server9 = new KVServer("localhost", 50008, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server10 = new KVServer("localhost", 50009, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server11 = new KVServer("localhost", 50010, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server12 = new KVServer("localhost", 50011, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server13 = new KVServer("localhost", 50012, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server14 = new KVServer("localhost", 50013, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server15 = new KVServer("localhost", 50014, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server16 = new KVServer("localhost", 50015, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server17 = new KVServer("localhost", 50016, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server18 = new KVServer("localhost", 50017, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server19 = new KVServer("localhost", 50018, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server20 = new KVServer("localhost", 50019, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            KVServer server21 = new KVServer("localhost", 50020, "localhost", 10, "FIFO", "db_files");
            Thread.sleep(1000);
            // Wait for 5 seconds to allow the server to start
            
            
            long  startTime = System.currentTimeMillis();
            kvClient = new KVStore("localhost", 50001);
            kvClient.connect();
            int num=0;
            for(String i:this.test_pairs.keySet()){
                // System.out.println(i);
                kvClient.put(i, test_pairs.get(i));
                num++;
                // if(num==100){
                //     break;
                // }
            }
            // for(int i=0;i<20;i++){
            //     kvClient.get("12.");
            // }
            long  endTime = System.currentTimeMillis(); 
            kvClient.disconnect();
            long res = endTime-startTime;
            System.out.println("___________________time to put requests:________________________________-4000"+ res);
            server2.clearCache();
            server2.clearStorage();
            server3.clearCache();
            server3.clearStorage();
            server4.clearCache();
            server4.clearStorage();
            server5.clearCache();
            server5.clearStorage();
            server6.clearCache();
            server6.clearStorage();
            server7.clearCache();
            server7.clearStorage();
            server8.clearCache();
            server8.clearStorage();
            server9.clearCache();
            server9.clearStorage();
            server10.clearCache();
            server10.clearStorage();
            server11.clearCache();
            server11.clearStorage();
            server12.clearCache();
            server12.clearStorage();
            server13.clearCache();
            server13.clearStorage();
            server14.clearCache();
            server14.clearStorage();
            server15.clearCache();
            server15.clearStorage();
            server16.clearCache();
            server16.clearStorage();
            server17.clearCache();
            server17.clearStorage();
            server18.clearCache();
            server18.clearStorage();
            server19.clearCache();
            server19.clearStorage();
            server20.clearCache();
            server20.clearStorage();
            server21.clearCache();
            server21.clearStorage();
        } catch (Exception e) {
            
        }
    }

    public static void main(String[] args) {
        // long  startTime = System.currentTimeMillis(); 
        PerformanceTest p =  new PerformanceTest();
        p.scan_folder("");
        p.run_tests();
        
        // long endTime = System.currentTimeMillis();
        // long res = endTime-startTime;
        // System.out.println("time to create 5 servers:"+ res);
        
        // p.performancePutShort(0, 1000);
    //     p.server.clearStorage();
        // p.performancePutMedium(0, 1000);
        // p.server.clearStorage();
        // p.performance20Put80Get(800, 200);

        // p.server.clearStorage();
        
    }
}
