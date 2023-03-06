package testing;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
public class PerformanceTest {
    private KVServer[] server_list;

    public PerformanceTest() {
        try {
            long  startTime = System.currentTimeMillis(); 
        
            int i=0;
            // this.server = new KVServer("127.0.0.1", 8080, 2000, "LRU", "db_files");
            this.server_list = new KVServer[10];
            for(i=0;i<5;i++){
                this.server_list[i] = new KVServer("127.0.0.1", 10200+i, 2000, "LRU", "db_files");
                // TimeUnit.SECONDS.sleep(1);
            }
            long endTime = System.currentTimeMillis();
        long res = endTime-startTime;
        System.out.println("time to create" +i+" servers:________________________________"+ res);
        } catch (Exception e) {
            
        }
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
        }

        catch(Exception e){
        }
    }
    public void run_tests(){
        long  startTime = System.currentTimeMillis(); 
        
        long endTime = System.currentTimeMillis();
    }
    public static void main(String[] args) {
        long  startTime = System.currentTimeMillis(); 
        PerformanceTest p =  new PerformanceTest();
        p=null;
        long endTime = System.currentTimeMillis();
        long res = endTime-startTime;
        System.out.println("time to create 5 servers:"+ res);
        
        // p.performancePutShort(0, 1000);
    //     p.server.clearStorage();
        // p.performancePutMedium(0, 1000);
        // p.server.clearStorage();
        // p.performance20Put80Get(800, 200);

        // p.server.clearStorage();
        // p.scan_folder("");
    }
}
