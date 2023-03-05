package testing;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import java.io.*;
import java.util.*;
public class PerformanceTest {
    private KVServer[] server_list;

    public PerformanceTest() {
        try {
            // this.server = new KVServer("127.0.0.1", 8080, 2000, "LRU", "db_files");
            this.server_list = new KVServer[5];
            this.server_list[0] = new KVServer("127.0.0.1", 10000, 2000, "LRU", "db_files");
            
            this.server_list[1] = new KVServer("127.0.0.1", 10001, 2000, "LRU", "db_files");
            this.server_list[2] = new KVServer("127.0.0.1", 10002, 2000, "LRU", "db_files");
            this.server_list[3] = new KVServer("127.0.0.1", 10003, 2000, "LRU", "db_files");
            this.server_list[4] = new KVServer("127.0.0.1", 10004, 2000, "LRU", "db_files");
            
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
        PerformanceTest p =  new PerformanceTest();
        // p.performancePutShort(0, 1000);
    //     p.server.clearStorage();
        // p.performancePutMedium(0, 1000);
        // p.server.clearStorage();
        // p.performance20Put80Get(800, 200);

        // p.server.clearStorage();
        p.scan_folder("");
    }
}
