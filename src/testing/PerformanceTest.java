package testing;

import app_kvServer.KVServer;
import java.io.*;
import java.util.*;
public class PerformanceTest {
    private KVServer server;

    public PerformanceTest() {
        try {
            this.server = new KVServer("127.0.0.1", 8080, 2000, "LRU");
        } catch (Exception e) {
            
        }
    }
    public void performancePutShort(int numGet, int numPut){
        long  startTime = System.currentTimeMillis(); 
        try {
            for(int i=0;i<numPut;i++){
                server.putKV(Integer.toString(i), Integer.toString(i));
            }
            
        } catch (Exception e) {
            
        }
        

        long endTime = System.currentTimeMillis();

        System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    }
    public void performancePutMedium(int numGet, int numPut){
        long  startTime = System.currentTimeMillis(); 
        try {
            for(int i=0;i<numPut;i++){
                server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO");
            }
            
        } catch (Exception e) {
            
        }
        

        long endTime = System.currentTimeMillis();

        System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    }
    public void performancePutLarge(int numGet, int numPut){
        long  startTime = System.currentTimeMillis(); 
        try {
            for(int i=0;i<numPut;i++){
                server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLO");
            }
            
        } catch (Exception e) {
            
        }
        

        long endTime = System.currentTimeMillis();

        System.out.println("Number of large puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    }
    public void performance80Put20Get(int numGet, int numPut){
        long  startTime = System.currentTimeMillis(); 
        try {
            for(int i=0;i<numPut;i++){
                // server.putKV(Integer.toString(i), Integer.toString(i));

                // server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO");

                server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLO");
            }

            for(int i=0;i<numGet;i++){
                server.getKV(Integer.toString(i));
            }
        } catch (Exception e) {
            
        }
        

        long endTime = System.currentTimeMillis();

        System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    }
    public void performance20Put80Get(int numGet, int numPut){
        long  startTime = System.currentTimeMillis(); 
        try {
            for(int i=0;i<numPut;i++){
                // server.putKV(Integer.toString(i), Integer.toString(i));

                // server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO");

                server.putKV(Integer.toString(i), "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLOHELLO"+
                "HELLOHELLOHELLOHELLOHELLOHELLOHELLO");
            }

            for(int i=0;i<8;i++){
                for(int j=0;j<100;j++){
                    server.getKV(Integer.toString(j));
                }
                
            }
        } catch (Exception e) {
            
        }
        

        long endTime = System.currentTimeMillis();

        System.out.println("Number of short puts: "+ Integer.toString(numPut)+ "_____Running time for this test is " + (endTime - startTime) + "ms"); 
    }

    public static void main(String[] args) {
        PerformanceTest p =  new PerformanceTest();
        // p.performancePutShort(0, 1000);
    //     p.server.clearStorage();
        // p.performancePutMedium(0, 1000);
        // p.server.clearStorage();
        p.performance20Put80Get(800, 200);

        p.server.clearStorage();
    }
}
