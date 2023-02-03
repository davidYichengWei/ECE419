package app_kvServer.storage;
import java.io.*;
import java.util.*;

import javax.swing.plaf.metal.MetalIconFactory.FileIcon16;

import app_kvServer.storage.IFileStorage;


public class FileStorage implements IFileStorage{
    public final String file_path;
    public final String file_name;
    private Integer key_max_len = 20;
    private Integer value_max_len = 120000;
    private Map<String, String> hash_table;
    private File db_file;

    public FileStorage() throws Exception{
        this.file_name = "db.properties";
        this.file_path = "db_files";
        initialize();
    }
    
    public FileStorage(String file_path, String file_name) throws Exception{
        this.file_path = file_path;
        this.file_name = file_name;
        initialize();
    }
    private void initialize() throws Exception{
        hash_table=new HashMap<String,String>();
        if(this.db_file ==null){
            File f1 = new File(this.file_path);
            if(!f1.exists()){
                try{
                    f1.mkdir();
                }
                catch(Exception e){
                    System.out.println("creating directory failed");
                }
                
            }
            this.db_file = new File(this.file_path+"/"+this.file_name);
            try{
                this.db_file.createNewFile();
            }
            catch(Exception e){
                System.out.println("creating database file failed");
            }
        }
    }
    private Map<String, String> loadKVFromFile(){
        Properties prop = new Properties();
        Map<String, String> temp = new HashMap<String, String>();
        try{
            FileInputStream fin = new FileInputStream(file_path+"/"+file_name);
            Iterator<String> it = prop.stringPropertyNames().iterator();
            while(it.hasNext())
            {
                String key = it.next();
                temp.put(key, prop.getProperty(key));
            }
            fin.close();
        } catch(Exception e){
            System.out.println("failed loading kv from file");
        }
        return temp;
    }
    private void storeKVInFile(){
        try{
            // for(String i:kv.keySet()){
            //     writer.write(i+" "+kv.get(i)+"\n");
            // }
            // writer.flush();
            // writer.close();
            Properties prop = new Properties();
            for(String i:hash_table.keySet()){
                prop.put(i, hash_table.get(i));

            }
            System.out.println("111");
            FileOutputStream file_output = new FileOutputStream(file_path+"/"+file_name);
            System.out.println("222");
            prop.store(file_output, null);
            System.out.println("333");
        } catch(IOException e){
            System.out.println("storing file wrong.");
        }
    }
    @Override
    public void putKV(String key, String value){
        if(value==null){
            this.hash_table.remove(key);
        }
        else{
            System.out.println("Value " + value);
            this.hash_table.put(key, value);
        }

        storeKVInFile();
    }
    
    @Override
    public String getKV(String key)
    {
        String value=null;
        value=this.hash_table.get(key);
        
        return value;
    }
    @Override
    public boolean ifInStorage(String key){
        if(this.hash_table.get(key)!=null){
            return true;
        }
        else{
            return false;
        }
    }
    @Override
    public void clearStorage(){
        try{
            this.db_file.delete();
            this.hash_table.clear();
        } catch(Exception e){
            System.out.println("cant delete database file");
        }
    }
}