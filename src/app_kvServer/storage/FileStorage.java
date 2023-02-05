package app_kvServer.storage;
import java.io.*;
import java.util.*;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import javax.swing.plaf.metal.MetalIconFactory.FileIcon16;

import app_kvServer.storage.IFileStorage;


public class FileStorage implements IFileStorage{
    private static Logger logger = Logger.getRootLogger();
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
        System.out.println("file exists");
        // if(this.db_file ==null){
            File f1 = new File(this.file_path);
            if(!f1.exists()){
                try{
                    f1.mkdir();
                }
                catch(Exception e){
                    logger.error("Error! " +
							"Unable to create directory for database. \n", e);
                }
                
            }
            else{
                this.db_file = new File(this.file_path+"/"+this.file_name);
            
                try{
                    if(this.db_file.exists()){
                        
                        loadKVFromFile();
                        logger.info("Loading existing database file into memory. \n");
                    }
                    else{
                        this.db_file.createNewFile();
                        logger.info("Creating database file. \n");
                    }
                    
                }
                catch(Exception e){
                    logger.error("Error! " +
							"Unable to create file for database. \n", e);
                }
            }
            
        
        
    }
    private void loadKVFromFile(){
        Properties prop = new Properties();
        // Map<String, String> temp = new HashMap<String, String>();
        try{
            FileInputStream fin= new FileInputStream(file_path+"/"+file_name);
            prop.load(fin);
            for (String key : prop.stringPropertyNames())
            {
                this.hash_table.put(key, prop.get(key).toString());
            }
            fin.close();
        } catch(Exception e){
            logger.error("Error! " +
							"Unable to open and load database file. \n", e);
        }
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
            FileOutputStream file_output = new FileOutputStream(file_path+"/"+file_name);
            prop.store(file_output, null);
            logger.info("Stored memory data into file. \n");
        } catch(IOException e){
            logger.error("Error! " +
							"Unable to store memory data into database file. \n", e);
        }
    }
    @Override
    public void putKV(String key, String value){
        if(value==null){
            this.hash_table.remove(key);
        }
        else{
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
            logger.info("Removed memory data and database file. \n");
        } catch(Exception e){
            logger.error("Error! " +
							"Unable to delete memory data and database file. \n", e);
        }
    }
}