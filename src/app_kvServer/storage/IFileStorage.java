package app_kvServer.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IFileStorage {
    void putKV(String key, String value) throws IOException;
    
    String getKV(String key) throws IOException;

    boolean ifInStorage(String key) throws IOException;

    void clearStorage() throws IOException;

}
