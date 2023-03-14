package testing;

import java.beans.Transient;

import org.junit.Test;

import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;


public class MultiServerTest extends TestCase {

	private KVStore kvClient;

    private List<String> testKeys = new ArrayList<String>(Arrays.asList
        ("dne-key1", "dne-key2", "dne-key3", "dne-key4", "dne-key5", 
        "dne-key6", "dne-key7", "dne-key8", "dne-key9", "dne-key10"));
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}

        // Start 2 new servers
        KVServer server2 = new KVServer("localhost", 50001, "localhost", 10, "FIFO", "db_files");
        // Wait for 5 seconds to allow the server to start
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        server2.clearStorage();

        KVServer server3 = new KVServer("localhost", 50002, "localhost", 10, "FIFO", "db_files");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        server3.clearStorage();
	}

	public void tearDown() {
		kvClient.disconnect();
	}

    @Test
	public void testRedirect() {
        boolean redirected = false;
        for (String key : testKeys) {
            KVMessage response = null;
		    Exception ex = null;

            try {
                response = kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }

            assert ex == null;
            if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
                redirected = true;
                break;
            }
        }

        assert redirected == true;
    }


    @Test
    public void testGetUnsetValues() {
        for (String key : testKeys) {
            KVMessage response = null;
		    Exception ex = null;

            try {
                response = kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }
    
            assertTrue(ex == null && 
                (response.getStatus() == StatusType.GET_ERROR || response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
        }
    }

    @Test
    public void testPutUnsetValue() {
        for (String key : testKeys) {
            KVMessage response = null;
		    Exception ex = null;

            try {
                response = kvClient.put(key, "value");
            } catch (Exception e) {
                ex = e;
            }
    
            assertTrue(ex == null && 
                (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
        }
    }

    @Test
    public void testPutUpdate() {
        for (String key : testKeys) {
            KVMessage response = null;
		    Exception ex = null;

            try {
                response = kvClient.put(key, "newValue");
            } catch (Exception e) {
                ex = e;
            }
    
            assertTrue(ex == null && 
                (response.getStatus() == StatusType.PUT_UPDATE || response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
        }
    }

    @Test
    public void testDelete() {
        for (String key : testKeys) {
            KVMessage response = null;
		    Exception ex = null;

            try {
                response = kvClient.put(key, "null");
            } catch (Exception e) {
                ex = e;
            }
    
            assertTrue(ex == null && 
                (response.getStatus() == StatusType.DELETE_SUCCESS || response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
        }
    }

    @Test
    public void testGetAfterDelete() {
        for (String key : testKeys) {
            KVMessage response = null;
            Exception ex = null;

            try {
                response = kvClient.put(key, "value");
            } catch (Exception e) {
                ex = e;
            }
    
            assert ex == null;
        }

        testDelete();
        testGetUnsetValues();
    }

}
