package testing;

import java.beans.Transient;

import org.junit.Test;

import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.ITransactionMessage;
import shared.messages.TransactionMessage;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;


public class MultiServerTest extends TestCase {

	private KVStore kvClient;

    private List<String> testKeys = new ArrayList<String>(Arrays.asList
        ("dne-key1", "dne-key2", "dne-key3", "dne-key4", "dne-key5", 
        "dne-key6", "dne-key7", "dne-key8", "dne-key9", "dne-key10"));

    private List<String> replicationKeys = new ArrayList<String>(Arrays.asList
        ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}

        // Start 3 new servers
        KVServer server2 = new KVServer("localhost", 50001, "localhost", 10, "FIFO", "db_files");
        // Wait for 5 seconds to allow the server to start
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // server2.clearStorage();

        KVServer server3 = new KVServer("localhost", 50002, "localhost", 10, "FIFO", "db_files");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // server3.clearStorage();

        KVServer server4 = new KVServer("localhost", 50003, "localhost", 10, "FIFO", "db_files");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Add replicationKeys to the server
        for (String key : replicationKeys) {
            try {
                kvClient.put(key, "value");
            } catch (Exception e) {
            }
        }
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


    // ----------------- Replication tests ----------------- //
    @Test
    public void testPutAfterReplication() {

        String replicationPutKey = "replication-key-put";
        
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(replicationPutKey, "value");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && 
            (response.getStatus() == StatusType.PUT_SUCCESS ||
            response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
        
    }

    @Test
    public void testUpdateAfterReplication() {
        String replicationUpdateKey = "replication-key-update";
        
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(replicationUpdateKey, "value");
            response = kvClient.put(replicationUpdateKey, "newValue");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && 
            (response.getStatus() == StatusType.PUT_UPDATE ||
            response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
    }

    @Test
    public void testGetAfterReplication() {
        for (String key : testKeys) {
            KVMessage response = null;
		    Exception ex = null;

            try {
                response = kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }
    
            assertTrue(ex == null && 
                (response.getStatus() == StatusType.GET_SUCCESS || response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
        }
    }

    @Test
    public void testEventualConsistency() {
        // Update an existing key, and make sure values are eventually consistent on all replicas
        String key = "2";

        try {
            kvClient.put(key, "newValue");
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Get the value of key 10 times, make sure all values are the same
        String value = null;
        for (int i = 0; i < 10; i++) {
            KVMessage response = null;
            Exception ex = null;

            try {
                response = kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }

            assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && 
            response.getValue().equals("newValue"));
        }
    }

    @Test
    public void testDeleteAfterReplication() {
        String keyToDelete = "3";

        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(keyToDelete, "null");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && 
            (response.getStatus() == StatusType.DELETE_SUCCESS ||
            response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE));
    }

    @Test
    public void testDeleteAndGetAfterReplication() {
        String keyToDelete = "4";

        try {
            kvClient.put(keyToDelete, "null");
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Get the value of key 10 times, make sure all responses are GET_ERROR
        for (int i = 0; i < 10; i++) {
            KVMessage response = null;
            Exception ex = null;

            try {
                response = kvClient.get(keyToDelete);
            } catch (Exception e) {
                ex = e;
            }

            assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
        }
    }

    // M4 tests
    @Test
    public void testTransactionPut() {
        List<String[]> transactionKVPairs = new ArrayList<String[]>();
        transactionKVPairs.add(new String[] {"k1", "v1"});
        transactionKVPairs.add(new String[] {"k2", "v2"});
        transactionKVPairs.add(new String[] {"k3", "v3"});

        TransactionMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.putTransaction(transactionKVPairs);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);
    }

    @Test
    public void testTransactionPutAndGet() {
        List<String[]> transactionKVPairs = new ArrayList<String[]>();
        transactionKVPairs.add(new String[] {"k1", "v1"});
        transactionKVPairs.add(new String[] {"k2", "v2"});
        transactionKVPairs.add(new String[] {"k3", "v3"});

        try {
            kvClient.putTransaction(transactionKVPairs);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Try get the values of the keys
        for (String[] kvPair : transactionKVPairs) {
            KVMessage response = null;
            Exception ex = null;

            try {
                response = kvClient.get(kvPair[0]);
            } catch (Exception e) {
                ex = e;
            }

            assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && 
            response.getValue().equals(kvPair[1]));
        }
    }

    @Test
    public void testTransactionDelete() {
        List<String[]> transactionKVPairs = new ArrayList<String[]>();
        transactionKVPairs.add(new String[] {"k1", "null"});
        transactionKVPairs.add(new String[] {"k2", "null"});
        transactionKVPairs.add(new String[] {"k3", "null"});

        TransactionMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.putTransaction(transactionKVPairs);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);
    }

    @Test
    public void testTransactionGetAfterDelete() {
        List<String[]> transactionKVPairs = new ArrayList<String[]>();
        transactionKVPairs.add(new String[] {"k1", "null"});
        transactionKVPairs.add(new String[] {"k2", "null"});
        transactionKVPairs.add(new String[] {"k3", "null"});

        TransactionMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.putTransaction(transactionKVPairs);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);

        // Try get the values and ensure status is GET_ERROR
        // for (String[] kvPair : transactionKVPairs) {
        //     KVMessage response = null;
        //     Exception ex = null;

        //     try {
        //         response = kvClient.get(kvPair[0]);
        //     } catch (Exception e) {
        //         ex = e;
        //     }

        //     assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
        // }
    }

    @Test
    public void testTransactionFailure() {
        // Start a new client
        KVStore newKVClient = new KVStore("localhost", 50000);
        try {
			newKVClient.connect();
		} catch (Exception e) {
		}

        List<String[]> transactionKVPairs = new ArrayList<String[]>();
        transactionKVPairs.add(new String[] {"k1", "v1"});
        transactionKVPairs.add(new String[] {"k2", "v2"});
        transactionKVPairs.add(new String[] {"k3", "v3"});

        TransactionMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.putTransaction(transactionKVPairs);
            newKVClient.put("k1", "v2");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == ITransactionMessage.TStatusType.TRANSACTION_PUT_FAILURE);

    }

}
