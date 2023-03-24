package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.*;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.net.InetAddress;
import app_kvServer.storage.FileStorage;
import ecs.ECSNode;

import org.slf4j.LoggerFactory; // Required for ZooKeeper
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.messages.Metadata;
import shared.messages.ServerMessage;
import shared.messages.ServerMessage.ServerMessageStatus;
import shared.module.MD5Hasher;

public class KVServer implements IKVServer, Runnable {

	private static Logger logger = Logger.getRootLogger();

	// Parameters for server initialization
	private String serverAddress = "localhost";
	private int port;
	private String zkAddress = "localhost";
	private String logPath;
	private String logLevel = "ALL";

	private ServerSocket serverSocket;
	private boolean running;
	private int cacheSize;
	private CacheStrategy cacheStrategy;
	private FileStorage fs;

	private ZooKeeper zk;
	private final String metadataPath = "/metadata";
	// Metadata of server keyrange, should be in the form <kr-from>, <kr-to>, <ip:port>; <kr-from>, <kr-to>, <ip:port>;...
	private String metadata = "initial metadata";
	private boolean shutdownFinished = false; // For the shutdown hook
	private Metadata metadataObj;
	private ClientConnection serverConnection;
	private ServerStatus status;

	private boolean shuttingDown = false;

	public void setMetadata(String metadata) {
		this.metadata = metadata;
		this.metadataObj = new Metadata(MD5Hasher.buildListOfPorts(metadata));
	}

	public Metadata getMetadataObj() {
		return metadataObj;
	}

	public void setStatus(ServerStatus status) {
		this.status = status;
	}

	public ServerStatus getStatus() {
		return status;
	}

	/**
	 * Start KV Server at given port
	 * @param serverAddress that the server binds to
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 * @param directory where the database files are stored
	 */
	public KVServer(String serverAddress, int port, String zkAddress, int cacheSize, String strategy, String fileDirectory) {
		
		this.serverAddress = serverAddress;
		this.port = port;
		this.zkAddress = zkAddress;
		this.cacheSize = cacheSize;
		this.cacheStrategy = CacheStrategy.valueOf(strategy);

		// To be able to call clearStorage() at test setup
		try {
			fs = new FileStorage(fileDirectory, serverAddress + ":" + port);
		}
		catch (Exception ex) {
			logger.error(ex);
		}

		if (fs == null) {
			logger.error("Failed to initialize fs");
		}

		new Thread(this).start(); // To prevent blocking in tests
	}

	public synchronized boolean getShutdownFinished() {
		return shutdownFinished;
	}

	public void setShutdownFinished(boolean shutdownFinished) {
		this.shutdownFinished = shutdownFinished;
	}

	public boolean getShuttingDown() {
		return shuttingDown;
	}

	public void setShuttingDown(boolean shuttingDown) {
		this.shuttingDown = shuttingDown;
	}

	public void deleteZnode() {
		try {
			zk.delete("/servers/" + serverAddress + ":" + port, zk.exists("/servers/" + serverAddress + ":" + port, false).getVersion());
			logger.info("Deleted znode for server: " + "/servers/" + serverAddress + ":" + port);
		}
		catch (KeeperException ex) {
			logger.error("Failed to delete znode for server");
		}
		catch (InterruptedException ex) {
			logger.error("Failed to delete znode for server");
		}
	}
	
	@Override
	public int getPort(){
		return port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		// InetAddress ia = InetAddress.getLocalHost();
		// String host = ia.getHostName();
		return serverAddress;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return cacheStrategy;
	}

	@Override
    public int getCacheSize(){
		return cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		
		return fs.ifInStorage(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		// return CacheStorage.ifInCache(key);
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return fs.getKV(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		
		fs.putKV(key, value);
		
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		fs.clearStorage();
		// TODO Auto-generated method stub
	}
	public ServerMessage receiveMessage(InputStream input) throws IOException {
		int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[1024];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if(index == 1024) {
                if(msgBytes == null){
                    tmp = new byte[1024];
                    System.arraycopy(bufferBytes, 0, tmp, 0, 1024);
                } else {
                    tmp = new byte[msgBytes.length + 1024];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
					1024);
                }

                msgBytes = tmp;
                bufferBytes = new byte[1024];
                index = 0;
            }

            /* only read valid characters, i.e. letters and numbers */
            if((read >= 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= 128 * 1024) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;


        /* build final String */
        ServerMessage msg = new ServerMessage(msgBytes);
		return msg;
	}

	public void updateZnodeMetadata(String metadata) {
		try {
			this.zk.setData(metadataPath, metadata.getBytes(), this.zk.exists(metadataPath, true).getVersion());
			logger.info("Updated znode metadata");
		}
		catch (KeeperException ex) {
			logger.error("Failed to update znode metadata");
		}
		catch (InterruptedException ex) {
			logger.error("Failed to update znode metadata");
		}
	}
	public String convert_map_string(Map<String, String> hash){
		String temp="";
		for(String i:hash.keySet()){
			String one_pair=i+"&&&&&"+hash.get(i);
			temp+=one_pair;
			temp+=";;;;;";
		}
		return temp;
	}
	public void dumpKV(String server){

		String kvPairs = convert_map_string(fs.get_all_pairs());
		
		String[] dest = server.split(":");
		try {
			Socket socket = new Socket(dest[0], Integer.parseInt(dest[1]));
			System.out.println(dest[0]);
			System.out.println(dest[1]);
			this.status = ServerStatus.SERVER_WRITE_LOCK;
			OutputStream output = socket.getOutputStream();
			InputStream input = socket.getInputStream();
			System.out.println("TRANSFERING KV PAIRS" +kvPairs);
			ServerMessage msg = new ServerMessage(ServerMessage.ServerMessageStatus.SEND_KV, kvPairs);
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
			
			ServerMessage reply = receiveMessage(input);
			if(reply.getServerStatus() == ServerMessageStatus.SEND_KV_ACK){
				fs.clearStorage();
			}
			this.status = ServerStatus.RUNNING;
		} catch (IOException e) {
            logger.error("Error dumping Server message: " + e.getMessage(), e);
        }

	}
	public void send_one_kv(String server, String key, String value){
		Map<String, String> move_map = new HashMap<String, String>();
		move_map.put(key, value);
		String kvPairs = convert_map_string(move_map);
		String[] dest = server.split(":");
		try {
			Socket socket = new Socket(dest[0], Integer.parseInt(dest[1]));
			System.out.println(dest[0]);
			System.out.println(dest[1]);
			this.status = ServerStatus.SERVER_WRITE_LOCK;
			OutputStream output = socket.getOutputStream();
			InputStream input = socket.getInputStream();
			System.out.println("REPLICATING KV PAIR" +kvPairs);
			ServerMessage msg = new ServerMessage(ServerMessage.ServerMessageStatus.REPLICATE_KV, kvPairs);
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
			
			ServerMessage reply = receiveMessage(input);
			
			if(reply.getServerStatus() == ServerMessageStatus.REPLICATE_KV_ACK){

				logger.info("REPLICATED PAIR");
			}
			
        } catch (IOException e) {
            logger.error("Error sending Server message: " + e.getMessage(), e);
        }
	}
	public void transferKV(String[] hashRange, String server){
		Map<String, String> move_map = fs.findKVOutOfRange(hashRange);
		// move_map.put("------------", "++++++++++++++++++");
		String kvPairs = convert_map_string(move_map);
		String[] dest = server.split(":");
		
		try {
			Socket socket = new Socket(dest[0], Integer.parseInt(dest[1]));
			System.out.println(dest[0]);
			System.out.println(dest[1]);
			this.status = ServerStatus.SERVER_WRITE_LOCK;
			OutputStream output = socket.getOutputStream();
			InputStream input = socket.getInputStream();
			System.out.println("TRANSFERING KV PAIRS" +kvPairs);
			ServerMessage msg = new ServerMessage(ServerMessage.ServerMessageStatus.SEND_KV, kvPairs);
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
			
			ServerMessage reply = receiveMessage(input);
			
			if(reply.getServerStatus() == ServerMessageStatus.SEND_KV_ACK){

				if (shuttingDown == false) {
					// SEND SET_RUNNING to serverToContact
					ServerMessage runMsg = new ServerMessage(ServerMessage.ServerMessageStatus.SET_RUNNING, "");
					byte[] runmsgBytes = runMsg.toByteArray();
            		output.write(runmsgBytes, 0, runmsgBytes.length);
            		output.flush();
				}
				else {
					fs.deleteKVBatch(move_map); // only delete KV pairs if shutting down
					// Set shutdownFinished to true
					shutdownFinished = true;
				}
			}
			this.status = ServerStatus.RUNNING;
			
        } catch (IOException e) {
            logger.error("Error sending Server message: " + e.getMessage(), e);
        }
		
		
		
	}

	public void receiveKV(Map<String, String> batch){
		fs.receive_pairs(batch);
	}
	
	// Shutdown hook triggered when KVServer is stopped with ctrl+c
	// Delete znode for the KVServer and wait to process ECSMessage
	public void shutDown() {
		fs.display();
		shuttingDown = true;
		deleteZnode();

		System.out.println("Shutting down KVServer ...");
		while(!getShutdownFinished()) {
			// Wait for ECSMessage
			// Need to set shutdownFinished to true at the end of data transfer
		}

		System.out.println("KVServer shut down.");
	}

	@Override
	/**
	 * Initializes and starts the server.
	 * Loops until the the server should be closed.
	 */
	public void run() {

		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection =
							new ClientConnection(client, this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	// Connect to Zookeeper, create a watcher for metadata changes, and create a znode for the KVServer
	private boolean initializeZooKeeper() {
		// Connect to Zookeeper, create a watcher for metadata changes
		try {
			zk = new ZooKeeper(zkAddress + ":2181", 3000, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					try {
						String newData = new String(zk.getData(metadataPath, true, null));
						if (event.getType() == Event.EventType.NodeDataChanged) {
							if (metadata.equals("initial metadata") == false) {
								// Process replication and reconsiliation if it's not the new node
								processServerChange(metadata, newData);
							}

							metadata = newData;
							metadataObj = new Metadata(MD5Hasher.buildListOfPorts(metadata));
							logger.info("Metadata value changed to: " + metadata);
						}
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
			logger.info("Connected to ZooKeeper");
		}
		catch (IOException ex) {
			logger.error("Failed to connect to ZooKeeper");
			return false;
		}

		// Create a znode for the KVServer under path /servers
		// path: /servers/server-<ip:port>
		// value: <ip:port>
		try {
			String serverPath = "/servers/" + serverAddress + ":" + port;
			if (zk.exists(serverPath, false) == null) {
				zk.create(serverPath, (serverAddress + ":" + port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				logger.info("Created znode for server: " + serverPath);
			}
		}
		catch (KeeperException ex) {
			logger.error("Failed to create znode for server");
			return false;
		}
		catch (InterruptedException ex) {
			logger.error("Failed to create znode for server");
			return false;
		}

		return true;
	}

	// Process replication and reconsiliation when a server is added or removed
	private void processServerChange(String oldMetadataString, String newMetadataString) {
		System.out.println("oldMetadataString: " + oldMetadataString);
		System.out.println("newMetadataString: " + newMetadataString);
		List<String> oldServerList = new ArrayList<>(Arrays.asList(MD5Hasher.buildListOfPorts(oldMetadataString).split(" ")));
		List<String> newServerList = new ArrayList<>(Arrays.asList(MD5Hasher.buildListOfPorts(newMetadataString).split(" ")));

		Metadata oldMetadata = new Metadata(MD5Hasher.buildListOfPorts(oldMetadataString));
		Metadata newMetadata = new Metadata(MD5Hasher.buildListOfPorts(newMetadataString));

		ECSNode nodeChange = Metadata.findDifferentNode(oldMetadata, newMetadata);
		String serverChange = nodeChange.getNodeHost() + ":" + nodeChange.getNodePort();
		logger.info("serverChange: " + serverChange);

		String currentServer = serverAddress + ":" + port;
		logger.info("currentServer: " + currentServer);
		ECSNode currentNode = newMetadata.findNode(MD5Hasher.hash(currentServer));
		System.out.println("Current node: " + currentNode.getNodeHost() + ":" + currentNode.getNodePort());

		// Adding a server
		if (newServerList.size() > oldServerList.size()) {
			logger.info("A server was added: " + serverChange + ". Processing replication and reconsiliation.");
			// 1st and 2nd predecessor of the new node need to replicate data to the new node
			ECSNode firstPredecessor = newMetadata.findPredecessor(nodeChange);
			System.out.println("First predecessor of new node: " + firstPredecessor.getNodeHost() + ":" + firstPredecessor.getNodePort());
			ECSNode secondPredecessor = newMetadata.findPredecessor(firstPredecessor);
			System.out.println("Second predecessor of new node: " + secondPredecessor.getNodeHost() + ":" + secondPredecessor.getNodePort());

			if (currentNode.equals(firstPredecessor) || currentNode.equals(secondPredecessor)) {
				// Replicate data to the new node
				logger.info("Current server " + currentServer + " is the 1st/2nd predecessor of the new server " 
					+ serverChange + ". Replicating data to the new server.");
				
				// Swap the start and end range to transfer KV pairs in the range
				String[] range = currentNode.getNodeHashRange();
				String tmp = range[0];
				range[0] = range[1];
				range[1] = tmp;
				transferKV(range, serverChange);
			}

			// The 3 successors of the new node need to reconsiliate data
			ECSNode firstSuccessor = newMetadata.findSuccessor(nodeChange);
			ECSNode secondSuccessor = newMetadata.findSuccessor(firstPredecessor);
			ECSNode thirdSuccessor = newMetadata.findSuccessor(secondPredecessor);

			if (currentNode.equals(firstSuccessor) || currentNode.equals(secondSuccessor) || currentNode.equals(thirdSuccessor)) {
				// Reconsiliate data, delete data out of the combined range of itself and its 2 predecessors
				logger.info("Current server " + currentServer + " is the 1st/2nd/3rd successor of the new server " 
					+ serverChange + ". Deleting data.");
				String[] totalRange = newMetadata.findTotalRange(currentNode);
				System.out.println("totalRange: " + totalRange[0] + " " + totalRange[1]);
				Map<String, String> pairsToDelete = fs.findKVOutOfRange(newMetadata.findTotalRange(currentNode));
				fs.deleteKVBatch(pairsToDelete);
			}
		}
		// Removing a server
		else {
			logger.info("A server was removed: " + serverChange + ". Processing replication and reconsiliation.");
			// If the 2nd successor of the current server in the new metadata is one of the 3 successors 
			// of the removed server in the old metadata, the current server needs to transfer its data to the 2nd successor
			ECSNode secondSuccessor = newMetadata.findSuccessor(newMetadata.findSuccessor(currentNode));

			ECSNode changeNodeSuccessor1 = oldMetadata.findSuccessor(nodeChange);
			ECSNode changeNodeSuccessor2 = oldMetadata.findSuccessor(changeNodeSuccessor1);
			ECSNode changeNodeSuccessor3 = oldMetadata.findSuccessor(changeNodeSuccessor2);

			if (!currentNode.equals(secondSuccessor) && 
				(secondSuccessor.equals(changeNodeSuccessor1) || secondSuccessor.equals(changeNodeSuccessor2) 
				|| secondSuccessor.equals(changeNodeSuccessor3))) {

				logger.info("Transferring data to the 2nd successor of the current server.");
				
				// Swap the start and end range to transfer KV pairs in the range
				String[] range = currentNode.getNodeHashRange();
				String tmp = range[0];
				range[0] = range[1];
				range[1] = tmp;
				String secondSuccessorHostPort = secondSuccessor.getNodeHost() + ":" + secondSuccessor.getNodePort();
				transferKV(range, secondSuccessorHostPort);
			}
		}
	}

	/**
	 * Initializes the server on a given port
	 */
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}

		// Connect to ZooKeeper and create a watcher on path /metadata
		if (initializeZooKeeper() == false) {
			return false;
		}

		// Test: update metadata to address:port
		// Result: it's working
		// try {
		// 	zk.setData(metadataPath, (serverAddress + ":" + port).getBytes(), zk.exists(metadataPath, true).getVersion());
		// }
		// catch (KeeperException ex) {
		// 	logger.error("Failed to update metadata");
		// 	return false;
		// }
		// catch (InterruptedException ex) {
		// 	logger.error("Failed to update metadata");
		// 	return false;
		// }
		this.setStatus(ServerStatus.SERVER_STOPPED);
		return true;
	}

	private boolean isRunning() {
		return this.running;
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}

	/**
	 * Main entry point for the KV server.
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			if (args.length %2 != 0) {
				// Print usage
				System.out.println("Usage: Server -a <address> -p <port> -d <file directory> -l <log path> -ll <log level>");
			}
			else {
				String address = "localhost";
				String fileDirectory = "db_files";
				int port = 8080;
				String logPath = "logs/server.log";
				String logLevel = "INFO";
				String zkAddress = "localhost";

				String curFlag = "-h";
				for (int i = 0; i < args.length; i++) {
					if (i %2 == 0) {
						curFlag = args[i];
					}
					else {
						switch (curFlag) {
							case "-a":
								address = args[i];
								break;
							case "-p":
								System.out.println("Setting port to " + args[i]);
								port = Integer.parseInt(args[i]);
								break;
							case "-d":
								fileDirectory = args[i];
								break;
							case "-l":
								logPath = args[i];
								break;
							case "-ll":
								logLevel = args[i];
								break;
							case "-zk":
								zkAddress = args[i];
								break;
							default:
								System.out.println("Usage: Server -a <address> -p <port> -d <file directory> -l <log path> -ll <log level>");
								System.exit(1);
								break;
						}
					}
				}

				try {
					new LogSetup(logPath, Level.toLevel(logLevel));
				} catch (IOException e) {
					System.out.println("Error! Unable to initialize logger!");
					e.printStackTrace();
					System.exit(1);
				}
				
				final KVServer server = new KVServer(address, port, zkAddress, 100, "FIFO", fileDirectory);

				// Add a shutdown hook to process graceful shutdown
				Runtime.getRuntime().addShutdownHook(new Thread() {
					@Override
					public void run() {
						server.shutDown();
					}
				});
			}
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument! Either <port> or <cacheSize> is not a number!");
			System.out.println("Usage: Server <port> <cacheSize> <cacheStrategy>");
			System.exit(1);
		}
	}
}
