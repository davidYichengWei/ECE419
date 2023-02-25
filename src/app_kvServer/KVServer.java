package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.net.InetAddress;
import app_kvServer.storage.FileStorage;

import org.slf4j.LoggerFactory; // Required for ZooKeeper
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class KVServer implements IKVServer, Runnable {

	private static Logger logger = Logger.getRootLogger();

	// Parameters for server initialization
	private String serverAddress = "localhost";
	private int port;
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
	public KVServer(String serverAddress, int port, int cacheSize, String strategy, String fileDirectory) {
		this.serverAddress = serverAddress;
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheStrategy = CacheStrategy.valueOf(strategy);

		// To be able to call clearStorage() at test setup
		try {
			fs = new FileStorage(fileDirectory);
		}
		catch (Exception ex) {
			logger.error(ex);
		}

		if (fs == null) {
			logger.error("Failed to initialize fs");
		}

		new Thread(this).start(); // To prevent blocking in tests
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
		return "127.0.0.1";
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
			zk = new ZooKeeper("localhost:2181", 3000, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					try {
						String newData = new String(zk.getData(metadataPath, true, null));
						if (event.getType() == Event.EventType.NodeDataChanged && !newData.equals(metadata)) {
							metadata = newData;
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
				
				new KVServer(address, port, 100, "FIFO", fileDirectory);
			}
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument! Either <port> or <cacheSize> is not a number!");
			System.out.println("Usage: Server <port> <cacheSize> <cacheStrategy>");
			System.exit(1);
		}
	}
}
