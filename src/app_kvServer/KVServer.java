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
	 */
	public KVServer(String serverAddress, int port, int cacheSize, String strategy) {
		this.serverAddress = serverAddress;
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheStrategy = CacheStrategy.valueOf(strategy);
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

	/**
	 * Initializes the server on a given port
	 */
	private boolean initializeServer() {
		logger.info("Initialize server ...");

		try {
			fs = new FileStorage();
		}
		catch (Exception ex) {
			logger.error(ex);
		}

		if (fs == null) {
			logger.error("Failed to initialize fs");
		}

		
		try {
			
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
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
	 * Main entry point for the echo server application.
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
				String fileDirectory;
				int port = 8080;
				String logPath = "logs/server.log";
				String logLevel = "ALL";

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
				
				new KVServer(address, port, 100, "FIFO");
			}

			// new LogSetup("logs/server.log", Level.ALL);
			// if(args.length != 3) {
			// 	System.out.println("Error! Invalid number of arguments!");
			// 	System.out.println("Usage: Server <port> <cacheSize> <cacheStrategy>");
			// } else {
			// 	int port = Integer.parseInt(args[0]);
			// 	int cacheSize = Integer.parseInt(args[1]);
			// 	String cacheStrategy = args[2];

			// 	try {
			// 		CacheStrategy strategy = CacheStrategy.valueOf(cacheStrategy);
			// 	}
			// 	catch (IllegalArgumentException e) {
			// 		System.out.println("Error! <cacheStrategy> must be one of: FIFO, LRU, LFU or None");
			// 		System.out.println("Usage: Server <port> <cacheSize> <cacheStrategy>");
			// 		System.exit(1);
			// 	}

			// 	new KVServer(port, cacheSize, cacheStrategy);
			// }
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument! Either <port> or <cacheSize> is not a number!");
			System.out.println("Usage: Server <port> <cacheSize> <cacheStrategy>");
			System.exit(1);
		}
	}
}
