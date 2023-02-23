package app_kvECS;

import java.util.Map;
import java.util.Collection;

import ecs.IECSNode;

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

import org.slf4j.LoggerFactory; // Required for ZooKeeper
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();

    private String address = "localhost";
    private int port = 8000;
    private String logPath = "logs/ecs.log";
	private String logLevel = "ALL";

    private ZooKeeper zk;
    private final String metadataPath = "/metadata";

    private boolean running;
    private ServerSocket serverSocket;

    public ECSClient(String address, int port) {
        this.address = address;
        this.port = port;
        run();
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    /**
	 * Initializes ECS on a given port
	 */
	private boolean initializeECS() {
        
		try {
            new LogSetup(logPath, Level.ALL);
		    logger.info("Initialize ECS ...");
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
            zk = new ZooKeeper("localhost:2181", 5000, null);
            logger.info("Connected to ZooKeeper");

			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

    /**
	 * Initializes and starts ECS.
	 * Loops until ECS should be closed.
	 */
	public void run() {

		running = initializeECS();

        // Create metadata znode
		try {
            byte[] data = "Hello, World!".getBytes();
            zk.create(metadataPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("ZNode created successfully!");
        } catch (KeeperException.NodeExistsException e) {
            System.err.println("ZNode already exists!");
        } catch (KeeperException e) {
            System.err.println("Failed to create znode: " + e.getMessage());

        } catch (Exception ex) {
            logger.error(ex);
        }

        // Get metadata znode
        try {
            Stat stat = new Stat();
            byte[] data = zk.getData(metadataPath, false, stat);
            System.out.println("ZNode data: " + new String(data));
        } catch (KeeperException e) {
            System.err.println("Failed to get znode data: " + e.getMessage());
        } catch (Exception ex) {
            logger.error(ex);
        }


		logger.info("ECS stopped.");
	}

    public static void main(String[] args) {
        new ECSClient("localhost", 8000);
    }
}
