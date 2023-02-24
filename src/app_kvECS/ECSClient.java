package app_kvECS;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
	private String logLevel = "INFO";

    private ZooKeeper zk;
    private final String metadataPath = "/metadata";
    private final String serverListPath = "/servers";
    private List<String> serverList;

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

    private void processServerListChange(List<String> oldList, List<String> newList) {
        // TODO
        System.out.println("Process server list change");
    }

    private boolean initializeZooKeeper() {
        // Connect to ZooKeeper and create a watcher on path /servers to watch for KVServer nodes
        try {
            zk = new ZooKeeper("localhost:2181", 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    try {
                        List<String> currentServers = zk.getChildren(serverListPath, true);
                        if (event.getType() == Event.EventType.NodeChildrenChanged 
                                && event.getPath().equals(serverListPath)) {
                            String serverListString = String.join(" ", currentServers);
                            logger.info("Server list changed!");
                            logger.info("Server list: " + serverListString);

                            processServerListChange(serverList, currentServers);
                            serverList = currentServers;
                        }
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            logger.info("Connected to ZooKeeper and created watcher on path /servers");
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }

        // Create metadata and /servers znode
		createZnode(metadataPath, "initial metadata");
        createZnode(serverListPath, "initial server list");

        return true;
    }

    /**
	 * Initializes ECS on a given port
	 */
	private boolean initializeECS() {
        
		try {
            new LogSetup(logPath, Level.INFO);
		    logger.info("Initialize ECS ...");
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

        if (initializeZooKeeper() == false) {
            return false;
        }

        return true;
	}

    public String getZkData(String path) {
        try {
            byte[] data = zk.getData(path, false, null);
            return new String(data);
        } catch (KeeperException e) {
            System.err.println("Failed to get znode data: " + e.getMessage());
        } catch (Exception ex) {
            logger.error(ex);
        }
        return null;
    }

    public void setZkData(String path, String data) {
        try {
            byte[] b = data.getBytes();
            zk.setData(path, b, zk.exists(path, true).getVersion());
        } catch (KeeperException e) {
            System.err.println("Failed to update znode data: " + e.getMessage());
        } catch (Exception ex) {
            logger.error(ex);
        }
    }

    public void createZnode(String path, String data) {
        try {
            byte[] b = data.getBytes();
            zk.create(path, b, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Created znode: " + path + " with data: " + data);
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("ZNode already exists, updating data...");
            setZkData(path, data);
        } catch (KeeperException e) {
            System.err.println("Failed to create znode: " + e.getMessage());
        } catch (Exception ex) {
            logger.error(ex);
        }
    }

    /**
	 * Initializes and starts ECS.
	 * Loops until ECS should be closed.
	 */
	public void run() {

		running = initializeECS();

        while(isRunning()) {
            int tmp = 1;
        }
        

		logger.info("ECS stopped.");
	}

    private boolean isRunning() {
		return this.running;
	}

    public static void main(String[] args) {
        new ECSClient("localhost", 8000);
    }
}
