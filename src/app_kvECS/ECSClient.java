package app_kvECS;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import ecs.IECSNode;
import ecs.ECSNode;
import shared.messages.Metadata;
import shared.messages.ECSMessage;
import shared.module.MD5Hasher;

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
    private List<String>  serverList = new ArrayList<>();

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

    // Find the server to be added/removed from the list and its successor in the ring,
    // return a list [server, successor]
    // If the server is the first to be added or last ro be removed, return [server]
    public List<String> findTargetServers(List<String> oldList, List<String> newList) {
        List<String> result = new ArrayList<>();
        if (oldList.size() == 0) {
            return newList;
        }
        if (newList.size() == 0) {
            return oldList;
        }

        // Find server to be added/removed
        HashSet<String> set = new HashSet<>();

        assert oldList.size() != newList.size();
        if (oldList.size() < newList.size()) {
            set.addAll(oldList);
            for (String server : newList) {
                if (!set.contains(server)) {
                    result.add(server);
                }
            }
        } else {
            set.addAll(newList);
            for (String server : oldList) {
                if (!set.contains(server)) {
                    result.add(server);
                }
            }
        }

        // Find successor of the server to be added/removed
        String listOfHostPorts = "";
        if (oldList.size() < newList.size()) {
            listOfHostPorts = String.join(" ", oldList);
        } else {
            listOfHostPorts = String.join(" ", newList);
        }
        Metadata metadata = new Metadata(listOfHostPorts);
        ECSNode successorNode = metadata.findNode(result.get(0));
        if (successorNode != null) {
            String successor = successorNode.getNodeHost() + ":" + successorNode.getNodePort();
            result.add(successor);
        }

        return result;
    }

    public void processServerListChange(List<String> oldList, List<String> newList) {
        System.out.println("Process server list change");

        List<String> targetServers = findTargetServers(oldList, newList);
        System.out.println("Server added/removed: " + targetServers.get(0));
        if (targetServers.size() == 2) {
            System.out.println("Its successor: " + targetServers.get(1));
        }

        // Send metadata to target servers in the list
        String listOfHostPorts = String.join(" ", newList);
        String metadata = MD5Hasher.buildKeyRangeMessage(listOfHostPorts);
        System.out.println("listOfHostPorts: " + listOfHostPorts);
        System.out.println("Metadata: " + metadata);

        for (int i = 0; i < targetServers.size(); i++) {
            String[] hostPort = targetServers.get(i).split(":");
            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);

            // Build ECSMessage
            String serverToContact = null;
            if (targetServers.size() > 1) {
                if (i == 0) {
                    serverToContact = targetServers.get(1);
                } else {
                    serverToContact = targetServers.get(0);
                }
            }
            ECSMessage message = new ECSMessage(ECSMessage.ECSMessageStatus.TRANSFER_BEGIN, metadata, serverToContact);

            ECSKVServerConnection connection = new ECSKVServerConnection(host, port, message);
            logger.info("Start a thread to send metadata to " + host + ":" + port);
            new Thread(connection).start();
        }
    }

    private boolean initializeZooKeeper() {
        // Connect to ZooKeeper and create a watcher on path /servers to watch for KVServer nodes
        try {
            zk = new ZooKeeper("localhost:2181", 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    try {
                        // Create /servers znode if it doesn't exist
                        if (zk.exists(serverListPath, true) == null) {
                            createZnode(serverListPath, "initial server list");
                        }
                        List<String> currentServers = zk.getChildren(serverListPath, true);
                        if (event.getType() == Event.EventType.NodeChildrenChanged 
                                && event.getPath().equals(serverListPath)) {
                            String serverListString = String.join(" ", currentServers);
                            logger.info("Server list changed!");
                            logger.info("Server list: " + serverListString);

                            // Send updated metadata to corresponding KVServers based on server list change
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

        // Create metadata znode
		createZnode(metadataPath, "initial metadata");
        
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
            // Do nothing
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
