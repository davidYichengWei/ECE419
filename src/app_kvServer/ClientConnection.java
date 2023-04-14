package app_kvServer;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import ecs.ECSNode;
import org.apache.log4j.*;
import java.net.UnknownHostException;

import app_kvServer.IKVServer.ServerStatus;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.Metadata;
import shared.messages.ECSMessage;
import shared.module.MD5Hasher;
import shared.messages.ServerMessage;
import shared.messages.ServerMessage.ServerMessageStatus;
import shared.messages.ITransactionMessage;
import shared.messages.TransactionMessage;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for handling client requests
 * by calling methods in KVServer.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    public enum MessageType {
		Client_Message, 	/* Request message from KVStore */
		ECS_Message, 		/* Message from ECS to initiate data transfer */
        Server_Message, 	/* Data transfer message from KVServer */
        Transaction_Message,/* Message from KVStore to initiate transaction */
        Unknown             /* Unknown message type, error */
	}

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    private KVServer server;

    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    /**
     * Constructs a new ClientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket, KVServer server) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.server = server;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while (isOpen) {
                try {
                    byte[] msgBytes = receiveBytes(input);
                    MessageType msgType = getMessageType(msgBytes);

                    if (msgType == MessageType.Client_Message) {
                        logger.info("Message type: Client_Message");
                        handleClientMessage(new Message(msgBytes));
                    } 
                    else if (msgType == MessageType.ECS_Message) {
                        logger.info("Message type: ECS_Message");
                        handleECSMessage(new ECSMessage(msgBytes));
                    } 
                    else if (msgType == MessageType.Server_Message) {
                        logger.info("Message type: Server_Message");
                        handleServerMessage(new ServerMessage(msgBytes));
                    } 
                    else if (msgType == MessageType.Transaction_Message) {
                        logger.info("Message type: Transaction_Message");
                        handleTransactionMessage(new TransactionMessage(msgBytes));
                    }
                    else {
                        logger.error("Unknown message type received");
                        sendFailedMessage("FAILED Message format unknown!");
                    }
                }
                catch (IllegalArgumentException iae) {
                    logger.error("Error! Unable to parse message", iae);
                }
                catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }
            }
        }
        catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        }
        catch (ClassNotFoundException e) {
            logger.error("Error! Class of object received not found!", e);
        }
        finally {
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            }
            catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    // Handle server data transfer message
    public void handleServerMessage(ServerMessage msg) { // Takes ServerMessage as input
        // TODO
        // Set server state
        ServerMessageStatus status = msg.getServerStatus();
        System.out.println("ServerMessage received: Transmitting KV pairs.");
        if(status == ServerMessage.ServerMessageStatus.SEND_KV){
            
            Map<String, String> pairs = msg.getPairs();
            for(String i:pairs.keySet()){
                System.out.println(i+"--------------------------------"+pairs.get(i));

            }
            this.server.receiveKV(pairs);
            ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.SEND_KV_ACK, "");
            sendServerMessage(reply, output);
            
        }
        
        else if (status == ServerMessage.ServerMessageStatus.SET_RUNNING){
            this.server.setStatus(ServerStatus.RUNNING);
            ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.SET_RUNNING_ACK, "");
            sendServerMessage(reply, output);
            this.isOpen=false;
        }
        else if(status==ServerMessage.ServerMessageStatus.REPLICATE_KV){
            Map<String, String> pairs = msg.getPairs();
            for(String i:pairs.keySet()){
                System.out.println(i+"--------------------------------"+pairs.get(i));

            }
            this.server.receiveKV(pairs);
            ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.REPLICATE_KV_ACK, "");
            sendServerMessage(reply, output);
        }
        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_GET){
            Map<String, String> pairs = msg.getPairs();
            try{
                for(String i:pairs.keySet()){
                    pairs.put(i, this.server.getKV(i));
                    
                }
                String reply_str = this.server.convert_map_string(pairs);
                ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.TRANSACTION_GET_ACK, reply_str);
                sendServerMessage(reply, output);
            }
            catch(Exception e){
                logger.error("Error! Unable to get pairs!", e);
            }
        }
        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_SEND_KV){
            Map<String, String> pairs = msg.getPairs();
            try{
                for(String i:pairs.keySet()){
                    this.server.transaction_map.put(i, pairs.get(i));
                    
                }
                ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.TRANSACTION_ACK, "");
                sendServerMessage(reply, output);
            }
            catch(Exception e){
                logger.error("Error! Unable to store pairs for transaction!", e);
            }
        }
        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_ABORT){
            this.server.transaction_map.clear();
            ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.TRANSACTION_ABORT_ACK, "");
            sendServerMessage(reply, output);
        }
        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_COMMIT){
            try{
                for(String i:this.server.transaction_map.keySet()){
                    this.server.putKV(i, this.server.transaction_map.get(i));
                    String keyHash = MD5Hasher.hash(i);
                    logger.info("keyHash is " + keyHash);
                    ECSNode current = server.getMetadataObj().findNode(keyHash);
                    ECSNode firstSuccessor = server.getMetadataObj().findSuccessor(current);
                    ECSNode secondSuccessor = server.getMetadataObj().findSuccessor(firstSuccessor);

                    boolean isResponsible = server.getHostname().equals(current.getNodeHost()) && server.getPort() == current.getNodePort();
                    boolean isReplicated1 = server.getHostname().equals(firstSuccessor.getNodeHost()) && server.getPort() == firstSuccessor.getNodePort();
                    boolean isReplicated2 = server.getHostname().equals(secondSuccessor.getNodeHost()) && server.getPort() == secondSuccessor.getNodePort();
                    if (!server.getHostname().equals(current.getNodeHost()) || server.getPort() != current.getNodePort()) {
                        KVMessage.StatusType trans_status = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
                        sendClientMessage(new Message(null, null, trans_status));
                        return;
                    }
                    // Check for write lock
                    if (server.getStatus() == IKVServer.ServerStatus.SERVER_WRITE_LOCK) {
                        KVMessage.StatusType trans_status = KVMessage.StatusType.SERVER_WRITE_LOCK;
                        sendClientMessage(new Message(null, null, trans_status));
                        return;
                    }
                    // Check if PUT, PUT_UPDATE or DELETE
                    boolean delete = false;
                    boolean update = false;
                    if (this.server.transaction_map.get(i).equals("null")) {
                        delete = true;
                    }
                    else {
                       
                        String value = server.getKV(i);
                        if (value != null) {
                            update = true;
                        }
                        
                        
                    }

                    
                    server.putKV(i,this.server.transaction_map.get(i));
                    KVMessage.StatusType trans_status = KVMessage.StatusType.PUT_SUCCESS;
                    if (delete) {
                        trans_status = KVMessage.StatusType.DELETE_SUCCESS;
                    }
                    else if (update) {
                        trans_status = KVMessage.StatusType.PUT_UPDATE;
                    }

                    sendClientMessage(new Message(
                        i, this.server.transaction_map.get(i),
                        trans_status));
                    if(!isReplicated1)
                    {
                        server.send_one_kv(firstSuccessor.getNodeHost(), firstSuccessor.getNodePort(), i,this.server.transaction_map.get(i));
                    }
                    if(!isReplicated2)
                    {
                        server.send_one_kv(secondSuccessor.getNodeHost(),secondSuccessor.getNodePort(), i,this.server.transaction_map.get(i));
                    }
                        
                    
            
                    
                }
            }
            catch(Exception e){
                logger.error("Error! Unable to commit changes for transaction!", e);
            }




            
            this.server.transaction_map.clear();
            ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.TRANSACTION_COMMIT_ACK, "");
            sendServerMessage(reply, output);
        }
        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_GET_REQ){

            Map<String, String> pairs = msg.getPairs();
            try{
                for(String i:pairs.keySet()){
                    if(this.server.transaction_map.get(i) == null)
                    {
                        if(this.server.getKV(i) == null)
                            pairs.put(i, "null");
                        else{
                            pairs.put(i,this.server.getKV(i));
                        }
                    }
                    else{
                        pairs.put(i, this.server.transaction_map.get(i));
                    }
                    
                    
                }
                String reply_str = this.server.convert_map_string(pairs);
                ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.TRANSACTION_GET_REQ_ACK, reply_str);
                sendServerMessage(reply, output);
            }
            catch(Exception e){
                logger.error("Error! Unable to get pairs!", e);
            }
            
            
            
        }
        
        
        


    }
    
    // Handle ECS message, start data transfer if needed
    public void handleECSMessage(ECSMessage msg) {
        System.out.println("ECSMessage received: " + msg.getStatus() + " " 
            + msg.getMetadata() + " " + msg.getServerToContact());

        String keyRangeMessage = msg.getMetadata();
        String listHostPorts = MD5Hasher.buildListOfPorts(keyRangeMessage);

        // Update metadata only if it's the first server
        if (msg.getStatus() == ECSMessage.ECSMessageStatus.NO_TRANSFER) {
            this.server.setMetadata(keyRangeMessage);
        }
        String serverToCon = msg.getServerToContact();
        
        // Reply to ECS server with ACK
        ECSMessage ecsReply = new ECSMessage(ECSMessage.ECSMessageStatus.ACK, null, null);
        sendECSMessage(ecsReply);

        // Process ECSMessage based on status
        if (msg.getStatus() == ECSMessage.ECSMessageStatus.TRANSFER) { 
            // try {
            // }
            // catch (IOException ioe) {
            //     logger.error("Error! Unable to send KV pairs to another server!", ioe);
            // }
            String hostPort = this.server.getHostname() + ":" + String.valueOf(server.getPort());
            String ServerPositionKey = MD5Hasher.hash(hostPort);
            Metadata newMetadata = new Metadata(MD5Hasher.buildListOfPorts(keyRangeMessage));
            ECSNode serverNode = newMetadata.findNode(ServerPositionKey);
            String[] key_range = serverNode.getNodeHashRange();
            // System.out.println("TRANSFER STATUS--------running transfer KV");
            if(this.server.getShuttingDown()){
                this.server.dumpKV(serverToCon);
            }
            else{
                this.server.transferKV(key_range, serverToCon);
            }
            
            this.server.updateZnodeMetadata(msg.getMetadata());

            if (this.server.getShuttingDown()) {
                server.setShutdownFinished(true);
            }
        }
        else if (msg.getStatus() == ECSMessage.ECSMessageStatus.RECEIVE) {
            // Update local metadata
            // Wait for data transfer from serverToContact

        }
        else if (msg.getStatus() == ECSMessage.ECSMessageStatus.NO_TRANSFER) {
            // If first server added, updated metadata, set state to RUNNING
            if (server.getShuttingDown() == false) {
                logger.info("First server added, setting state to RUNNING directly");
                server.setStatus(IKVServer.ServerStatus.RUNNING);
            }
            else { // If last server removed, keep shutting down
                logger.info("Last server removed, just shut down");
                // Should we delete the data on the last server?
                server.setShutdownFinished(true);
            }
        }

    }

    // Handle client request message
    public void handleClientMessage(Message msg) {
        logger.info("RECEIVE request from \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: {"
                + msg.getStatus() + ", <"
                + msg.getKey() + ", "
                + msg.getValue() + ">}");
        
        // Synchronize I/O to prevent concurrent access to K/V pairs
        synchronized (server) {
            // Logic to process message and to send back a response
            if (server.getStatus() == IKVServer.ServerStatus.SERVER_STOPPED) {
                // System.out.println("____________________"+server.getStatus());
                KVMessage.StatusType status = KVMessage.StatusType.SERVER_STOPPED;
                sendClientMessage(new Message(
                        null, null,
                        status));
            }
            else if (msg.getStatus() == KVMessage.StatusType.KEYRANGE) {
                try {
                    String hostPorts = server.getMetadataObj().buildListOfHostPorts();
                    String value = MD5Hasher.buildKeyRangeMessage(hostPorts);
                    KVMessage.StatusType status = KVMessage.StatusType.KEYRANGE_SUCESS;
                    sendClientMessage(new Message(
                            null, value,
                            status));
                }
                catch (Exception ex) {
                    logger.error("KEYRANGE ERROR", ex);
                    sendClientMessage(new Message(
                            null, null,
                            KVMessage.StatusType.GET_ERROR));
                }
                return;
            }
            else if (msg.getStatus() == KVMessage.StatusType.GET) {
                // Check if msg key is within total range
                String keyHash = MD5Hasher.hash(msg.getKey());
                logger.info("keyHash is " + keyHash);
                ECSNode current = server.getMetadataObj().findNode(keyHash);
                ECSNode firstSuccessor = server.getMetadataObj().findSuccessor(current);
                ECSNode secondSuccessor = server.getMetadataObj().findSuccessor(firstSuccessor);

                boolean isResponsible = server.getHostname().equals(current.getNodeHost()) && server.getPort() == current.getNodePort();
                boolean isReplicated1 = server.getHostname().equals(firstSuccessor.getNodeHost()) && server.getPort() == firstSuccessor.getNodePort();
                boolean isReplicated2 = server.getHostname().equals(secondSuccessor.getNodeHost()) && server.getPort() == secondSuccessor.getNodePort();

                if (!(isResponsible || isReplicated1 || isReplicated2)) {
                    KVMessage.StatusType status = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
                    sendClientMessage(new Message(null, null, status));
                    return;
                }

                try {
                    String value = server.getKV(msg.getKey());
                    KVMessage.StatusType status = KVMessage.StatusType.GET_SUCCESS;
                    if (value == null) {
                        status = KVMessage.StatusType.GET_ERROR;
                    }

                    sendClientMessage(new Message(
                        msg.getKey(), value,
                        status));
                }
                catch (Exception ex) {
                    logger.error("GET ERROR", ex);
                    sendClientMessage(new Message(
                        msg.getKey(), msg.getValue(),
                        KVMessage.StatusType.GET_ERROR));
                }
            }
            else if (msg.getStatus() == KVMessage.StatusType.PUT) {
                // Check if node is responsible for PUT
                String keyHash = MD5Hasher.hash(msg.getKey());
                logger.info("keyHash is " + keyHash);
                ECSNode current = server.getMetadataObj().findNode(keyHash);
                ECSNode firstSuccessor = server.getMetadataObj().findSuccessor(current);
                ECSNode secondSuccessor = server.getMetadataObj().findSuccessor(firstSuccessor);

                boolean isResponsible = server.getHostname().equals(current.getNodeHost()) && server.getPort() == current.getNodePort();
                boolean isReplicated1 = server.getHostname().equals(firstSuccessor.getNodeHost()) && server.getPort() == firstSuccessor.getNodePort();
                boolean isReplicated2 = server.getHostname().equals(secondSuccessor.getNodeHost()) && server.getPort() == secondSuccessor.getNodePort();
                if (!server.getHostname().equals(current.getNodeHost()) || server.getPort() != current.getNodePort()) {
                    KVMessage.StatusType status = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
                    sendClientMessage(new Message(null, null, status));
                    return;
                }
                // Check for write lock
                if (server.getStatus() == IKVServer.ServerStatus.SERVER_WRITE_LOCK) {
                    KVMessage.StatusType status = KVMessage.StatusType.SERVER_WRITE_LOCK;
                    sendClientMessage(new Message(null, null, status));
                    return;
                }
                // Check if PUT, PUT_UPDATE or DELETE
                boolean delete = false;
                boolean update = false;
                if (msg.getValue().equals("null")) {
                    delete = true;
                }
                else {
                    try {
                        String value = server.getKV(msg.getKey());
                        if (value != null) {
                            update = true;
                        }
                    }
                    catch (Exception ex) {
                        logger.error("Error getting value during put checking", ex);
                    }
                }

                try {
                    server.putKV(msg.getKey(), msg.getValue());
                    KVMessage.StatusType status = KVMessage.StatusType.PUT_SUCCESS;
                    if (delete) {
                        status = KVMessage.StatusType.DELETE_SUCCESS;
                    }
                    else if (update) {
                        status = KVMessage.StatusType.PUT_UPDATE;
                    }

                    sendClientMessage(new Message(
                        msg.getKey(), msg.getValue(),
                        status));
                    if(!isReplicated1)
                    {
                        server.send_one_kv(firstSuccessor.getNodeHost(), firstSuccessor.getNodePort(), msg.getKey(), msg.getValue());
                    }
                    if(!isReplicated2)
                    {
                        server.send_one_kv(secondSuccessor.getNodeHost(),secondSuccessor.getNodePort(), msg.getKey(), msg.getValue());
                    }
                    

                    
                }
                catch (Exception ex) {
                    KVMessage.StatusType errorStatus = KVMessage.StatusType.PUT_ERROR;
                    if (delete) {
                        errorStatus = KVMessage.StatusType.DELETE_ERROR;
                    }
                    logger.error("PUT ERROR", ex);
                    sendClientMessage(new Message(
                        msg.getKey(), msg.getValue(),
                        errorStatus));
                }
            }
        }
    }

    // Handle client transaction message
    public void handleTransactionMessage(TransactionMessage msg) {
        assert (msg.getStatus() == ITransactionMessage.TStatusType.TRANSACTION_PUT);

        // Build a map from kvPairsList, whose key is the server and value is a list of kvPairs handled by that server
        List<String[]> kvPairsList = msg.getKeyValuePairs();
        Map<String, List<String[]>> serverKVPairsMap = new HashMap<String, List<String[]>>();
        Metadata metadata = server.getMetadataObj();

        System.out.println("Building serverKVPairsMap");
        for (String[] kvPair : kvPairsList) {
            String key = kvPair[0];
            String value = kvPair[1];

            String keyHash = MD5Hasher.hash(key);
            ECSNode responsibleNode = metadata.findNode(keyHash);
            String hostPort = responsibleNode.getNodeHost() + ":" + responsibleNode.getNodePort();
            if (serverKVPairsMap.containsKey(hostPort)) {
                serverKVPairsMap.get(hostPort).add(kvPair);
            }
            else {
                List<String[]> kvPairs = new ArrayList<String[]>();
                kvPairs.add(kvPair);
                serverKVPairsMap.put(hostPort, kvPairs);
            }
        }

        // Print out serverKVPairsMap to check values
        for (String hostPort : serverKVPairsMap.keySet()) {
            System.out.println("Server: " + hostPort);
            List<String[]> kvPairs = serverKVPairsMap.get(hostPort);
            for (String[] kvPair : kvPairs) {
                System.out.println("key: " + kvPair[0] + ", value: " + kvPair[1]);
            }
        }

        // TRANSACTION_GET: get the initial values of keys in the map from the corresponding servers 
        // at the beginning of transaction
        // Server should reply with TRANSACTION_GET_ACK and kvPairs
        logger.info("[Transaction] Getting initial values of keys");
        Map<String, Map<String, String>> initialServerKVPairsMap = 
            transactionCommunication(serverKVPairsMap, ServerMessage.ServerMessageStatus.TRANSACTION_GET);

        // Sleep for 5 seconds to make it easier to test transaction abort
        try {
            Thread.sleep(5000);
        }
        catch (Exception ex) {
            logger.error("Error sleeping", ex);
        }

        // TRANSACTION_SEND_KV: send kv pairs in the map to the corresponding servers
        // Server should reply with TRANSACTION_ACK
        logger.info("[Transaction] Sending put requests to servers");
        transactionCommunication(serverKVPairsMap, ServerMessage.ServerMessageStatus.TRANSACTION_SEND_KV);

        // Get the values again and check if they have been changed by other clients
        logger.info("[Transaction] Getting values of keys again to check if they have been changed");
        Map<String, Map<String, String>> finalServerKVPairsMap = 
            transactionCommunication(serverKVPairsMap, ServerMessage.ServerMessageStatus.TRANSACTION_GET);

        // Check if the values have been changed
        boolean changed = false;
        for (String hostPort : finalServerKVPairsMap.keySet()) {
            Map<String, String> initialKVPairs = initialServerKVPairsMap.get(hostPort);
            Map<String, String> finalKVPairs = finalServerKVPairsMap.get(hostPort);
            for (String key : initialKVPairs.keySet()) {
                if (!initialKVPairs.get(key).equals(finalKVPairs.get(key))) {
                    changed = true;
                    break;
                }
            }
        }

        if (changed) {
            // Abort transaction and reply with TRANSACTION_PUT_FAILURE
            logger.info("[Transaction] Values have been changed by other clients, ABORT");
            transactionCommunication(serverKVPairsMap, ServerMessage.ServerMessageStatus.TRANSACTION_ABORT);
            sendTransactionMessage(new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT_FAILURE));
        }
        else {
            // Commit transaction and reply with TRANSACTION_PUT_SUCCESS
            logger.info("[Transaction] Values have not been changed, COMMIT");
            transactionCommunication(serverKVPairsMap, ServerMessage.ServerMessageStatus.TRANSACTION_COMMIT);
            sendTransactionMessage(new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS));
        }
    }

    // Send requests to servers in serverKVPairsMap and receive responses
    // status: TRANSACTION_GET, TRANSACTION_SEND_KV, TRANSACTION_COMMIT, TRANSACTION_ABORT
    // Use multi-threading to send requests concurrently
    // Return: for TRANSACTION_GET, return a map where key is the hostPort of the server 
    // and value is a map of key-value pairs; for other statuses, return null
    public Map<String, Map<String, String>> transactionCommunication
        (Map<String, List<String[]>> serverKVPairsMap, final ServerMessage.ServerMessageStatus status) {
        // For TRANSACTION_GET: get the current values of keys in the map from the corresponding servers
        final Map<String, Map<String, String>>[] currentServerKVPairsMap = 
            new ConcurrentHashMap[]{new ConcurrentHashMap<String, Map<String, String>>()}; // Use array to make it final
        // For counting the number of replies received
        final AtomicInteger[] sharedCounter = {new AtomicInteger(0)}; // Use array to make it final

        for (final String hostPort : serverKVPairsMap.keySet()) {
            String[] hostPortSplit = hostPort.split(":");
            final String host = hostPortSplit[0];
            final int port = Integer.parseInt(hostPortSplit[1]);
            final List<String[]> kvPairs = serverKVPairsMap.get(hostPort);
            
            if (!host.equals(server.getHostname()) || port != server.getPort()) {
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // Build a socket connection to KVServer at host:port
                        Socket transactionSocket = null;
                        OutputStream transactionOutput = null;
                        InputStream transactionInput = null;
                        try {
                            transactionSocket = new Socket(host, port);
                            transactionOutput = transactionSocket.getOutputStream();
                            transactionInput = transactionSocket.getInputStream();
                        } catch (UnknownHostException e) {
                            logger.error("Unknown host: " + e.getMessage(), e);
                        } catch (IOException e) {
                            logger.error("Error connecting to server: " + e.getMessage(), e);
                        }

                        // Build ServerMessage
                        String kvDelimiter = ";;;;;";
                        String kvPairDelimiter = "&&&&&";
                        String combinedKVPairs = "";
                        for (String[] kvPair : kvPairs) {
                            String key = kvPair[0];
                            String value = kvPair[1];
                            combinedKVPairs += key + kvPairDelimiter + value + kvDelimiter;
                        }

                        ServerMessage serverMsg = new ServerMessage(
                            status,
                            combinedKVPairs);

                        // Send ServerMessage to KVServer at host:port
                        sendServerMessage(serverMsg, transactionOutput);

                        // Receive ServerMessage reply
                        byte[] reply;
                        try {
                            reply = receiveBytes(transactionInput);
                        }
                        catch (IOException e) {
                            logger.error("Error receiving reply from server", e);
                            return;
                        }
                        catch (ClassNotFoundException e) {
                            logger.error("Error receiving reply from server", e);
                            return;
                        }
                        
                        ServerMessage replyMsg;
                        try {
                            replyMsg = new ServerMessage(reply);
                        } catch (IllegalArgumentException e) {
                            logger.error("Error parsing ServerMessage", e);
                            return;
                        }

                        if (status == ServerMessage.ServerMessageStatus.TRANSACTION_GET) {
                            assert (replyMsg.getServerStatus() == ServerMessage.ServerMessageStatus.TRANSACTION_GET_ACK);
                            // Store the map of key-value pairs in currentServerKVPairsMap
                            Map <String, String> replyKVPairs = replyMsg.getPairs();
                            currentServerKVPairsMap[0].put(hostPort, replyKVPairs);
                        }
                        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_SEND_KV) {
                            assert (replyMsg.getServerStatus() == ServerMessage.ServerMessageStatus.TRANSACTION_ACK);
                        }
                        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_COMMIT) {
                            assert (replyMsg.getServerStatus() == ServerMessage.ServerMessageStatus.TRANSACTION_COMMIT_ACK);
                        }
                        else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_ABORT) {
                            assert (replyMsg.getServerStatus() == ServerMessage.ServerMessageStatus.TRANSACTION_ABORT_ACK);
                        }
                        
                        sharedCounter[0].incrementAndGet(); // Increament the counter by 1 to indicate that this thread is done

                        // Close the socket connection
                        try {
                            transactionSocket.close();
                        } catch (IOException e) {
                            logger.error("Error closing socket", e);
                        }
                    }
                });

                thread.start();
            }
            else { // The server is the coordinator itself
                
                if (status == ServerMessage.ServerMessageStatus.TRANSACTION_GET) {
                    // Get values for kvPairs from the current server
                    Map<String, String> currentServerPairs = new ConcurrentHashMap<String, String>();
                    for (String[] kvPair : kvPairs) {
                        String key = kvPair[0];
                        String value = kvPair[1];
                        String currentValue = null;
                        try {
                            currentValue = server.getKV(key);
                        }
                        catch (Exception e) {
                            logger.error("Error getting value for key " + key, e);
                        }
                        
                        if (currentValue == null) {
                            currentValue = "null"; // Use the String "null" to represent null values
                        }
                        currentServerPairs.put(key, currentValue);
                    }
                    // Store the map of key-value pairs in currentServerKVPairsMap
                    currentServerKVPairsMap[0].put(hostPort, currentServerPairs);
                }
                else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_SEND_KV) {
                    Map<String, String> pairs = new HashMap<String, String>();

                    try {
                        for (String[] kvPair : kvPairs) {
                            String key = kvPair[0];
                            String value = kvPair[1];
                            this.server.transaction_map.put(key, value);
                        }
                    }
                    catch (Exception e) {
                        logger.error("Error stroing transaction pairs", e);
                    }
                    
                }
                else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_COMMIT) {
                    try {
                        for( String i:this.server.transaction_map.keySet()){
                            this.server.putKV(i, this.server.transaction_map.get(i));
                            String keyHash = MD5Hasher.hash(i);
                            logger.info("keyHash is " + keyHash);
                            ECSNode current = server.getMetadataObj().findNode(keyHash);
                            ECSNode firstSuccessor = server.getMetadataObj().findSuccessor(current);
                            ECSNode secondSuccessor = server.getMetadataObj().findSuccessor(firstSuccessor);
        
                            boolean isResponsible = server.getHostname().equals(current.getNodeHost()) && server.getPort() == current.getNodePort();
                            boolean isReplicated1 = server.getHostname().equals(firstSuccessor.getNodeHost()) && server.getPort() == firstSuccessor.getNodePort();
                            boolean isReplicated2 = server.getHostname().equals(secondSuccessor.getNodeHost()) && server.getPort() == secondSuccessor.getNodePort();
                            
                            // Check if PUT, PUT_UPDATE or DELETE
                            boolean delete = false;
                            boolean update = false;
                            if (this.server.transaction_map.get(i).equals("null")) {
                                delete = true;
                            }
                            else {
                               
                                String value = server.getKV(i);
                                if (value != null) {
                                    update = true;
                                }
                                
                                
                            }
                            server.putKV(i,this.server.transaction_map.get(i));
                            KVMessage.StatusType trans_status = KVMessage.StatusType.PUT_SUCCESS;
                            if (delete) {
                                trans_status = KVMessage.StatusType.DELETE_SUCCESS;
                            }
                            else if (update) {
                                trans_status = KVMessage.StatusType.PUT_UPDATE;
                            }
        
                            sendClientMessage(new Message(
                                i, this.server.transaction_map.get(i),
                                trans_status));
                            if(!isReplicated1)
                            {
                                server.send_one_kv(firstSuccessor.getNodeHost(), firstSuccessor.getNodePort(), i,this.server.transaction_map.get(i));
                            }
                            if(!isReplicated2)
                            {
                                server.send_one_kv(secondSuccessor.getNodeHost(),secondSuccessor.getNodePort(), i,this.server.transaction_map.get(i));
                            }
                                
                            
                    
                            
                        }
                    }
                    catch(Exception e){
                        logger.error("Error! Unable to commit changes for transaction!", e);
                    }
                    this.server.transaction_map.clear();
                }
                else if (status == ServerMessage.ServerMessageStatus.TRANSACTION_ABORT) {
                    this.server.transaction_map.clear();
                }
                else {
                    logger.error("Invalid status for transactionCommunication");
                }
                
                sharedCounter[0].incrementAndGet(); // Increament the counter by 1 to indicate that this thread is done
            }

        }

        // Wait for all threads to finish
        while (sharedCounter[0].get() < serverKVPairsMap.size()) {
            // System.out.println("Replies received: " + sharedCounter[0].get() + "/" + serverKVPairsMap.size());
            try {
                Thread.sleep(1000); // Can reduce it later
            } catch (InterruptedException e) {
                logger.error("Error sleeping", e);
            }
        }

        if (status == ServerMessage.ServerMessageStatus.TRANSACTION_GET) {
            return currentServerKVPairsMap[0];
        }
        else {
            return null;
        }
    }

    // Get the type of message received
    public MessageType getMessageType(byte[] msgBytes) {

        try {
            Message msg = new Message(msgBytes);
            return MessageType.Client_Message;
        }
        catch (IllegalArgumentException iae) {
        }

        try {
            ECSMessage msg = new ECSMessage(msgBytes);
            return MessageType.ECS_Message;
        }
        catch (IllegalArgumentException iae) {
        }

        try {
            ServerMessage msg = new ServerMessage(msgBytes);
            return MessageType.Server_Message;
        }
        catch (IllegalArgumentException iae) {
        }

        try {
            TransactionMessage msg = new TransactionMessage(msgBytes);
            return MessageType.Transaction_Message;
        }
        catch (IllegalArgumentException iae) {
        }

        return MessageType.Unknown;
    }

    /**
     * Method sends a Message object using this socket,
     * containing response status, key and value.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendClientMessage(Message msg) {
        try {
            logger.info("KVServer: sending response to \t<"
                    + clientSocket.getInetAddress().getHostAddress() + ":"
                    + clientSocket.getPort() + ">: {"
                    + msg.getStatus() + ", <"
                    + msg.getKey() + ", "
                    + msg.getValue() + ">}");
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending message: " + e.getMessage(), e);
        }
    }
    public void sendServerMessage(ServerMessage msg, OutputStream output){
        try {
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending Server message: " + e.getMessage(), e);
        }
    }
    

    public void sendECSMessage(ECSMessage msg) {
        try {
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending ECS message: " + e.getMessage(), e);
        }
    }

    public void sendTransactionMessage(TransactionMessage msg) {
        try {
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending Transaction message: " + e.getMessage(), e);
        }
    }


    // Read bytes from input stream
    private byte[] receiveBytes(InputStream input) throws IOException, ClassNotFoundException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and numbers */
            if((read >= 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
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

        return msgBytes;
    }

    /**
     * Method sends a Message object using this socket,
     * containing response status, key and value.
     * @param description of the error.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendFailedMessage(String errorDescription) throws IOException {
        try {
            byte[] msgBytes = toByteArray(errorDescription);
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending message: " + e.getMessage(), e);
            throw new IOException("Error sending message: " + e.getMessage());
        }
    }

    private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}

}
