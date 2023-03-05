package app_kvServer;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import ecs.ECSNode;
import org.apache.log4j.*;

import app_kvServer.IKVServer.ServerStatus;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.ECSMessage;
import shared.module.MD5Hasher;
import shared.messages.ServerMessage;
import shared.messages.ServerMessage.ServerMessageStatus;

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
                    byte[] msgBytes = receiveBytes();
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
                    else {
                        logger.error("Unknown message type received");
                    }
                }
                catch (IllegalArgumentException iae) {
                    logger.error("Error! Unable to parse message", iae);
                    sendFailedMessage("FAILED Message format unknown!");
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
            this.server.receiveKV(pairs);
            ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.SEND_KV_ACK, "");
            sendServerMessage(reply);
            
        }
        
        else if (status == ServerMessage.ServerMessageStatus.SET_RUNNING){
            this.server.setStatus(ServerStatus.RUNNING);
            ServerMessage reply = new ServerMessage(ServerMessage.ServerMessageStatus.SET_RUNNING_ACK, "");
            sendServerMessage(reply);
            this.isOpen=false;
        }


    }
    
    // Handle ECS message, start data transfer if needed
    public void handleECSMessage(ECSMessage msg) {
        System.out.println("ECSMessage received: " + msg.getStatus() + " " 
            + msg.getMetadata() + " " + msg.getServerToContact());

        String keyRangeMessage = msg.getMetadata();
        String listHostPorts = MD5Hasher.buildListOfPorts(keyRangeMessage);
        this.server.setMetadata(listHostPorts);
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
            String hostPort = this.server.getHostname();
            String ServerPositionKey = MD5Hasher.hash(hostPort);
            ECSNode serverNode = this.server.getMetadataObj().findNode(ServerPositionKey);
            String[] key_range = serverNode.getNodeHashRange();
            this.server.transferKV(key_range, serverToCon);
            this.server.updateZnodeMetadata(msg.getMetadata());
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
                System.out.println("____________________"+server.getStatus());
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
            String keyHash = MD5Hasher.hash(msg.getKey());
            ECSNode current = server.getMetadataObj().findNode(keyHash);
            if (!server.getHostname().equals(current.getNodeHost()) || server.getPort() != current.getNodePort()) {
                KVMessage.StatusType status = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
                sendClientMessage(new Message(
                        null, null,
                        status));
            }
            else if (msg.getStatus() == KVMessage.StatusType.GET) {
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
                // Check for write lock
                if (server.getStatus() == IKVServer.ServerStatus.SERVER_WRITE_LOCK) {
                    KVMessage.StatusType status = KVMessage.StatusType.SERVER_WRITE_LOCK;
                    sendClientMessage(new Message(
                            null, null,
                            status));
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

    // Get the type of message received
    public MessageType getMessageType(byte[] msgBytes) {
        try {
            Message msg = new Message(msgBytes);
            return MessageType.Client_Message;
        }
        catch (IllegalArgumentException iae) {
            try {
                ECSMessage msg = new ECSMessage(msgBytes);
                return MessageType.ECS_Message;
            }
            catch (IllegalArgumentException iae2) {
                return MessageType.Server_Message;
            }
        }
    }

    /**
     * Method sends a Message object using this socket,
     * containing response status, key and value.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendClientMessage(Message msg) {
        try {
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending message: " + e.getMessage(), e);
        }
    }
    public void sendServerMessage(ServerMessage msg){
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


    // Read bytes from input stream
    private byte[] receiveBytes() throws IOException, ClassNotFoundException {
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
