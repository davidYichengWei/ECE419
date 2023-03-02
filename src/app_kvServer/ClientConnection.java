package app_kvServer;

import java.io.*;
import java.net.Socket;

import org.apache.log4j.*;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.ECSMessage;


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
                        handleServerMessage();
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
    public void handleServerMessage() { // Takes ServerMessage as input
        // TODO
    }

    // Handle ECS message, start data transfer if needed
    public void handleECSMessage(ECSMessage msg) {
        System.out.println("ECSMessage received: " + msg.getStatus() + " " 
            + msg.getMetadata() + " " + msg.getServerToContact());
        
        // Reply to ECS server with TRANSFER_ACK
        ECSMessage ecsReply = new ECSMessage(ECSMessage.ECSMessageStatus.TRANSFER_ACK, null, null);
        sendECSMessage(ecsReply);

        // Begin transfer of data
        // TODO
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
        synchronized (this) {
            // Logic to process message and to send back a response
            if (msg.getStatus() == KVMessage.StatusType.GET) {
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
