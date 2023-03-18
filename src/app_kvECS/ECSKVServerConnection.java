package app_kvECS;

import org.apache.log4j.Logger;
import shared.messages.ECSMessage;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Represents a connection to a KVServer from the ECS to send metadata for data transfer.
 */
public class ECSKVServerConnection implements Runnable {
    
    private static Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private String host;
    private int port;
    private ECSMessage message;
    private ECSClient ecs;

    private Socket socket;
    private InputStream input;
    private OutputStream output;

    public ECSKVServerConnection(String host, int port, ECSMessage message, ECSClient ecs) {
        this.host = host;
        this.port = port;
        this.message = message;
        this.ecs = ecs;
    }

    public void run() {
        // Build a socket connection to KVServer at host:port
        try {
            socket = new Socket(host, port);
            output = socket.getOutputStream();
            input = socket.getInputStream();
        } catch (UnknownHostException e) {
            logger.error("Unknown host: " + e.getMessage(), e);
        } catch (IOException e) {
            logger.error("Error connecting to server: " + e.getMessage(), e);

            // Server crashed, the ECS needs to update metadata in znode
            logger.info("Updating metadata on the ECS to begin reconsiliation");
            ecs.setZkData(ecs.getMetadataPath(), message.getMetadata());
        }

        // Send the ECSMessage to the KVServer
        try {
            sendMessage(message);
        } catch(IOException ioe) {
            logger.error("Unable to send message to KVServer!");
        }

        System.out.println("Message sent, waiting for response");

        // Receive the response from the KVServer
        try {
            ECSMessage response = receiveMessage();
            System.out.println("Response received: " + response.getStatus());
        } catch (IOException ioe) {
            logger.error("Unable to receive message from KVServer!");
        } catch (ClassNotFoundException cnfe) {
            logger.error("Unable to find class definition for received message!");
        } catch (IllegalArgumentException iae) {
            logger.error("Unable to parse received message!");
        }

        // Close the connection
        try {
            tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    public void tearDownConnection() throws IOException {
        logger.info("tearing down the connection with " + host + ":" + port);
        if (socket != null) {
            socket.close();
            socket = null;
            logger.info("connection closed!");
        }
    }

    /**
     * Method sends a Message object using this socket,
     * containing response status, key and value.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(ECSMessage msg) throws IOException {
        try {
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
            System.out.println("Sent message to " + host + ":" + port);
            System.out.println("Content: " + msg.getStatus() + " " + msg.getMetadata() + " " + msg.getServerToContact());
        } catch (IOException e) {
            logger.error("Error sending message: " + e.getMessage(), e);
            throw new IOException("Error sending message: " + e.getMessage());
        }
    }

    /**
     * Reads a Message object sent by the client.
     * @throws IOException some I/O error regarding the output stream
     * @throws ClassNotFoundException definition of the object received not found
     */
    private ECSMessage receiveMessage() throws IOException, ClassNotFoundException, IllegalArgumentException {
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

        /* build final String */
        ECSMessage msg = new ECSMessage(msgBytes);
        logger.info("RECEIVE message from \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: {"
                + msg.getStatus() + "}");

        return msg;
    }
}
