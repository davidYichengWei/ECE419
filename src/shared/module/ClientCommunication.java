package shared.module;

import org.apache.log4j.Logger;
import shared.messages.Message;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

public class ClientCommunication implements Communication {

    private Logger logger = Logger.getRootLogger();
    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    public ClientCommunication(String address, int port) throws IOException {
        try {
            this.clientSocket = new Socket(address, port);
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            logger.info("Connection established");
        } catch (UnknownHostException e) {
            logger.error("Unknown host: " + e.getMessage(), e);
            throw new IOException("Unknown host: " + e.getMessage());
        }
    }
    public synchronized void closeConnection() {
        logger.info("try to close connection ...");

        try {
            tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }
    private void tearDownConnection() throws IOException {
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            clientSocket.close();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

    @Override
    public void sendMessage(Message msg) throws IOException {
        try {
            byte[] msgBytes = msg.toByteArray();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending message: " + e.getMessage(), e);
            throw new IOException("Error sending message: " + e.getMessage());
        }
    }

    @Override
    public Message receiveMessage() throws IOException {

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
        Message msg = new Message(msgBytes);
        return msg;
    }
}
