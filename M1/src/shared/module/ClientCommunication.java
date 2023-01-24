package shared.module;

import org.apache.log4j.Logger;
import shared.messages.Message;

import java.io.*;
import java.net.Socket;

public class ClientCommunication implements Communication {

    private Logger logger = Logger.getRootLogger();
    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;

    public ClientCommunication(String address, int port) throws IOException {
        try {
            this.clientSocket = new Socket(address, port);
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            logger.info("Connection established");
        } catch (IOException e) {
            logger.error("Error connecting to server: " + e.getMessage(), e);
            throw new IOException("Error connecting to server: " + e.getMessage());
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
            ObjectOutputStream out = new ObjectOutputStream(output);
            out.writeObject(msg);
        } catch (IOException e) {
            logger.error("Error sending message: " + e.getMessage(), e);
            throw new IOException("Error sending message: " + e.getMessage());
        }
    }

    @Override
    public Message receiveMessage() throws IOException {
        try {
            ObjectInputStream in = new ObjectInputStream(input);
            Message message = (Message) in.readObject();
            return message;
        } catch (IOException e) {
            logger.error("Error receiving message: " + e.getMessage(), e);
            throw new IOException("Error receiving message: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.debug("Error loading class: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
