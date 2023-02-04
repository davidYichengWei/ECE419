package shared.module;

import org.apache.log4j.Logger;
import shared.messages.Message;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

public class ClientCommunication implements Communication {

    private Logger logger = Logger.getRootLogger();
    private Socket clientSocket;
    private ObjectInputStream input;
    private ObjectOutputStream output;

    public ClientCommunication(String address, int port) throws IOException {
        try {
            this.clientSocket = new Socket(address, port);
            output = new ObjectOutputStream(clientSocket.getOutputStream());
            input = new ObjectInputStream(clientSocket.getInputStream());
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
            output.writeObject(msg);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending message: " + e.getMessage(), e);
            throw new IOException("Error sending message: " + e.getMessage());
        }
    }

    @Override
    public Message receiveMessage() throws IOException {
        try {
            Message message = (Message) input.readObject();
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
