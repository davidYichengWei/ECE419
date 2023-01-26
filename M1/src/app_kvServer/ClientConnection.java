package app_kvServer;

import java.io.*;
import java.net.Socket;

import org.apache.log4j.*;
import shared.messages.KVMessage;
import shared.messages.Message;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for handling client requests
 * by calling methods in KVServer.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private ObjectInputStream input;
    private ObjectOutputStream output;

    /**
     * Constructs a new ClientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = new ObjectOutputStream(clientSocket.getOutputStream());
            input = new ObjectInputStream(clientSocket.getInputStream());

            while (isOpen) {
                try {
                    Message latestMessage = receiveMessage();

                    // Logic to process message and to send back a response
                    if (latestMessage.getStatus() == KVMessage.StatusType.GET) {
                        sendMessage(new Message(
                                latestMessage.getKey(), latestMessage.getValue(),
                                KVMessage.StatusType.GET_SUCCESS));
                    }
                    else if (latestMessage.getStatus() == KVMessage.StatusType.PUT) {
                        sendMessage(new Message(
                                latestMessage.getKey(), latestMessage.getValue(),
                                KVMessage.StatusType.PUT_SUCCESS));
                    }
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

    /**
     * Method sends a Message object using this socket,
     * containing response status, key and value.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(Message msg) throws IOException {
        output.writeObject(msg);
        output.flush();
        logger.info("SEND response to \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: {"
                + msg.getStatus() + ", <"
                + msg.getKey() + ", "
                + msg.getValue() + ">}");
    }

    /**
     * Reads a Message object sent by the client.
     * @throws IOException some I/O error regarding the output stream
     * @throws ClassNotFoundException definition of the object received not found
     */
    private Message receiveMessage() throws IOException, ClassNotFoundException {
        Message msg = (Message) input.readObject();
        logger.info("RECEIVE request from \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: {"
                + msg.getStatus() + ", <"
                + msg.getKey() + ", "
                + msg.getValue() + ">}");

        return msg;
    }


}
