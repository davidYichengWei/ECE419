package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import ecs.ECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.Metadata;
import shared.messages.TransactionMessage;
import shared.module.MD5Hasher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private boolean stop = false;
    private BufferedReader stdin;
    private String serverAddress;
    private int serverPort;
    private KVStore keyValueStore;
    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
        if (keyValueStore != null) keyValueStore.disconnect();
        keyValueStore = new KVStore(hostname, port);
        keyValueStore.connect();
    }
    private void disconnect() {
        if (keyValueStore != null) {
            keyValueStore.disconnect();
            keyValueStore = null;
        }

    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return keyValueStore;
    }

    public void run() {
        Thread shutdownHook = new Thread() {
            @Override
            public void run() {
                shutDown();
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                if (cmdLine != null) {
                    this.handleCommand(cmdLine);
                }
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            this.disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (Exception e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("disconnect")) {
            this.disconnect();
        } else if (tokens[0].equals("put")) {
            if (keyValueStore == null) {
                printError("Not connected to any server!");
            }
            else if(tokens.length >= 3) {
                try{
                    String key = tokens[1];
                    String value = cmdLine.substring(cmdLine.indexOf(key) + key.length() + 1);
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                    if (keyBytes.length > 20 || valueBytes.length > 120 * 1024) {
                        printError("Key must be a max length of 20 bytes and value must be a max length of 120 kiloBytes.");
                    } else {
                        boolean serverUp = true;
                        try {
                            Message response = (Message) keyValueStore.put(key, value);
                            if (response == null) {
                                serverUp = false;
                            } else {
                                value = response.getValue();
                                System.out.println("Received Response: {" + response.getStatus() + ", <" + key + ", " + value + ">}");
                            }
                        } catch (Exception e) {
                            serverUp = false;
                            value = null;
                        }
                        if (!serverUp) {
                            System.out.println("Failed to receive response from KVServer, attempting to contact other KVServer in cached metadata");
                            this.retryKVServerRequest("put", key, value);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("get")) {
            if (keyValueStore == null) {
                printError("Not connected to any server!");
            }
            else if (tokens.length == 2) {
                String key = tokens[1];
                String value;
                boolean serverUp = true;
                try {
                    Message response = (Message) keyValueStore.get(key);
                    if (response == null) {
                        serverUp = false;
                    } else {
                        value = response.getValue();
                        System.out.println("Received Response: {" + response.getStatus() + ", <" + key + ", " + value + ">}");
                    }
                } catch (Exception e) {
                    serverUp = false;
                    value = null;
                }
                if (!serverUp) {
                    System.out.println("Failed to receive response from KVServer, attempting to contact other KVServer in cached metadata");
                    this.retryKVServerRequest("get", key, null);
                }
            } else {
                this.printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("keyrange")) {
            if (keyValueStore == null) {
                printError("Not connected to any server!");
            }
            else if (tokens.length == 1) {
                String value;
                try {
                    Message response = (Message) keyValueStore.getKeyRange();
                    if (response == null) {
                    } else if (response.getStatus() == KVMessage.StatusType.SERVER_STOPPED) {
                        printError("Server Stopped!");
                    } else {
                        value = response.getValue();
                        String newListPorts = MD5Hasher.buildListOfPorts(value);
                        Metadata newMetadata = new Metadata(newListPorts);
                        keyValueStore.setMetadata(newMetadata);
                        System.out.println("Received Response, updating metadata: {" + response.getStatus() + ", <" + value + ">}");
                    }
                } catch (Exception e) {
                    printError("Error occurred in keyrange request");
                }
            } else {
                this.printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = this.setLevel(tokens[1]);
                if (level.equals("UnknownLevel")) {
                    this.printError("No valid log level!");
                    this.printPossibleLogLevels();
                } else {
                    System.out.println("KVClient> Log level changed to level " + level);
                }
            } else {
                this.printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("tput")){
            System.out.println("Please enter key value pairs, type <tput confirm> to send request, <tput cancel> to cancel:");
            this.getInTransactionPut();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    public void getInTransactionPut() {
        boolean done = false;
        boolean validInput = true;
        List<String[]> keyValuePairs = new ArrayList<>();
        while(!done) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                String[] tokens = cmdLine.split("\\s+");
                if (tokens.length < 2) {
                    validInput = false;
                    done = true;
                    this.printError("Invalid number of arguments please print key value pairs one at a time. Transaction Canceled");
                    break;
                }
                if (tokens[0].equals("tput") && tokens[1].equals("cancel")) {
                    validInput = false;
                    done = true;
                    System.out.println("Transaction cancelled");
                    break;
                }
                if (tokens[0].equals("tput") && tokens[1].equals("confirm")) {
                    done = true;
                }
                else {
                    String key = tokens[0];
                    String value = cmdLine.substring(cmdLine.indexOf(key) + key.length() + 1);
                    String[] pair = new String[]{key, value};
                    keyValuePairs.add(pair);
                }
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
        if (validInput) {
            try {
                TransactionMessage response = keyValueStore.putTransaction(keyValuePairs);
                System.out.println("Received Response: " + response.getStatus().toString());
            } catch (Exception e) {
                printError("Error occurred in transaction request");
            }
        }
    }

    private void retryKVServerRequest(String method, String key, String value) {
        // Remove current connection from metadata
        serverAddress = keyValueStore.getServerAdress();
        serverPort = keyValueStore.getServerPort();
        String hostPort = serverAddress + ":" + String.valueOf(serverPort);
        String serverPositionKey = MD5Hasher.hash(hostPort);
        ECSNode search = keyValueStore.getMetadata().findNode(serverPositionKey);
        keyValueStore.getMetadata().removeNode(search);

        boolean connectionSuccessful = false;
        for (ECSNode node :  keyValueStore.getMetadata().getTree()) {
            try{
                serverAddress = node.getNodeHost();
                serverPort = node.getNodePort();
                System.out.println("Attempting to connect to " + serverAddress + ":" + serverPort);
                newConnection(serverAddress, serverPort);
                connectionSuccessful = true;
                break;
            } catch(NumberFormatException nfe) {
                printError("No valid address. Port must be a number!");
                logger.info("Unable to parse argument <port>", nfe);
                keyValueStore.getMetadata().removeNode(node);
            } catch (UnknownHostException e) {
                printError("Unknown Host!");
                logger.info("Unknown Host!", e);
                keyValueStore.getMetadata().removeNode(node);
            } catch (Exception e) {
                printError("Could not establish connection!");
                logger.warn("Could not establish connection!", e);
                keyValueStore.getMetadata().removeNode(node);
            }
        }
        if (!connectionSuccessful) {
            System.out.println("Failed to contact other KVServer(s) in cached metadata");
            disconnect();
            return;
        }

        // Issue request again
        if (method.equals("put")) {
            boolean serverUp = false;
            try {
                Message response = (Message) keyValueStore.put(key, value);
                if (response == null) {
                    serverUp = false;
                } else {
                    value = response.getValue();
                    System.out.println("Received Response: {" + response.getStatus() + ", <" + key + ", " + value + ">}");
                }
            } catch (Exception e) {
                serverUp = false;
                value = null;
            }
            if (!serverUp) {
                System.out.println("Failed to receive response from KVServer, attempting to contact other KVServer in cached metadata");
                this.retryKVServerRequest("put", key, value);
            }
        }
        else if (method.equals("get")) {
            boolean serverUp = true;
            try {
                Message response = (Message) keyValueStore.get(key);
                if (response == null) {
                    serverUp = false;
                } else {
                    value = response.getValue();
                    System.out.println("Received Response: {" + response.getStatus() + ", <" + key + ", " + value + ">}");
                }
            } catch (Exception e) {
                serverUp = false;
                value = null;
            }
            if (!serverUp) {
                System.out.println("Failed to receive response from KVServer, attempting to contact other KVServer in cached metadata");
                this.retryKVServerRequest("get", key, null);
            }
        }
    }

    private void printPossibleLogLevels() {
        System.out.println("KVClient> Possible log levels are:");
        System.out.println("KVClient> ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return "UnknownLevel";
        }
    }


    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t put <key> <value> pair to the server \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t get the <value> of <key> from the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }


    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient app = new KVClient();

            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void shutDown() {
        stop = true;
        this.disconnect();
    }
}
