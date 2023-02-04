package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "EchoClient> ";
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
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
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
            else if(tokens.length == 3) {
                try{
                    String key = tokens[1];
                    String value = cmdLine.substring(cmdLine.indexOf(key) + key.length() + 1);
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                    if (keyBytes.length > 20 || valueBytes.length > 120 * 1024) {
                        printError("Key must be a max length of 20 bytes and value must be a max length of 120 kiloBytes.");
                    } else {
                        Message response = (Message) keyValueStore.put(key, value);
                        System.out.print(response.getStatus() + ": " + key + ":" + value);
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
                try {
                    Message response = (Message) keyValueStore.get(key);
                    System.out.print(response.getStatus() + ": " + response.getValue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
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
                    System.out.println("EchoClient> Log level changed to level " + level);
                }
            } else {
                this.printError("Invalid number of parameters!");
            }
        } else {
            printError("Unknown command");
            printHelp();
        }
    }
    private void printPossibleLogLevels() {
        System.out.println("EchoClient> Possible log levels are:");
        System.out.println("EchoClient> ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
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
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
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
}
