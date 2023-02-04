package client;

import shared.messages.KVMessage;
import shared.messages.Message;
import shared.module.ClientCommunication;

import java.io.IOException;
import java.net.UnknownHostException;

public class KVStore implements KVCommInterface {
	private ClientCommunication clientCommunicationModule;
	private String serverAdress;
	private int serverPort;
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.serverAdress = address;
		this.serverPort = port;

	}

	@Override
	public void connect() throws UnknownHostException {
		// TODO Auto-generated method stub
		if (this.clientCommunicationModule != null) {
			this.clientCommunicationModule.closeConnection();
			return;
		}
		try {
			clientCommunicationModule = new ClientCommunication(serverAdress, serverPort);
		} catch (IOException e) {
			// Handle the exception here
			throw new UnknownHostException("Unknown host: " + e.getMessage());
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if (this.clientCommunicationModule != null) {
			this.clientCommunicationModule.closeConnection();
			this.clientCommunicationModule = null;
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		Message putRequest = new Message(key, value, KVMessage.StatusType.PUT);
		this.clientCommunicationModule.sendMessage(putRequest);
		Message response = this.clientCommunicationModule.receiveMessage();
		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		Message getRequest = new Message(key, null, KVMessage.StatusType.GET);
		this.clientCommunicationModule.sendMessage(getRequest);
		Message response = this.clientCommunicationModule.receiveMessage();
		return response;
	}
}
