package client;

import ecs.ECSNode;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.Metadata;
import shared.module.ClientCommunication;
import shared.module.MD5Hasher;

import java.io.IOException;
import java.net.UnknownHostException;

public class KVStore implements KVCommInterface {
	private ClientCommunication clientCommunicationModule;
	private String serverAdress;
	private int serverPort;
	private Metadata metadata;
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.serverAdress = address;
		this.serverPort = port;

		// Initialize metadata with single server
		String hostPort = this.serverAdress + ":" + this.serverPort;
		this.metadata = new Metadata(hostPort);

	}

	public int getServerPort() {
		return serverPort;
	}

	public String getServerAdress() {
		return serverAdress;
	}

	public void setServerAdress(String serverAdress) {
		this.serverAdress = serverAdress;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	@Override
	public void connect() throws UnknownHostException {
		// TODO Auto-generated method stub
		if (this.clientCommunicationModule != null) {
			this.clientCommunicationModule.closeConnection();
			this.clientCommunicationModule = null;
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
		boolean requestSuccessful = false;
		String HashKey = MD5Hasher.hash(key);
		Message putRequest = new Message(key, value, KVMessage.StatusType.PUT);
		Message response = null;

		while(!requestSuccessful) {
			//Find responsible server
			ECSNode responsibleNode = metadata.findNode(HashKey);
			if (responsibleNode.getNodeHost() != this.getServerAdress() || responsibleNode.getNodePort() != this.getServerPort() ) {
				String ip = responsibleNode.getNodeHost();
				int port = responsibleNode.getNodePort();
				this.setServerAdress(ip);
				this.setServerPort(port);
				this.connect();
			}
			this.clientCommunicationModule.sendMessage(putRequest);
			response = this.clientCommunicationModule.receiveMessage();

			// If server not responsible, update Metadata
			if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				KVMessage keyValueResponse = this.getKeyRange();
				if (keyValueResponse.getStatus() == KVMessage.StatusType.SERVER_STOPPED) return keyValueResponse;
				String keyRanges = keyValueResponse.getValue();
				String newListPorts = MD5Hasher.buildListOfPorts(keyRanges);
				metadata = new Metadata(newListPorts);
			}
			else {
				requestSuccessful = true;
			}
		}


		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		boolean requestSuccessful = false;
		String HashKey = MD5Hasher.hash(key);
		Message getRequest = new Message(key, null, KVMessage.StatusType.GET);
		Message response = null;

		while(!requestSuccessful) {
			//Find responsible server
			ECSNode responsibleNode = metadata.findNode(HashKey);
			if (responsibleNode.getNodeHost() != this.getServerAdress() || responsibleNode.getNodePort() != this.getServerPort() ) {
				String ip = responsibleNode.getNodeHost();
				int port = responsibleNode.getNodePort();
				this.setServerAdress(ip);
				this.setServerPort(port);
				this.connect();
			}
			this.clientCommunicationModule.sendMessage(getRequest);
			response = this.clientCommunicationModule.receiveMessage();

			// If server not responsible, update Metadata
			if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				KVMessage keyValueResponse = this.getKeyRange();
				if (keyValueResponse.getStatus() == KVMessage.StatusType.SERVER_STOPPED) return keyValueResponse;
				String keyRanges = keyValueResponse.getValue();
				String newListPorts = MD5Hasher.buildListOfPorts(keyRanges);
				metadata = new Metadata(newListPorts);
			}
			else {
				requestSuccessful = true;
			}
		}

		return response;
	}

	public  KVMessage getKeyRange() throws Exception {
		Message keyRangeRequest = new Message(null, null, KVMessage.StatusType.KEYRANGE);
		this.clientCommunicationModule.sendMessage(keyRangeRequest);
		Message response = this.clientCommunicationModule.receiveMessage();
		return response;
	}
}
