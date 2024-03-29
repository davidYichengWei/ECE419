package client;

import ecs.ECSNode;
import shared.messages.*;
import shared.module.ClientCommunication;
import shared.module.MD5Hasher;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

public class KVStore implements KVCommInterface {
	private ClientCommunication clientCommunicationModule;
	private String serverAdress;
	private int serverPort;
	private Metadata metadata;

	private boolean manuallyDisconnected = false; // To avoid automatically connecting when manually disconnected

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

	public Metadata getMetadata() {
		return metadata;
	}

	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
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

		manuallyDisconnected = false;
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if (this.clientCommunicationModule != null) {
			this.clientCommunicationModule.closeConnection();
			this.clientCommunicationModule = null;
		}

		manuallyDisconnected = true;
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {

		if (manuallyDisconnected) {
			throw new Exception("Client is manually disconnected");
		}

		// TODO Auto-generated method stub
		boolean requestSuccessful = false;
		String HashKey = MD5Hasher.hash(key);
		Message putRequest = new Message(key, value, KVMessage.StatusType.PUT);
		Message response = null;

		while(!requestSuccessful) {
			//Find responsible server
			ECSNode serverToContact = metadata.findNode(HashKey);

			if (serverToContact.getNodeHost() != this.getServerAdress() || serverToContact.getNodePort() != this.getServerPort() ) {
				String ip = serverToContact.getNodeHost();
				int port = serverToContact.getNodePort();
				this.setServerAdress(ip);
				this.setServerPort(port);
				this.connect();
			}
			this.clientCommunicationModule.sendMessage(putRequest);
			try {
				response = this.clientCommunicationModule.receiveMessage();
			} catch (IOException e) {
				// handle exception
				System.err.println("Error receiving message: " + e.getMessage());
				throw e;
			}

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

	public TransactionMessage putTransaction(List<String[]> keyValuePairs) throws Exception {
		if (manuallyDisconnected) {
			throw new Exception("Client is manually disconnected");
		}
		TransactionMessage putTransactionRequest = new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT);
		for (String[] keyValuePair:keyValuePairs) {
			String key = keyValuePair[0];
			String value = keyValuePair[1];
			putTransactionRequest.addKeyValuePair(key,value);
		}
		this.clientCommunicationModule.sendTransactionMessage(putTransactionRequest);
		TransactionMessage response = this.clientCommunicationModule.receiveTransactionMessage();
		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		
		if (manuallyDisconnected) {
			throw new Exception("Client is manually disconnected");
		}

		// TODO Auto-generated method stub
		boolean requestSuccessful = false;
		String HashKey = MD5Hasher.hash(key);
		Message getRequest = new Message(key, null, KVMessage.StatusType.GET);
		Message response = null;

		while(!requestSuccessful) {
			//Find responsible server
			ECSNode responsibleNode = metadata.findNode(HashKey);
			ECSNode firstSuccessor = metadata.findSuccessor(responsibleNode);
			ECSNode secondSuccessor = metadata.findSuccessor(firstSuccessor);
			ECSNode[] nodes = {responsibleNode, firstSuccessor, secondSuccessor};

			Random random = new Random();
			ECSNode serverToContact = nodes[random.nextInt(nodes.length)];
			System.out.println("Attempting to send get request to node: " + serverToContact.getNodeHost() + ":" + serverToContact.getNodePort());

			if (!serverToContact.getNodeHost().equals(this.getServerAdress()) || serverToContact.getNodePort() != this.getServerPort()) {
				String ip = serverToContact.getNodeHost();
				int port = serverToContact.getNodePort();
				this.setServerAdress(ip);
				this.setServerPort(port);
				this.connect();
			}
			this.clientCommunicationModule.sendMessage(getRequest);
			try {
				response = this.clientCommunicationModule.receiveMessage();
			} catch (IOException e) {
				// handle exception
				System.err.println("Error receiving message: " + e.getMessage());
				throw e;
			}

			// If server not responsible, update Metadata
			if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				KVMessage keyValueResponse = this.getKeyRange();
				if (keyValueResponse.getStatus() == KVMessage.StatusType.SERVER_STOPPED) return keyValueResponse;
				String keyRanges = keyValueResponse.getValue();
				String newListPorts = MD5Hasher.buildListOfPorts(keyRanges);
				metadata = new Metadata(newListPorts);
				System.out.println("Updating metadata");
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
