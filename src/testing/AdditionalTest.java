package testing;

import app_kvClient.KVClient;
import client.KVCommInterface;
import org.junit.Test;

import junit.framework.TestCase;
import shared.messages.Message;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testStub() {
		assertTrue(true);
	}

	@Test
	public void testPutCommand() throws Exception {
		KVClient client = new KVClient();
		client.newConnection("localhost", 50000);
		client.handleCommand("put key1 value1");
		KVCommInterface store = client.getStore();
		Message response = (Message) store.get("key1");
		assertEquals("value1", response.getValue());
	}
	@Test //Testing invalid command
	public void testInvalidCommand() {

		Exception ex = null;


		try {
			KVClient kvClient = new KVClient();
			kvClient.handleCommand("foo bar");
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}
	@Test //Invalid number of arguments
	public void testInvalidArguments() {

		Exception ex = null;


		try {
			KVClient kvClient = new KVClient();
			kvClient.handleCommand("connect localhost");
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

}
