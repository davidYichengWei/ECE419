package testing;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import shared.messages.*;

public class TransactionMessageTest extends TestCase {

    private TransactionMessage message;

    @Test
    public void testAddKeyValuePair() {
        message = new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);
        message.addKeyValuePair("key1", "value1");
        message.addKeyValuePair("key2", "value2");
        List<String[]> expected = new ArrayList<>();
        expected.add(new String[]{"key1", "value1"});
        expected.add(new String[]{"key2", "value2"});
        assertArrayEquals(expected.toArray(), message.getKeyValuePairs().toArray());
    }

    @Test
    public void testGetEncodedString() {
        message = new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);
        message.addKeyValuePair("key1", "value1");
        message.addKeyValuePair("key2", "value2");
        String expected = "TRANSACTION_PUT_SUCCESS###key1@@@value1###key2@@@value2";
        assertEquals(expected, message.getEncodedString());
    }

    @Test
    public void testToByteArray() {
        message = new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);
        message.addKeyValuePair("key1", "value1");
        message.addKeyValuePair("key2", "value2");
        byte[] expected = "TRANSACTION_PUT_SUCCESS###key1@@@value1###key2@@@value2\n\r".getBytes();
        assertArrayEquals(expected, message.toByteArray());
    }

    @Test
    public void testConstructorWithEncodedStringBytes() {
        String encodedString = "TRANSACTION_PUT_SUCCESS###key1@@@value1###key2@@@value2\n\r";
        byte[] bytes = encodedString.getBytes();
        TransactionMessage expected = new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);
        expected.addKeyValuePair("key1", "value1");
        expected.addKeyValuePair("key2", "value2");
        TransactionMessage actual = new TransactionMessage(bytes);
        assertEquals(expected.getStatus(), actual.getStatus());
        assertArrayEquals(expected.getKeyValuePairs().toArray(), actual.getKeyValuePairs().toArray());
    }
    @Test
    public void testGetStatus() {
        message = new TransactionMessage(ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS);
        assertEquals(ITransactionMessage.TStatusType.TRANSACTION_PUT_SUCCESS, message.getStatus());
    }

}
