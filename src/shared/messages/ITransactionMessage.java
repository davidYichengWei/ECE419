package shared.messages;

import java.util.List;

public interface ITransactionMessage {

    public enum TStatusType {
        TRANSACTION_PUT,
        TRANSACTION_PUT_SUCCESS,
        TRANSACTION_PUT_FAILURE
    }

    public void addKeyValuePair(String key, String value);

    /**
     * @return the key that is associated with this message,
     * 		null if not key is associated.
     */
    public List<String[]> getKeyValuePairs();
}


