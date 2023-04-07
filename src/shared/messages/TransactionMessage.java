package shared.messages;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TransactionMessage implements ITransactionMessage, Serializable {
    private List<String[]> keyValuePairs;
    private static Logger logger = Logger.getRootLogger();
    private static final String KEYVALUEDELIMITER = "@@@";
    private static final String PAIRDELIMITER = "###";
    private TStatusType status;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    public TransactionMessage(TStatusType status) {
        this.keyValuePairs = new ArrayList<>();
        this.status = status;
    }

    public TransactionMessage(byte[] encodedStringBytes) throws IllegalArgumentException {
        this.keyValuePairs = new ArrayList<>();
        byte[] bytes = addCtrChars(encodedStringBytes);
        String encodedString = new String(bytes).trim();
        String[] parts = encodedString.split(PAIRDELIMITER);
        this.status = TStatusType.valueOf(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            String[] pair = parts[i].split(KEYVALUEDELIMITER);
            if (pair.length == 2) {
                this.addKeyValuePair(pair[0], pair[1]);
            }
        }
    }
    @Override
    public void addKeyValuePair(String key, String value) {
        keyValuePairs.add(new String[]{key, value});
    }
    @Override
    public List<String[]> getKeyValuePairs() {
        return keyValuePairs;
    }
    public String getEncodedString() {
        StringBuilder sb = new StringBuilder();
        sb.append(status.toString()).append(PAIRDELIMITER);
        for (String[] pair : keyValuePairs) {
            sb.append(pair[0]).append(KEYVALUEDELIMITER).append(pair[1]).append(PAIRDELIMITER);
        }
        // Remove the trailing PAIRDELIMITER if there are any pairs
        if (!keyValuePairs.isEmpty()) {
            sb.delete(sb.length() - PAIRDELIMITER.length(), sb.length());
        }
        return sb.toString();
    }
    public byte[] toByteArray(){
        byte[] bytes = this.getEncodedString().getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }
    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{RETURN,LINE_FEED};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public TStatusType getStatus() {
        return this.status;
    }
}
