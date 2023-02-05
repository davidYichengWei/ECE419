package shared.messages;

import org.apache.log4j.Logger;

import java.io.Serializable;

public class Message implements KVMessage, Serializable {


    private static Logger logger = Logger.getRootLogger();
    private static final String DELIMITER = " ";
    private String key;
    private String value;
    private StatusType status;
    private String msg;
    private byte[] msgBytes;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    public Message(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }
    public Message(byte[] encodedStringBytes) {
        byte[] bytes = addCtrChars(encodedStringBytes);
        String encodedString = new String(bytes).trim();
        String[] parts = encodedString.split(DELIMITER);
        logger.info("status: " + parts[0].toUpperCase());
        this.status = StatusType.valueOf(parts[0].toUpperCase());
        logger.info("key: " + parts[1]);
        this.key = parts[1];

        if (status != StatusType.GET) {
            logger.info("value: " + parts[2]);
            this.value = parts[2];
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }


    public String getEncodedString() {
        return this.getStatus().toString() + DELIMITER + this.getKey() + DELIMITER + this.getValue();
    }

    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{RETURN,LINE_FEED};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public byte[] toByteArray(){
        byte[] bytes = this.getEncodedString().getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }



}
