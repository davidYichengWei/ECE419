package shared.messages;
import java.io.Serializable;
public class ServerMessage implements Serializable {
    public enum ServerMessageStatus {
		SEND_KV, 	/* KVServer transfer data to serverToContact */
        RECEIVE, 	/* KVServer receive data from serverToContact */
        NO_TRANSFER, 	/* KVServer is the first server added or last server deleted, no data transfer required */
		SEND_KV_ACK 		/* From KVServer -> ECS to acknowledge */
	}
    private String KVPairs;
    private ServerMessageStatus status;
    private static final String DELIMITER = "@";
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    public ServerMessage(ServerMessageStatus status, String value) {
        this.status = status;
        this.KVPairs = value;
    }
    public ECSMessage(byte[] encodedStringBytes) throws IllegalArgumentException {
        byte[] bytes = addCtrChars(encodedStringBytes);
        String encodedString = new String(bytes).trim();
        String[] parts = encodedString.split(DELIMITER);
        this.status = ECSMessageStatus.valueOf(parts[0].toUpperCase());
        if (parts.length > 1) {
            this.metadata = parts[1];
            if (parts.length > 2) {
                this.serverToContact = parts[2];
            }
        }
    }
}