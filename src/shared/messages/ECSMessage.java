package shared.messages;

import java.io.Serializable;

// Communication protocol between ECS and KVServer
public class ECSMessage implements Serializable {
    public enum ECSMessageStatus {
		TRANSFER_BEGIN, 	/* From ECS -> KVServer */
		TRANSFER_DONE 		/* From KVServer -> ECS */
	}

    private String metadata; // Updated metadata, null if status is TRANSFER_DONE
    private ECSMessageStatus status;
    // Server to contact for data transfer, only needed if status is TRANSFER_BEGIN
    // and there are 2 servers involved
    private String serverToContact;

    private static final String DELIMITER = " ";
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    public ECSMessage(ECSMessageStatus status, String metadata, String serverToContact) {
        this.status = status;
        this.metadata = metadata;
        this.serverToContact = serverToContact;
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

    public String getMetadata() {
        return metadata;
    }

    public ECSMessageStatus getStatus() {
        return status;
    }

    public String getServerToContact() {
        return serverToContact;
    }

    public String getEncodedString() {
        return this.getStatus().toString() + DELIMITER + this.getMetadata() + DELIMITER + this.getServerToContact();
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
