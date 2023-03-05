package shared.messages;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
public class ServerMessage implements Serializable {
    public enum ServerMessageStatus {
		SEND_KV, 	/* KVServer transfer data to serverToContact */
		SEND_KV_ACK,		/* reply after receiving data */
        SET_RUNNING,
        SET_RUNNING_ACK
	}
    private String KVPairs;
    private ServerMessageStatus status;
    private static final String DELIMITER = "@@@";
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    public ServerMessage(ServerMessageStatus status, String value) {
        this.status = status;
        this.KVPairs = value;
    }
    public ServerMessage(byte[] encodedStringBytes) throws IllegalArgumentException {
        byte[] bytes = addCtrChars(encodedStringBytes);
        String encodedString = new String(bytes).trim();
        String[] parts = encodedString.split(DELIMITER);
        System.out.println("encodedString" + encodedString);
        this.status = ServerMessageStatus.valueOf(parts[0].toUpperCase());
        if(parts.length >1)
            this.KVPairs = parts[1];
        else
            this.KVPairs = "";
    }
    public ServerMessageStatus getServerStatus(){
        return this.status;
    }
    public Map<String, String> getPairs(){
        Map<String, String> temp = new HashMap<String, String>();
        String[] strings = this.KVPairs.split(";;;;;");
        for (String str : strings) {
            String[] s = str.split("&&&&&");
            if(s.length==2)
                temp.put(s[0],s[1]);
        }
        return temp;
        
    }
    public String getEncodedString() {
        return this.getServerStatus().toString() + DELIMITER + this.KVPairs;
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