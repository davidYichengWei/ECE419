package shared.messages;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
public class ServerMessage implements Serializable {
    public enum ServerMessageStatus {
		SEND_KV, 	/* KVServer transfer data to serverToContact */
		SEND_KV_ACK 		/* reply after receiving data */
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

    public ServerMessageStatus getServerStatus(){
        return this.status;
    }
    public Map<String, String> getPairs(){
        Map<String, String> temp = new HashMap<String, String>();
        String[] strings = this.KVPairs.split(",");
        for (String str : strings) {
            String[] s = str.split("=");
            temp.put(s[0],s[1]);
        }
        return temp;
        
    }
    
}