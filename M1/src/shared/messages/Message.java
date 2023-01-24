package shared.messages;

import java.io.Serializable;

public class Message implements KVMessage, Serializable {
    private String key;
    private String value;
    private StatusType status;

    public Message(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
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
}
