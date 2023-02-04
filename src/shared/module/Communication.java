package shared.module;

import shared.messages.Message;

import java.io.IOException;

public interface Communication {

    public void sendMessage(Message msg) throws IOException;
    public Message receiveMessage() throws IOException;

}
