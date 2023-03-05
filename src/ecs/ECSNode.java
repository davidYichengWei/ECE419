package ecs;

import shared.module.MD5Hasher;

public class ECSNode implements IECSNode {
    private String keyRangeFrom; //NOT INCLUSIVE
    private String keyRangeTo;
    private String name;
    private String host;
    private int port;

    public ECSNode(String name, String host, int port, String startHashValue) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.keyRangeFrom = startHashValue;
        String hostPort = host + ":" + String.valueOf(port);
        this.keyRangeTo = MD5Hasher.hash(hostPort);
    }

    public void setKeyRangeFrom(String keyRangeFrom) {
        this.keyRangeFrom = keyRangeFrom;
    }

    public void setKeyRangeTo(String keyRangeTo) {
        this.keyRangeTo = keyRangeTo;
    }

    @Override
    public String getNodeName() {
        return name;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[] {this.keyRangeFrom, this.keyRangeTo};
    }
}
