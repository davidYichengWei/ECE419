package shared.messages;

import ecs.ECSNode;

import java.util.TreeSet;

public class Metadata implements IMetadata{
    private TreeSet<ECSNode> tree;

    public Metadata(String listOfHostPorts) {
        this.tree = new TreeSet<>((Node1, Node2) -> {
            String KeyRangeTo1 = Node1.getNodeHashRange()[1];
            String KeyRangeTo2 = Node2.getNodeHashRange()[1];
            return KeyRangeTo1.compareTo(KeyRangeTo2);
        });
        String[] hostPorts = listOfHostPorts.split(" ");
        // Insert all Servers into tree
        for (String hostPort : hostPorts) {
            String[] parts = hostPort.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            // Insert into tree
            ECSNode newNode = new ECSNode(null, host, port, null);
            this.tree.add(newNode);
        }
        // Set all keyRangeFrom of all nodes
        for (ECSNode node : tree) {
            ECSNode predecessor = tree.lower(node);
            if (predecessor == null) {//first node, its keyRangeFrom will be the greatest node
                predecessor = tree.last();
            }
            String predTo = predecessor.getNodeHashRange()[1];
            node.setKeyRangeFrom(predTo);
        }
    }

    @Override
    public void addNode(ECSNode node) {
        this.tree.add(node);
        // Set all keyRangeFrom of all nodes
        for (ECSNode n : tree) {
            ECSNode predecessor = tree.lower(n);
            if (predecessor == null) {//first node, its keyRangeFrom will be the greatest node
                predecessor = tree.last();
            }
            String predTo = predecessor.getNodeHashRange()[1];
            n.setKeyRangeFrom(predTo);
        }
    }

    @Override
    public void removeNode(ECSNode node) {
        this.tree.remove(node);
        // Set all keyRangeFrom of all nodes
        for (ECSNode n : tree) {
            ECSNode predecessor = tree.lower(n);
            if (predecessor == null) {//first node, its keyRangeFrom will be the greatest node
                predecessor = tree.last();
            }
            String predTo = predecessor.getNodeHashRange()[1];
            n.setKeyRangeFrom(predTo);
        }
    }

    @Override
    public ECSNode findNode(String key) {
        ECSNode dummyNode = new ECSNode(null, null, -1, key);
        ECSNode node = tree.ceiling(dummyNode);
        if (node == null) {
            node = tree.first();
        }
        return node;
    }

    public TreeSet<ECSNode> getTree() {
        return tree;
    }

    public String buildListOfHostPorts() {
        // To be used to send to client when server is not responsible, client can then rebuild its metadata
        StringBuilder sb = new StringBuilder();
        for (ECSNode node : tree) {
            sb.append(node.getNodeHost())
                .append(":")
                .append(node.getNodePort())
                .append(" ");
        }
        String listOfHostPorts = sb.toString().trim();
        return listOfHostPorts;
    }
}
