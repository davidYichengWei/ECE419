package shared.messages;

import ecs.ECSNode;

import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;

public class Metadata implements IMetadata{
    private TreeSet<ECSNode> tree;

    public Metadata(String listOfHostPorts) {
        this.tree = new TreeSet<>(new Comparator<ECSNode>() {
            @Override
            public int compare(ECSNode Node1, ECSNode Node2) {
                String KeyRangeTo1 = Node1.getNodeHashRange()[1];
                String KeyRangeTo2 = Node2.getNodeHashRange()[1];
                return KeyRangeTo1.compareTo(KeyRangeTo2);
            }
        });
        String[] hostPorts;
        if (listOfHostPorts != null) {
            hostPorts = listOfHostPorts.split(" ");
        } else {
            hostPorts = new String[0];
        }
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
        System.out.println("Finding node for key: " + key);
        ECSNode dummyNode = new ECSNode(null, null, -1, key);
        dummyNode.setKeyRangeTo(key);
        ECSNode node = tree.ceiling(dummyNode);
        if (node == null) {
            node = tree.first();
        }
        System.out.println("Found node: " + node.getNodeHost() + ":" + node.getNodePort() + " with hash range " + Arrays.toString(node.getNodeHashRange()));
        return node;
    }
    public TreeSet<ECSNode> getTree() {
        return tree;
    }

    public ECSNode findSuccessor(ECSNode node) {
        ECSNode successor = tree.ceiling(node);
        if (node == null) {
            node = tree.first();
        }
        System.out.println("Found node: " + successor.getNodeHost() + ":" + successor.getNodePort() + " with hash range " + Arrays.toString(successor.getNodeHashRange()));
        return successor;
    }

    public ECSNode findPredecessor(ECSNode node) {
        ECSNode predecessor = tree.floor(node);
        if (node == null) {
            node = tree.last();
        }
        System.out.println("Found node: " + predecessor.getNodeHost() + ":" + predecessor.getNodePort() + " with hash range " + Arrays.toString(predecessor.getNodeHashRange()));
        return predecessor;
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

    // returns the difference between two Metadata (extra node not shared between M1, M2)
    public static ECSNode findDifferentNode(Metadata M1, Metadata M2) {
        ECSNode extraNode = null;
        TreeSet<ECSNode> tree1 = M1.getTree();
        TreeSet<ECSNode> tree2 = M2.getTree();
        if (tree1.size() == tree2.size()) {
            return extraNode;
        }
        TreeSet<ECSNode> biggerTree;
        TreeSet<ECSNode> smallerTree;
        if (tree1.size() > tree2.size()) {
            biggerTree = tree1;
            smallerTree = tree2;
        }
        else {
            biggerTree = tree2;
            smallerTree = tree1;
        }
        for (ECSNode node : biggerTree) {
            if (!smallerTree.contains(node)) {
                extraNode = node;
                break;
            }
        }
        return extraNode;
    }

    //returns the responsible range combined with the replicated range for a given ECSNode [from, to]
    public String[] findTotalRange(ECSNode node) {
        String from;
        String to;
        ECSNode firstPredecessor = this.findPredecessor(node);
        ECSNode secondPredecessor = this.findPredecessor(firstPredecessor);

        from = secondPredecessor.getNodeHashRange()[0];
        to = node.getNodeHashRange()[1];

        return new String[] {from, to};
    }

    // Returns the replicated range for a given ECSNode [from, to]
    public String[] findReplicatedRange(ECSNode node) {
        String from;
        String to;
        ECSNode firstPredecessor = this.findPredecessor(node);
        ECSNode secondPredecessor = this.findPredecessor(firstPredecessor);

        from = secondPredecessor.getNodeHashRange()[0];
        to = firstPredecessor.getNodeHashRange()[1];

        return new String[] {from, to};
    }

}
