package testing;

import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.Metadata;
import shared.module.MD5Hasher;

import java.util.Comparator;
import java.util.TreeSet;

public class MetadataTest extends TestCase {
    public void testAddNode() {
        Metadata metadata = new Metadata("localhost:8080 localhost:8081");
        ECSNode newNode = new ECSNode("node3", "localhost", 8082, null);
        metadata.addNode(newNode);
        TreeSet<ECSNode> expectedTree = new TreeSet<>(new Comparator<ECSNode>() {
            @Override
            public int compare(ECSNode Node1, ECSNode Node2) {
                String KeyRangeTo1 = Node1.getNodeHashRange()[1];
                String KeyRangeTo2 = Node2.getNodeHashRange()[1];
                return KeyRangeTo1.compareTo(KeyRangeTo2);
            }
        });
        expectedTree.add(new ECSNode(null, "localhost", 8080, null));
        expectedTree.add(new ECSNode(null, "localhost", 8081, null));
        expectedTree.add(new ECSNode(null, "localhost", 8082, null));
        assertEquals(expectedTree, metadata.getTree());
    }

    public void testRemoveNode() {
        Metadata metadata = new Metadata("localhost:8080 localhost:8081 localhost:8082");
        ECSNode nodeToRemove = new ECSNode(null, "localhost", 8081, null);
        metadata.removeNode(nodeToRemove);
        TreeSet<ECSNode> expectedTree = new TreeSet<>(new Comparator<ECSNode>() {
            @Override
            public int compare(ECSNode Node1, ECSNode Node2) {
                String KeyRangeTo1 = Node1.getNodeHashRange()[1];
                String KeyRangeTo2 = Node2.getNodeHashRange()[1];
                return KeyRangeTo1.compareTo(KeyRangeTo2);
            }
        });
        expectedTree.add(new ECSNode(null, "localhost", 8080, null));
        expectedTree.add(new ECSNode(null, "localhost", 8082, null));
        assertEquals(expectedTree, metadata.getTree());
    }

    public void testFindNode() {
        Metadata metadata = new Metadata("localhost:8080 localhost:8081 localhost:8082");
        assertNotNull(metadata.findNode("testKey"));
    }

    public void testConstructorWithNull() {
        Metadata metadata = new Metadata(null);
        TreeSet<ECSNode> expectedTree = new TreeSet<>(new Comparator<ECSNode>() {
            @Override
            public int compare(ECSNode Node1, ECSNode Node2) {
                String KeyRangeTo1 = Node1.getNodeHashRange()[1];
                String KeyRangeTo2 = Node2.getNodeHashRange()[1];
                return KeyRangeTo1.compareTo(KeyRangeTo2);
            }
        });
        assertEquals(expectedTree, metadata.getTree());
    }
    public void testFindSuccessor() {
        Metadata metadata = new Metadata("localhost:8081 localhost:8082 localhost:8083");
        ECSNode newNode = new ECSNode("node4", "localhost", 8084, null);
        metadata.addNode(newNode);

        String HashKey = MD5Hasher.hash("localhost:8084");
        ECSNode responsibleNode = metadata.findNode(HashKey);
        ECSNode firstSuccessor = metadata.findSuccessor(responsibleNode);
        ECSNode secondSuccessor = metadata.findSuccessor(firstSuccessor);
        ECSNode thirdSuccessor = metadata.findSuccessor(secondSuccessor);

//        System.out.println("Responsible node is: " + responsibleNode.getNodeHost() + ":" + responsibleNode.getNodePort());
//        System.out.println("First successor is: " + firstSuccessor.getNodeHost() + ":" + firstSuccessor.getNodePort());
//        System.out.println("Second successor is: " + secondSuccessor.getNodeHost() + ":" + secondSuccessor.getNodePort());
//        System.out.println("Third successor is: " + thirdSuccessor.getNodeHost() + ":" + thirdSuccessor.getNodePort());

        assertEquals("localhost:8084", responsibleNode.getNodeHost() + ":" + responsibleNode.getNodePort());
        assertFalse("localhost:8084".equals(firstSuccessor.getNodeHost() + ":" + firstSuccessor.getNodePort()));
        assertFalse("localhost:8084".equals(secondSuccessor.getNodeHost() + ":" + secondSuccessor.getNodePort()));
        assertFalse("localhost:8084".equals(thirdSuccessor.getNodeHost() + ":" + thirdSuccessor.getNodePort()));
    }

    public void testFindPredecessor() {
        Metadata metadata = new Metadata("localhost:8081 localhost:8082 localhost:8083");
        ECSNode newNode = new ECSNode("node4", "localhost", 8084, null);
        metadata.addNode(newNode);

        String HashKey = MD5Hasher.hash("localhost:8084");
        ECSNode responsibleNode = metadata.findNode(HashKey);
        ECSNode firstPredecessor = metadata.findPredecessor(responsibleNode);
        ECSNode secondPredecessor = metadata.findPredecessor(firstPredecessor);
        ECSNode thirdPredecessor = metadata.findPredecessor(secondPredecessor);

//        System.out.println("Responsible node is: " + responsibleNode.getNodeHost() + ":" + responsibleNode.getNodePort());
//        System.out.println("First predecessor is: " + firstPredecessor.getNodeHost() + ":" + firstPredecessor.getNodePort());
//        System.out.println("Second predecessor is: " + secondPredecessor.getNodeHost() + ":" + secondPredecessor.getNodePort());
//        System.out.println("Third predecessor is: " + thirdPredecessor.getNodeHost() + ":" + thirdPredecessor.getNodePort());

        assertEquals("localhost:8084", responsibleNode.getNodeHost() + ":" + responsibleNode.getNodePort());
        assertFalse("localhost:8084".equals(firstPredecessor.getNodeHost() + ":" + firstPredecessor.getNodePort()));
        assertFalse("localhost:8084".equals(secondPredecessor.getNodeHost() + ":" + secondPredecessor.getNodePort()));
        assertFalse("localhost:8084".equals(thirdPredecessor.getNodeHost() + ":" + thirdPredecessor.getNodePort()));
    }
}
