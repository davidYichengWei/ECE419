package testing;

import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.Metadata;

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
}
