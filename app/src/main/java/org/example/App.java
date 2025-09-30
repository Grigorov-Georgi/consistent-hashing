package org.example;

import java.util.List;

public class App {
    public static void main(String[] args) {
        ConsistentHashRing<String> ring = new ConsistentHashRing<>(128);

        ring.addNode("node1");
        ring.addNode("node2");
        ring.addNode("node3");

        String key = "first_key";
        String owner = ring.getNode(key);
        System.out.println("Key " + key + " is owned by " + owner);

        // Show 3 distinct owners for replication
        List<String> owners = ring.getDistinctNodes(key, 3);
        System.out.println("Replicas for " + key + " : " + owners);

        // Demonstrate minimal remapping when removing a node
        ring.removeNode("node3");
        String ownerAfter = ring.getNode(key);
        System.out.println("After removing node3, key '" + key + "' is owned by " + ownerAfter);
    }
}
