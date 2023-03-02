package shared.module;

import ecs.ECSNode;
import shared.messages.Metadata;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Hasher {
    public static String hash(String key) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            // For each byte in the array, format as a two-digit hexadecimal string
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            String keyHashValue = sb.toString().toUpperCase();
            return keyHashValue;
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
    public static String buildKeyRangeMessage(String listOfHostPorts) {
        if (listOfHostPorts.length() == 0) {
            return null;
        }

        Metadata M = new Metadata(listOfHostPorts);
        StringBuilder sb = new StringBuilder();
        for (ECSNode node : M.getTree()) {
            String KRFrom = node.getNodeHashRange()[0];
            String KRTo = node.getNodeHashRange()[1];
            sb.append(KRFrom)
                    .append(", ")
                    .append(KRTo)
                    .append(", ")
                    .append(node.getNodeHost())
                    .append(":")
                    .append(node.getNodePort())
                    .append("; ");
        }
        String keyRangeMessage = sb.toString().trim();
        return keyRangeMessage;
    }

    public static String buildListOfPorts(String keyRangeMessage) {
        if (keyRangeMessage.length() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        String[] nodes = keyRangeMessage.split(";");

        for (String node : nodes) {
            String[] values = node.split(",");
            String ipPort = values[2].trim();
            sb.append(ipPort).append(" ");
        }

        String listOfPorts = sb.toString().trim();
        return listOfPorts;
    }


}