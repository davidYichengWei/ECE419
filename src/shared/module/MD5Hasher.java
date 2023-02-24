package shared.module;

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
}
