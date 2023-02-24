package org.apache.beam.examples.cryptograph;//From https://www.baeldung.com/java-cipher-class

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class Cryptographer {

    public static String encryptMessage(String message, String key, String transformation)
            throws InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException,
            BadPaddingException, IllegalBlockSizeException {

        Cipher cipher = Cipher.getInstance(transformation);
        System.out.println("encryptMessage_algorithm:" + transformation.split("/")[0]);
        SecretKey secretKey = new SecretKeySpec(key.getBytes(), transformation.split("/")[0]);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] encryptedBytes = cipher.doFinal(message.getBytes());
        return Base64.getEncoder().encodeToString(encryptedBytes);
    }

    public static String decryptMessage(String encryptedMessage, String key, String transformation)
            throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
            BadPaddingException, IllegalBlockSizeException {

        Cipher cipher = Cipher.getInstance(transformation);
        System.out.println("decryptMessage_algorithm:" + transformation.split("/")[0]);
        SecretKey secretKey = new SecretKeySpec(key.getBytes(), transformation.split("/")[0]);
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] decodedBytes = Base64.getDecoder().decode(encryptedMessage);
        byte[] decryptedBytes = cipher.doFinal(decodedBytes);
        return new String(decryptedBytes);
    }
}