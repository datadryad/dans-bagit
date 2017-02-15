package org.datadryad.dansbagit;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility library containing operations that we might want to do on files and their bits
 */
public class Files
{
    /**
     * Clean the given string to ensure it is safe to use as a filename
     *
     * @param inputName the unsafe string
     * @return the safe string
     */
    public static String sanitizeFilename(String inputName) {
        return inputName.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
    }

    /**
     * Get the hex string out of the given message digest
     * @param md    message digest object
     * @return  hex string
     */
    public static String digestToString(MessageDigest md) {
        byte[] b = md.digest();
        String result = "";
        for (int i=0; i < b.length; i++)
        {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }
        return result;
    }

    /**
     * Calcuate the md5 checksum of the given input stream
     *
     * @param is input stream
     * @return  md5 hex string
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    public static String md5Hex(InputStream is)
            throws NoSuchAlgorithmException, IOException
    {
        MessageDigest md = MessageDigest.getInstance("MD5");
        DigestInputStream dis = new DigestInputStream(is, md);

        int count;
        int BUFFER = 1000000;
        byte data[] = new byte[BUFFER];
        while((count = dis.read(data, 0, BUFFER)) != -1) {
            // do nothing, we only care about the results of the digest calculation
        }

        return Files.digestToString(md);
    }
}
