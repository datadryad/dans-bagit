package org.datadryad.dansbagit;

import java.security.MessageDigest;

public class Files
{
    public static String sanitizeFilename(String inputName) {
        return inputName.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
    }

    public static String digestToString(MessageDigest md) {
        byte[] b = md.digest();
        String result = "";
        for (int i=0; i < b.length; i++)
        {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }
        return result;
    }
}
