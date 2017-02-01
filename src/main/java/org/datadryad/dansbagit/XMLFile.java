package org.datadryad.dansbagit;

import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class XMLFile
{
    public String xml2String(Element element)
            throws IOException
    {
        Document doc = new Document(element);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Serializer serializer = new Serializer(baos, "UTF-8");
        serializer.setIndent(4);
        serializer.write(doc);
        serializer.flush();
        return baos.toString("UTF-8");
    }
}
