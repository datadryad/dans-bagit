package org.datadryad.dansbagit;

import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Parent XML class which provides features of use to other classes which work with XML
 *
 */
public class XMLFile
{
    /**
     * Convert the given element to an XML string
     *
     * @param element   the element
     * @return  a string of the xml
     * @throws IOException
     */
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
