package org.datadryad.dansbagit;

import nu.xom.Attribute;
import nu.xom.Element;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * XML class to represent the DANS files.xml format
 */
public class DANSFiles extends XMLFile
{
    /** Namespace of the DC Terms metadata elements */
    private static String DCTERMS_NAMESPACE = "http://purl.org/dc/terms/";

    /** Namespace of the DC metadata elements */
    private static String DC_NAMESPACE = "http://purl.org/dc/elements/1.1/";

    /** The PREMIS namespace */
    private static String PREMIS_NAMESPACE = "http://www.loc.gov/standards/premis";

    /** DANS Identifier namespaces */
    private static String ID_NAMESPACE = "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/";
    private static String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";
    
    /** map of metadata linking file paths, to a set of key/value pairs */
    private Map<String, Map<String, String>> metadata = new HashMap<String, Map<String, String>>();

    /**
     * Add metadata about a file
     *
     * @param path  the file path the metadata concerns
     * @param field     the metadata field to add
     * @param value     the metadata value to add
     */
    public void addFileMetadata(String path, String field, String value)
    {
        if (!this.metadata.containsKey(path)) {
            this.metadata.put(path, new HashMap<String, String>());
        }
        Map<String, String> map = this.metadata.get(path);

        map.put(field, value);
    }

    /**
     * Convert the in-memory information to an XML string (suitable for then writing to file)
     *
     * @return  the XML as a string
     * @throws IOException
     */
    public String toXML()
            throws IOException
    {
        Element files = new Element("files");
        files.addNamespaceDeclaration("dcterms", DCTERMS_NAMESPACE);
        files.addNamespaceDeclaration("dc", DC_NAMESPACE);
        files.addNamespaceDeclaration("premis", PREMIS_NAMESPACE);
        files.addNamespaceDeclaration("id-type", ID_NAMESPACE);
        files.addNamespaceDeclaration("xsi", XSI_NAMESPACE);

        for (String path : this.metadata.keySet())
        {
            Element fileEntry = new Element("file");
            Attribute pathAttr = new Attribute("filepath", path);
            fileEntry.addAttribute(pathAttr);

            Map<String, String> fields = this.metadata.get(path);
            for (String field : fields.keySet())
            {
                String value = fields.get(field);
                String namespace = this.getNamespace(field);
                if (namespace == null)
                {
                    continue;
                }
                Element fieldElement = new Element(field, namespace);
                fieldElement.appendChild(value);
                if(field.equals("dcterms:identifier") || field.equals("identifier")) {
                    Attribute att = new Attribute("type", "id-type:DOI");
                    att.setNamespace("xsi", XSI_NAMESPACE);
                    fieldElement.addAttribute(att);
                }
                fileEntry.appendChild(fieldElement);
            }

            files.appendChild(fileEntry);
        }

        return this.xml2String(files);
    }

    /**
     * Get the namespace for a given field.  The field should be prefixed with it's namespace, such as "dc:title"
     * and the namespace needs to be one of those recognised by this class
     *
     * @param fieldName the field including namespace prefix
     * @return  the namespace URI
     */
    private String getNamespace(String fieldName)
    {
        if (fieldName.startsWith("dc:"))
        {
            return DC_NAMESPACE;
        }
        else if (fieldName.startsWith("dcterms:"))
        {
            return DCTERMS_NAMESPACE;
        }
        else if (fieldName.startsWith("premis:"))
        {
            return PREMIS_NAMESPACE;
        }
        return null;
    }
}
