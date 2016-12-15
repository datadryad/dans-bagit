package org.datadryad.dansbagit;


import nu.xom.Document;
import nu.xom.Element;

import java.util.HashMap;
import java.util.Map;

public class DANSFiles
{
    /** Namespace of the DC Terms metadata elements */
    private static String DCTERMS_NAMESPACE = "http://purl.org/dc/terms/";

    /** Namespace of the DC metadata elements */
    private static String DC_NAMESPACE = "http://purl.org/dc/elements/1.1/";

    /** The PREMIS namespace */
    private static String PREMIS_NAMESPACE = "http://www.loc.gov/standards/premis";

    private Map<String, Map<String, String>> metadata = new HashMap<String, Map<String, String>>();

    public void addFileMetadata(String path, String field, String value)
    {
        if (!this.metadata.containsKey(path)) {
            this.metadata.put(path, new HashMap<String, String>());
        }
        Map<String, String> map = this.metadata.get(path);

        map.put(field, value);
    }

    public String toXML()
    {
        Element files = new Element("files");
        files.addNamespaceDeclaration("dcterms", DCTERMS_NAMESPACE);
        files.addNamespaceDeclaration("dc", DC_NAMESPACE);
        files.addNamespaceDeclaration("premis", PREMIS_NAMESPACE);

        for (String path : this.metadata.keySet())
        {
            Element fileEntry = new Element("file");

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
                fileEntry.appendChild(fieldElement);
            }

            files.appendChild(fileEntry);
        }

        Document doc = new Document(files);
        return doc.toXML();
    }

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
