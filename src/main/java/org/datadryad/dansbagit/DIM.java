package org.datadryad.dansbagit;

import nu.xom.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * XML class representing the DIM (DSpace Internal Metadata) format
 */
public class DIM extends XMLFile
{
    private static String DIM_NAMESPACE = "http://www.dspace.org/xmlns/dspace/dim";

    /**
     * A list of fields and their values
     */
    private List<Map<String, String>> fields = new ArrayList<Map<String, String>>();

    /**
     * Add a new field with the given value
     *
     * @param field the field
     * @param value the value
     */
    public void addDSpaceField(String field, String value)
    {
        String[] bits = field.split("\\.");
        String schema = null;
        String element = null;
        String qualifier = null;
        if (bits.length >= 2)
        {
            schema = bits[0];
            element = bits[1];
        }
        if (bits.length == 3)
        {
            qualifier = bits[2];
        }

        this.addField(schema, element, qualifier, value);
    }

    /**
     * Add a field with the given schema, element, qualifier and value
     *
     * @param schema
     * @param element
     * @param qualifier
     * @param value
     */
    public void addField(String schema, String element, String qualifier, String value)
    {
        Map<String, String> entry = new HashMap<String, String>();
        entry.put("mdschema", schema);
        entry.put("element", element);
        entry.put("qualifier", qualifier);
        entry.put("value", value);
        this.fields.add(entry);
    }

    /**
     * Convert the in-memory metadata to an XML string
     *
     * @return  an xml string
     * @throws IOException
     */
    public String toXML()
            throws IOException
    {
        Element dim = new Element("dim:dim", DIM_NAMESPACE);
        dim.addNamespaceDeclaration("dim", DIM_NAMESPACE);

        Attribute attr = new Attribute("dspaceType", "ITEM");
        dim.addAttribute(attr);

        for (Map<String, String> entry : this.fields)
        {
            Element field = new Element("dim:field", DIM_NAMESPACE);
            for (String key : entry.keySet())
            {
                String value = entry.get(key);
                if ("value".equals(key) && value != null)
                {
                    field.appendChild(value);
                }
                else if (value != null)
                {
                    Attribute a = new Attribute(key, value);
                    field.addAttribute(a);
                }
            }
            dim.appendChild(field);
        }

        return this.xml2String(dim);
    }

    public static DIM parse(InputStream is)
            throws IOException
    {
        Builder parser = new Builder();
        Document doc;
        try
        {
            doc = parser.build(is);
        }
        catch (ParsingException e)
        {
            throw new RuntimeException(e);
        }
        Element root = doc.getRootElement();

        DIM dim = new DIM();
        for (int i = 0; i < root.getChildCount(); i++)
        {
            Node dimNode = root.getChild(i);
            if (dimNode instanceof Element) {
                Element dimEntry = (Element) dimNode;
                Attribute schemaAttr = dimEntry.getAttribute("mdschema");
                Attribute elementAttr = dimEntry.getAttribute("element");
                Attribute qualifierAttr = dimEntry.getAttribute("qualifier");

                String schema = null;
                String element = null;
                String qualifier = null;

                if (schemaAttr != null)
                {
                    schema = schemaAttr.getValue();
                }
                if (elementAttr != null)
                {
                    element = elementAttr.getValue();
                }
                if (qualifierAttr != null)
                {
                    qualifier = qualifierAttr.getValue();
                }

                String value = dimEntry.getValue();
                dim.addField(schema, element, qualifier, value);
            }
        }

        return dim;
    }
}
