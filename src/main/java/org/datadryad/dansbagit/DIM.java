package org.datadryad.dansbagit;

import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DIM extends XMLFile
{
    private static String DIM_NAMESPACE = "http://www.dspace.org/xmlns/dspace/dim";

    private List<Map<String, String>> fields = new ArrayList<Map<String, String>>();

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

    public void addField(String schema, String element, String qualifier, String value)
    {
        Map<String, String> entry = new HashMap<String, String>();
        entry.put("mdschema", schema);
        entry.put("element", element);
        entry.put("qualifier", qualifier);
        entry.put("value", value);
        this.fields.add(entry);
    }

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

        //Document doc = new Document(dim);
        //return doc.toXML();
        return this.xml2String(dim);
    }
}
