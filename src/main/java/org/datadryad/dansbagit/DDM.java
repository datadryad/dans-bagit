package org.datadryad.dansbagit;

import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DDM extends XMLFile
{
    private static String DC_NAMESPACE = "http://purl.org/dc/elements/1.1/";
    private static String DCTERMS_NAMESPACE = "http://purl.org/dc/terms/";
    private static String DCX_NAMESPACE = "http://easy.dans.knaw.nl/schemas/dcx/dai/";
    private static String DDM_NAMESPACE = "http://easy.dans.knaw.nl/schemas/md/ddm/";
    private static String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";
    private static String ID_NAMESPACE = "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/";

    private static final Map<String, String> nsMap;
    static
    {
        nsMap = new HashMap<String, String>();
        nsMap.put("dc", DC_NAMESPACE);
        nsMap.put("dcterms", DCTERMS_NAMESPACE);
        nsMap.put("dcx-dai", DCX_NAMESPACE);
        nsMap.put("ddm", DDM_NAMESPACE);
        nsMap.put("xsi", XSI_NAMESPACE);
        nsMap.put("id-type", ID_NAMESPACE);
    }

    private static final List<String> profileOrder;
    static
    {
        profileOrder = new ArrayList<String>();
        profileOrder.add("dc:title");
        profileOrder.add("dc:description");
        profileOrder.add("dc:creator");
        profileOrder.add("ddm:created");
        profileOrder.add("ddm:available");
        profileOrder.add("ddm:audience");
        profileOrder.add("ddm:accessRights");
    }

    private Map<String, List<Map<String, String>>> profileFields = new HashMap<String, List<Map<String, String>>>();
    private Map<String, List<Map<String, String>>> dcmiFields = new HashMap<String, List<Map<String, String>>>();

    public void addProfileField(String field, String value)
    {
        this.addProfileField(field, value, null);
    }

    public void addProfileField(String field, String value, Map<String, String> attrs)
    {
        this.addToRegister(this.profileFields, field, value, attrs);
    }

    public void addDCMIField(String field, String value)
    {
        this.addDCMIField(field, value, null);
    }

    public void addDCMIField(String field, String value, Map<String, String> attrs)
    {
        this.addToRegister(this.dcmiFields, field, value, attrs);
    }

    public String toXML()
            throws IOException
    {
        Element ddm = new Element("ddm:DDM", DDM_NAMESPACE);
        ddm.addNamespaceDeclaration("dcterms", DCTERMS_NAMESPACE);
        ddm.addNamespaceDeclaration("dc", DC_NAMESPACE);
        ddm.addNamespaceDeclaration("dcx-dai", DCX_NAMESPACE);
        ddm.addNamespaceDeclaration("ddm", DDM_NAMESPACE);
        ddm.addNamespaceDeclaration("xsi", XSI_NAMESPACE);
        ddm.addNamespaceDeclaration("id-type", ID_NAMESPACE);

        if (this.profileFields.size() > 0)
        {
            Element profile = new Element("ddm:profile", DDM_NAMESPACE);
            this.populateElement(profile, this.profileFields, DDM.profileOrder);
            ddm.appendChild(profile);
        }

        if (this.dcmiFields.size() > 0)
        {
            Element profile = new Element("ddm:dcmiMetadata", DDM_NAMESPACE);
            this.populateElement(profile, this.dcmiFields, null);
            ddm.appendChild(profile);
        }

        return this.xml2String(ddm);
        // Document doc = new Document(ddm);
        // return doc.toXML();
    }

    private void addToRegister(Map<String, List<Map<String, String>>> register, String field, String value, Map<String, String> attrs)
    {
        if (!register.containsKey(field))
        {
            register.put(field, new ArrayList<Map<String, String>>());
        }
        List<Map<String, String>> fieldList = register.get(field);

        Map<String, String> entry = new HashMap<String, String>();
        entry.put("_value", value);
        if (attrs != null)
        {
            entry.putAll(attrs);
        }

        fieldList.add(entry);
    }

    private String getNamespace(String field)
    {
        String[] bits = field.split(":");
        String ns = DDM.nsMap.get(bits[0]);
        return ns;
    }

    private void populateElement(Element element, Map<String, List<Map<String, String>>> register, List<String> order)
    {
        if (order != null)
        {
            for (String field : order)
            {
                this.doField(element, register, field);
            }
        }
        else
        {
            for (String field : register.keySet())
            {
                this.doField(element, register, field);
            }
        }
    }

    private void doField(Element element, Map<String, List<Map<String, String>>> register, String field)
    {
        String ns = this.getNamespace(field);

        List<Map<String, String>> entries = register.get(field);
        if (entries == null)
        {
            return;
        }
        for (Map<String, String> entry : entries)
        {
            Element el = new Element(field, ns);
            for (String key : entry.keySet())
            {
                String value = entry.get(key);
                if ("_value".equals(key))
                {
                    el.appendChild(value);
                }
                else
                {
                    String ans = this.getNamespace(key);
                    Attribute attr = new Attribute(key, ans, value);
                    el.addAttribute(attr);
                }
            }
            element.appendChild(el);
        }
    }
}
