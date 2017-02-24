package org.datadryad.dansbagit;

import nu.xom.Attribute;
import nu.xom.Element;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Class to represent XML in the DANS DDM format
 */
public class DDM extends XMLFile
{
    private static String DC_NAMESPACE = "http://purl.org/dc/elements/1.1/";
    private static String DCTERMS_NAMESPACE = "http://purl.org/dc/terms/";
    private static String DCX_NAMESPACE = "http://easy.dans.knaw.nl/schemas/dcx/dai/";
    private static String DDM_NAMESPACE = "http://easy.dans.knaw.nl/schemas/md/ddm/";
    private static String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";
    private static String ID_NAMESPACE = "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/";

    /**
     * A map of namespaces to prefixes
     */
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

    /**
     * The order in which fields should appear in the profile section of the XML
     */
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

    private static final Map<String, List<Map<String, String>>> profileDefaults;
    static
    {
        profileDefaults = new HashMap<String, List<Map<String, String>>>();

        List<Map<String, String>> title = new ArrayList<Map<String, String>>();
        Map<String, String> tval = new HashMap<String, String>();
        tval.put("_value", "Untitled");
        title.add(tval);
        profileDefaults.put("dc:title", title);

        List<Map<String, String>> desc = new ArrayList<Map<String, String>>();
        Map<String, String> dval = new HashMap<String, String>();
        dval.put("_value", "No description available");
        desc.add(dval);
        profileDefaults.put("dc:description", desc);

        List<Map<String, String>> cont = new ArrayList<Map<String, String>>();
        Map<String, String> cval = new HashMap<String, String>();
        cval.put("_value", "Unknown Author");
        cont.add(cval);
        profileDefaults.put("dc:creator", cont);

        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSSX");
        String createdDate = sdf.format(now);
        List<Map<String, String>> created = new ArrayList<Map<String, String>>();
        Map<String, String> createdval = new HashMap<String, String>();
        createdval.put("_value", createdDate);
        created.add(createdval);
        profileDefaults.put("dc:creator", created);

        List<Map<String, String>> available = new ArrayList<Map<String, String>>();
        Map<String, String> aval = new HashMap<String, String>();
        aval.put("_value", "2099-12-31T23:59:59Z");
        available.add(aval);
        profileDefaults.put("ddm:available", available);

        List<Map<String, String>> audience = new ArrayList<Map<String, String>>();
        Map<String, String> audval = new HashMap<String, String>();
        audval.put("_value", "D20000");
        audience.add(audval);
        profileDefaults.put("ddm:audience", audience);

        List<Map<String, String>> access = new ArrayList<Map<String, String>>();
        Map<String, String> accval = new HashMap<String, String>();
        accval.put("_value", "NO_ACCESS");
        access.add(accval);
        profileDefaults.put("ddm:accessRights", access);
    }

    /** profile values */
    private Map<String, List<Map<String, String>>> profileFields = new HashMap<String, List<Map<String, String>>>();

    /** dcmi values */
    private Map<String, List<Map<String, String>>> dcmiFields = new HashMap<String, List<Map<String, String>>>();

    /**
     * Add a field to the profile section of the document
     *
     * @param field the profile field
     * @param value the value of the field
     */
    public void addProfileField(String field, String value)
    {
        this.addProfileField(field, value, null);
    }

    /**
     * Add a field to the profile section of the document, with the associated attributes
     *
     * @param field the profile field
     * @param value the value of the fields
     * @param attrs a map of attributes and their values
     */
    public void addProfileField(String field, String value, Map<String, String> attrs)
    {
        this.addToRegister(this.profileFields, field, value, attrs);
    }

    /**
     * Add a field to the dcmi section of the document
     *
     * @param field the dcmi field
     * @param value the value of the field
     */
    public void addDCMIField(String field, String value)
    {
        this.addDCMIField(field, value, null);
    }

    /**
     * Add a field to the dcmi section of the document with the associated attributes
     *
     * @param field the dcmi field
     * @param value the value fo the field
     * @param attrs a map of attributes and their values
     */
    public void addDCMIField(String field, String value, Map<String, String> attrs)
    {
        this.addToRegister(this.dcmiFields, field, value, attrs);
    }

    /**
     * Serialise the DDM as an XML string
     *
     * @return  the xml string
     * @throws IOException
     */
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
            this.populateElement(profile, this.profileFields, DDM.profileOrder, DDM.profileDefaults);
            ddm.appendChild(profile);
        }

        if (this.dcmiFields.size() > 0)
        {
            Element profile = new Element("ddm:dcmiMetadata", DDM_NAMESPACE);
            this.populateElement(profile, this.dcmiFields, null, null);
            ddm.appendChild(profile);
        }

        return this.xml2String(ddm);
    }

    /**
     * Add a field, with its value and attributes to the specified internal metadata register
     *
     * @param register  the internal metadata register
     * @param field the field
     * @param value the value
     * @param attrs a map of attributes
     */
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

    /**
     * Get the namespace for a given field.  The field should be prefixed with it's namespace, such as "dc:title"
     * and the namespace needs to be one of those recognised by this class
     *
     * @param field the field including namespace prefix
     * @return  the namespace URI
     */
    private String getNamespace(String field)
    {
        String[] bits = field.split(":");
        String ns = DDM.nsMap.get(bits[0]);
        return ns;
    }

    /**
     * Populate an element with the values from the given internal metadata register
     *
     * @param element
     * @param register
     * @param order
     */
    private void populateElement(Element element, Map<String, List<Map<String, String>>> register, List<String> order, Map<String, List<Map<String, String>>> defaults)
    {
        if (defaults == null)
        {
            defaults = new HashMap<String, List<Map<String, String>>>();
        }
        if (order != null)
        {
            for (String field : order)
            {
                this.doField(element, register, field, defaults.get(field));
            }
        }
        else
        {
            for (String field : register.keySet())
            {
                this.doField(element, register, field, defaults.get(field));
            }
        }
    }

    /**
     * Add the field from the internal metadata register to the specified element
     *
     * @param element
     * @param register
     * @param field
     */
    private void doField(Element element, Map<String, List<Map<String, String>>> register, String field, List<Map<String, String>> def)
    {
        String ns = this.getNamespace(field);

        List<Map<String, String>> entries = register.get(field);
        if (entries == null)
        {
            if (def != null)
            {
                entries = def;
            }
            else
            {
                return;
            }
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
