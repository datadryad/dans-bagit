package org.datadryad.dansbagit;

import java.util.HashMap;
import java.util.Map;

public class TagFile
{
    private Map<String, String> tags;

    public TagFile()
    {
        this.tags = new HashMap<String, String>();
    }

    public TagFile(Map<String, String> tags)
    {
        this.tags = tags;
    }

    public void add(String path, String tag)
    {
        this.tags.put(path, tag);
    }

    public boolean hasEntries()
    {
        return this.tags.size() > 0;
    }

    public String serialise()
    {
        StringBuilder sb = new StringBuilder();
        for (String path : this.tags.keySet())
        {
            String value = this.tags.get(path);
            sb.append(value).append("\t").append(path).append("\n");
        }
        return sb.toString();
    }
}
