package org.datadryad.dansbagit;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class TagFile
{
    private Map<String, String> tags;

    public TagFile()
    {
        this.tags = new HashMap<String, String>();
    }

    public TagFile(HashMap<String, String> tags)
    {
        this.tags = tags;
    }

    public void add(String path, String tag)
    {
        this.tags.put(path, tag);
    }

    public String getValue(String path)
    {
        return this.tags.get(path);
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

    public static TagFile parse(InputStream is)
    {
        Scanner s = new Scanner(is).useDelimiter("\\A");
        String result = s.hasNext() ? s.next() : "";

        TagFile tf = new TagFile();

        String[] lines = result.split("\\n");
        for (String line : lines)
        {
            int lastTab = line.lastIndexOf("\t");
            String val = line.substring(0, lastTab);
            String path = line.substring(lastTab + 1);
            tf.add(path, val);
        }

        return tf;
    }
}
