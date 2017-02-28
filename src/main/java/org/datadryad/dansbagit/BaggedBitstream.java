package org.datadryad.dansbagit;

import java.io.InputStream;

public class BaggedBitstream
{
    private InputStream inputStream;
    private String filename;
    private String format;
    private String description;
    private String dataFileIdent;
    private String bundle;

    public BaggedBitstream(InputStream is, String filename, String format, String description, String dataFileIdent, String bundle)
    {
        this.inputStream = is;
        this.filename = filename;
        this.format = format;
        this.description = description;
        this.dataFileIdent = dataFileIdent;
        this.bundle = bundle;
    }

    public InputStream getInputStream()
    {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream)
    {
        this.inputStream = inputStream;
    }

    public String getFilename()
    {
        return filename;
    }

    public void setFilename(String filename)
    {
        this.filename = filename;
    }

    public String getFormat()
    {
        return format;
    }

    public void setFormat(String format)
    {
        this.format = format;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getDataFileIdent()
    {
        return dataFileIdent;
    }

    public void setDataFileIdent(String dataFileIdent)
    {
        this.dataFileIdent = dataFileIdent;
    }

    public String getBundle()
    {
        return bundle;
    }

    public void setBundle(String bundle)
    {
        this.bundle = bundle;
    }
}
