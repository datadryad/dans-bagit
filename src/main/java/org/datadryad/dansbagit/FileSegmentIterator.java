package org.datadryad.dansbagit;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.zip.ZipEntry;

public class FileSegmentIterator implements Iterator<FileSegmentInputStream>
{
    private RandomAccessFile raf = null;
    private long pointer = 0;
    private long size = -1;
    private boolean md5 = false;

    public FileSegmentIterator(File file, long size, boolean md5)
            throws Exception
    {
        this.raf = new RandomAccessFile(file, "r");
        this.raf.seek(0);
        this.size = size;
        this.md5 = md5;
    }

    @Override
    public boolean hasNext()

    {
        try
        {
            return this.pointer < this.raf.length();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileSegmentInputStream next()
    {
        // first make sure the file is at the right point
        this.setFilePointer(this.pointer);

        String checksum = "";
        if (this.md5)
        {
            FileSegmentInputStream fsis1 = new FileSegmentInputStream(this.raf, this.size);
            checksum = this.getMD5(fsis1);
            try
            {
                fsis1.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            this.setFilePointer(this.pointer);
        }

        // then move the pointer for next time
        this.pointer += this.size;

        // now construct the input stream
        FileSegmentInputStream fsis2 = new FileSegmentInputStream(this.raf, this.size);
        if (!"".equals(checksum))
        {
            fsis2.setMd5(checksum);
        }
        return fsis2;
    }

    private void setFilePointer(long point)
    {
        try
        {
            this.raf.seek(point);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String getMD5(InputStream fsis)
    {
        try
        {
            // return org.apache.commons.codec.digest.DigestUtils.md5Hex(fsis);
            return Files.md5Hex(fsis);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
    }
}
