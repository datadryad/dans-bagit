package org.datadryad.dansbagit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public class FileSegmentIterator implements Iterator<FileSegmentInputStream>
{
    private RandomAccessFile raf = null;
    private long pointer = 0;
    private long size = -1;

    public FileSegmentIterator(File file, long size)
            throws Exception
    {
        this.raf = new RandomAccessFile(file, "r");
        this.raf.seek(0);
        this.size = size;
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
        try
        {
            this.raf.seek(this.pointer);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        // then move the pointer for next time
        this.pointer += this.size;

        // now construct the input stream
        return new FileSegmentInputStream(this.raf, this.size);
    }
}
