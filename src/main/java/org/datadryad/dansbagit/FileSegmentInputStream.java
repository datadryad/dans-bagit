package org.datadryad.dansbagit;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class FileSegmentInputStream extends InputStream
{
    private RandomAccessFile raf;
    private long size;
    private long pointer = 0;
    private boolean eof = false;

    public FileSegmentInputStream(RandomAccessFile file, long size)
    {
        super();
        this.raf = file;
        this.size = size;
    }

    @Override
    public int read() throws IOException
    {
        if (this.pointer >= this.size || this.eof)
        {
            return -1;
        }
        this.pointer++;

        int res = this.raf.read();
        if (res == -1) {
            this.eof = true;
        }
        return res;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        if (this.pointer >= this.size || this.eof)
        {
            return -1;
        }

        long max = this.size - this.pointer;
        long x = b.length < max ? b.length : max;
        int y = Math.toIntExact(x);

        byte[] ib = new byte[y];
        int res = this.raf.read(ib);

        if (res == -1)
        {
            this.eof = true;
            return -1;
        }
        this.pointer += res;

        System.arraycopy(ib, 0, b, 0, res);
        return res;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        if (this.pointer >= this.size || this.eof)
        {
            return -1;
        }

        byte[] ib = new byte[len];
        int res = this.read(ib);

        if (res == -1)
        {
            return -1;
        }

        System.arraycopy(ib, 0, b, off, res);

        if (res == -1) {
            this.eof = true;
        }
        return res;
    }
}
