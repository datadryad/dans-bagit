package org.datadryad.dansbagit;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * InputStream which provides access to a specific sgment of a file
 */
public class FileSegmentInputStream extends InputStream
{
    private RandomAccessFile raf;
    private long size;
    private long pointer = 0;
    private boolean eof = false;
    private String md5;

    /**
     * Create the input stream around a random access file object.  The file object should already be seeked to the
     * appropriate start point.  This input stream will read "size" bytes and then declare EOF
     *
     * @param file  the file object to read from
     * @param size  the maximum number of bytes to read before declaring EOF
     */
    public FileSegmentInputStream(RandomAccessFile file, long size)
    {
        super();
        this.raf = file;
        this.size = size;
    }

    /**
     * Get the MD5 (if set)
     * @return
     */
    public String getMd5()
    {
        return md5;
    }

    /**
     * Set the md5 value (this class does not calculate that value for you)
     *
     * @param md5
     */
    public void setMd5(String md5)
    {
        this.md5 = md5;
    }

    /**
     * Read the next single byte
     *
     * @return  an int representing the byte or -1 if EOF
     * @throws IOException
     */
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

    /**
     * Fill the byte array, as far as is possible, with up to b.length bytes
     *
     * @param b byte array to populate
     * @return  the number of bytes read or -1 if EOF
     * @throws IOException
     */
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

    /**
     * Fill the byte array as far as is possible, between b[off] and b[off+len]
     *
     * @param b the byte array to populate
     * @param off   where to start filling the byte array
     * @param len   how many bytes to read
     * @return  the number of bytes read or -1 if EOF
     * @throws IOException
     */
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
