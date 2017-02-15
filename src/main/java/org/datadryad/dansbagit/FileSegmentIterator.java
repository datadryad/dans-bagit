package org.datadryad.dansbagit;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

/**
 * An Iterator which yields FileSegmentInputStream instances.  This is used to iterate through a
 * large zip file in chunks.
 */
public class FileSegmentIterator implements Iterator<FileSegmentInputStream>
{
    private RandomAccessFile raf = null;
    private long pointer = 0;
    private long size = -1;
    private boolean md5 = false;

    /**
     * Create a new iterator around the given file, with segments of a given size.
     *
     * @param file  A file object to iterate over
     * @param size  the size of the chunks each iteration will provide
     * @param md5   whether to calculate the md5 as the chunks are created (has a performance overhead)
     * @throws IOException if there are any problems reading the file
     */
    public FileSegmentIterator(File file, long size, boolean md5)
        throws IOException
    {
        this.raf = new RandomAccessFile(file, "r");
        this.raf.seek(0);
        this.size = size;
        this.md5 = md5;
    }

    /**
     * Is there another segment to be read?
     *
     * @return  true if there is another segment, false if not
     */
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

    /**
     * Get the next input stream
     *
     * @return  an input stream for the next segment
     */
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

    /**
     * Set the pointer to the appropriate place in the file
     *
     * @param point offset from the start of the file
     */
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

    /**
     * Calculate the input stream for the input stream.  This will read from the stream, so you
     * will need to reset the pointer after using this method
     *
     * @param fsis
     * @return
     */
    private String getMD5(InputStream fsis)
    {
        try
        {
            return Files.md5Hex(fsis);

            // if we could use DigestUtils (which we can't because DSpace) this would be a better way
            // return org.apache.commons.codec.digest.DigestUtils.md5Hex(fsis);
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
