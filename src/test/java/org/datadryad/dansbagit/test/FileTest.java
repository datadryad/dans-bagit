package org.datadryad.dansbagit.test;

import org.apache.commons.io.FileUtils;
import org.datadryad.dansbagit.FileSegmentInputStream;
import org.datadryad.dansbagit.FileSegmentIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileTest
{
    private List<String> cleanup = new ArrayList<String>();
    private String fileContent = null;
    private String testfile = null;

    @Before
    public void setUp()
            throws Exception
    {
        this.cleanup = new ArrayList<String>();

        String txt = "";
        for (int i = 0; i < 10; i++)
        {
            for (int j = 0; j < 100; j++)
            {
                txt += Integer.toString(i);
            }
        }
        this.fileContent = txt;

        this.testfile = System.getProperty("user.dir") + "/src/test/resources/working/testfile.txt";
        this.cleanup.add(this.testfile);

        PrintWriter out = new PrintWriter(testfile, "UTF-8");
        out.write(txt);
        out.close();
    }

    @After
    public void tearDown()
            throws IOException
    {
        for (String path : this.cleanup)
        {
            File f = new File(path);
            if (!f.exists())
            {
                continue;
            }

            if (f.isDirectory())
            {
                FileUtils.deleteDirectory(f);
            }
            else
            {
                f.delete();
            }
        }
    }

    @Test
    public void testFileSegmentInputStreamWholeFile()
            throws Exception
    {
        // first let's try reading the whole file
        RandomAccessFile raf = new RandomAccessFile(this.testfile, "r");

        // do it character by character
        FileSegmentInputStream fsis1 = new FileSegmentInputStream(raf, raf.length());
        byte[] bytes = new byte[1000];
        int c;
        int i = 0;
        while ((c = fsis1.read()) != -1) {
            bytes[i] = (byte) c;
            i++;
        }
        String got1 = new String(bytes, "UTF-8");
        assert got1.equals(this.fileContent);

        // reset the reader
        raf.seek(0);
        FileSegmentInputStream fsis2 = new FileSegmentInputStream(raf, raf.length());

        // do it using the buffer, using a buffer size that requires two visits
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[600];
        int length;
        while ((length = fsis2.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        String got2 = result.toString("UTF-8");
        assert got2.equals(this.fileContent);

        // reset the reader
        raf.seek(0);
        FileSegmentInputStream fsis3 = new FileSegmentInputStream(raf, raf.length());

        // doing with a single buffer array and several calls to fill the byte array
        byte[] full = new byte[1000];
        int off = 0;
        while ((length = fsis3.read(full, off, 400)) != -1)
        {
            off += 400;
        }
        String got3 = new String(full, "UTF-8");
        assert got3.equals(this.fileContent);
    }

    @Test
    public void testFileSegmentInputStreamPartial()
            throws Exception
    {
        RandomAccessFile raf = new RandomAccessFile(this.testfile, "r");
        raf.seek(200);

        // do it character by character
        FileSegmentInputStream fsis1 = new FileSegmentInputStream(raf, 450);
        byte[] bytes = new byte[450];
        int c;
        int i = 0;
        while ((c = fsis1.read()) != -1) {
            bytes[i] = (byte) c;
            i++;
        }
        String got1 = new String(bytes, "UTF-8");
        assert got1.equals(this.fileContent.substring(200, 650));

        // reset the reader
        raf.seek(300);
        FileSegmentInputStream fsis2 = new FileSegmentInputStream(raf, 550);

        // do it using the buffer, using a buffer size that requires several visits
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[100];
        int length;
        while ((length = fsis2.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        String got2 = result.toString("UTF-8");
        assert got2.equals(this.fileContent.substring(300, 850));

        // reset the reader
        raf.seek(150);
        FileSegmentInputStream fsis3 = new FileSegmentInputStream(raf, 120);

        // doing with a single buffer array and several calls to fill the byte array
        byte[] full = new byte[120];
        int off = 0;
        while ((length = fsis3.read(full, off, 30)) != -1)
        {
            off += 30;
        }
        String got3 = new String(full, "UTF-8");
        assert got3.equals(this.fileContent.substring(150, 270));
    }

    @Test
    public void testFileSegmentIteratorNoMD5()
            throws Exception
    {
        FileSegmentIterator fsi = new FileSegmentIterator(new File(this.testfile), 100, false);

        int i = 1;
        String got = "";
        while (fsi.hasNext())
        {
            FileSegmentInputStream fsis = fsi.next();

            // read the input stream
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[50];
            int length;
            while ((length = fsis.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            got += result.toString("UTF-8");

            assert got.equals(this.fileContent.substring(0, i * 100));
            i++;
        }

        assert got.equals(this.fileContent);
    }

    @Test
    public void testFileSegmentIteratorWithMD5()
            throws Exception
    {
        FileSegmentIterator fsi = new FileSegmentIterator(new File(this.testfile), 100, true);

        int i = 1;
        String got = "";
        while (fsi.hasNext())
        {
            FileSegmentInputStream fsis = fsi.next();
            String md5 = fsis.getMd5();
            assert md5 != null;

            // read the input stream
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[50];
            int length;
            while ((length = fsis.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            got += result.toString("UTF-8");

            assert got.equals(this.fileContent.substring(0, i * 100));
            i++;
        }

        assert got.equals(this.fileContent);
    }
}
