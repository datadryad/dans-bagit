package org.datadryad.dansbagit.test;

import org.apache.commons.io.FileUtils;
import org.datadryad.dansbagit.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.*;
import java.util.*;

public class BagItTest
{
    private List<String> cleanup = new ArrayList<String>();

    @Before
    public void setUp()
    {
        this.cleanup = new ArrayList<String>();
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
    public void testMakeBag()
            throws IOException, Exception
    {
        String workingDir = System.getProperty("user.dir") + "/src/test/resources/working/testmakebag";
        this.cleanup.add(workingDir);

        String zipPath = System.getProperty("user.dir") + "/src/test/resources/working/testmakebag.zip";
        this.cleanup.add(zipPath);

        File f = new File(workingDir);
        if (!f.exists())
        {
            f.mkdirs();
        }

        DANSBag db = new DANSBag("testbag", zipPath, workingDir);

        // try adding a random set of bytes as a bitstream
        InputStream is = new ByteArrayInputStream("asdklfjqwoie weoifjwoef jwoeifjwefpji".getBytes());
        db.addBitstream(is, "myfile.txt", "text/plain", "some plain text", "10.whatever/ident/1", "ORIGINAL");

        // set some metadata on that data file
        DIM dfdim = new DIM();
        dfdim.addDSpaceField("dc.contributor.author", "Author A");
        dfdim.addDSpaceField("dc.contributor.author", "Author B");
        dfdim.addDSpaceField("dc.identifier", "10.1234/ident/1");
        db.addDatafileDIM(dfdim, "10.whatever/ident/1");

        // create some DDM metadata
        DDM ddm = new DDM();

        ddm.addProfileField("dc:title", "The Title");
        ddm.addProfileField("dc:creator", "Creator 1");
        ddm.addProfileField("dc:creator", "Creator 2");
        ddm.addProfileField("dc:creator", "Creator 1");

        Map<String, String> attrs = new HashMap<String, String>();
        attrs.put("xsi:type", "id-type:DOI");

        ddm.addDCMIField("dcterms:hasPart", "10.1234/ident");
        ddm.addDCMIField("dcterms:identifier", "10.4321/main", attrs);

        db.setDDM(ddm);

        // create some DIM metadata
        DIM dim = new DIM();
        dim.addDSpaceField("dc.contributor.author", "Author 1");
        dim.addDSpaceField("dc.contributor.author", "Author 2");
        dim.addDSpaceField("dc.identifier", "10.1234/ident/a");

        db.setDatasetDIM(dim);

        // finally output to file
        db.writeToFile();

        // check that we can get all the properties from the object
        String md5 = db.getMD5();
        long size = db.size();
        assert db.getZipName().equals("testmakebag.zip");

        // lets read the whole file through a normal input stream
        InputStream input = db.getInputStream();
        byte[] whole = this.readInput(input, 8096);
        input.close();

        // now try reading the whole file through the FileSegementIterator
        FileSegmentIterator fsi = db.getSegmentIterator(1000);
        byte[] allparts = new byte[0];
        while (fsi.hasNext())
        {
            FileSegmentInputStream fsis = fsi.next();
            byte[] seg = this.readInput(fsis, 500);
            allparts = this.combine(allparts, seg);
        }

        assert Arrays.equals(whole, allparts);

        db.cleanupWorkingDir();
        db.cleanupZip();
    }

    private byte[] readInput(InputStream is, int bufferSize)
            throws Exception
    {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[bufferSize];
        int length;
        while ((length = is.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toByteArray();
    }

    public byte[] combine(byte[] a, byte[] b){
        int length = a.length + b.length;
        byte[] result = new byte[length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}
