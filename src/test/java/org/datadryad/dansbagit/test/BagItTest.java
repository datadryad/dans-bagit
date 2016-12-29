package org.datadryad.dansbagit.test;

import static org.junit.Assert.*;

import org.apache.commons.io.FileUtils;
import org.datadryad.dansbagit.DANSBag;
import org.datadryad.dansbagit.DDM;
import org.datadryad.dansbagit.DIM;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            throws IOException
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

        DANSBag db = new DANSBag(zipPath, workingDir);

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
    }
}
