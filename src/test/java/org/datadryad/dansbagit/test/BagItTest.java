package org.datadryad.dansbagit.test;

import static org.junit.Assert.*;

import org.apache.commons.io.FileUtils;
import org.datadryad.dansbagit.DANSBag;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
        db.writeToFile();
    }
}
