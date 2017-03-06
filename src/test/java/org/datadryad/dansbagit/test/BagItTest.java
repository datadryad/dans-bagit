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
        byte[] bytes = "asdklfjqwoie weoifjwoef jwoeifjwefpji".getBytes();
        InputStream is = new ByteArrayInputStream(bytes);
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
        assert db.getMD5() != null;
        long size = db.size();
        assert db.getZipName().equals("testmakebag.zip");

        assert db.getZipPath() != null;
        assert db.getDatasetDIM() != null;
        assert db.getWorkingDir() != null;

        Set<String> idents = db.dataFileIdents();
        assert idents.size() == 1;
        assert idents.contains("10.whatever/ident/1");

        for (String ident : idents)
        {
            assert db.getDatafileDIM(ident) != null;

            Set<String> bundles = db.listBundles(ident);
            assert bundles.size() == 1;
            assert bundles.contains("ORIGINAL");

            for (String bundle : bundles)
            {
                Set<BaggedBitstream> bbs = db.listBitstreams(ident, bundle);
                assert bbs.size() == 1;

                for (BaggedBitstream bb : bbs)
                {
                    assert bb.getDescription().equals("some plain text");
                    assert bb.getBundle().equals("ORIGINAL");
                    assert bb.getDataFileIdent().equals("10.whatever/ident/1");
                    assert bb.getFilename().equals("myfile.txt");
                    assert bb.getFormat().equals("text/plain");

                    InputStream bbis = bb.getInputStream();
                    assert bbis != null;

                    byte[] retrieved = new byte[bytes.length];
                    bbis.read(retrieved);
                    assert Arrays.equals(retrieved, bytes);
                }
            }
        }

        // lets read the whole file through a normal input stream
        InputStream input = db.getInputStream();
        byte[] whole = this.readInput(input, 8096);
        input.close();

        // now try reading the whole file through the FileSegementIterator
        FileSegmentIterator fsi = db.getSegmentIterator(1000, true);
        byte[] allparts = new byte[0];
        while (fsi.hasNext())
        {
            FileSegmentInputStream fsis = fsi.next();
            String cs = fsis.getMd5();
            assert cs != null;
            byte[] seg = this.readInput(fsis, 500);
            allparts = this.combine(allparts, seg);
        }

        assert Arrays.equals(whole, allparts);

        db.cleanupWorkingDir();
        db.cleanupZip();
    }

    @Test
    public void testSegments()
            throws Exception
    {
        Map<String, String> checksums = new HashMap<String, String>();

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

        // now try reading the whole file through the FileSegementIterator
        FileSegmentIterator fsi = db.getSegmentIterator(1000, true);
        int i = 1;
        while (fsi.hasNext())
        {
            FileSegmentInputStream fsis = fsi.next();
            String outPath = zipPath + "." + Integer.toString(i);
            this.cleanup.add(outPath);

            String cs = fsis.getMd5();
            checksums.put(outPath, cs);

            File targetFile = new File(outPath);
            java.nio.file.Files.copy(fsis, targetFile.toPath());
            // FileUtils.copyInputStreamToFile(fsis, targetFile);

            i++;
        }

        for (String path : checksums.keySet())
        {
            FileInputStream fis = new FileInputStream(new File(path));
            // String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
            String md5 = Files.md5Hex(fis);
            fis.close();
            assert md5.equals(checksums.get(path));

            System.out.println(path + " - " + md5);
        }
    }

    @Test
    public void testReadBag()
            throws Exception
    {
        String workingDir = System.getProperty("user.dir") + "/src/test/resources/working/testreadbag";
        this.cleanup.add(workingDir);

        String zipPath = System.getProperty("user.dir") + "/src/test/resources/bags/testreadbag.zip";

        File f = new File(workingDir);
        if (!f.exists())
        {
            f.mkdirs();
        }

        DANSBag db = new DANSBag(zipPath, workingDir);

        assert db.getWorkingDir() != null;
        assert db.getMD5() != null;
        assert db.getZipPath() != null;
        assert db.getZipName() != null;
        assert db.getInputStream() != null;
        assert db.getSegmentIterator(1000, true) != null;
        assert db.size() > -1;

        DIM dsDim = db.getDatasetDIM();
        assert dsDim != null;

        Set<String> idents = db.dataFileIdents();
        assert idents.size() == 1;
        assert idents.contains("10.whatever/ident/1");

        byte[] bytes = "asdklfjqwoie weoifjwoef jwoeifjwefpji".getBytes();
        for (String ident : idents)
        {
            DIM dfDim = db.getDatafileDIM(ident);
            assert dfDim != null;

            Set<String> bundles = db.listBundles(ident);
            assert bundles.size() == 1;
            assert bundles.contains("ORIGINAL");

            for (String bundle : bundles)
            {
                Set<BaggedBitstream> bbs = db.listBitstreams(ident, bundle);
                assert bbs.size() == 1;

                for (BaggedBitstream bb : bbs)
                {
                    assert bb.getDescription().equals("some plain text");
                    assert bb.getBundle().equals("ORIGINAL");
                    assert bb.getDataFileIdent().equals("10.whatever/ident/1");
                    assert bb.getFilename().equals("myfile.txt");
                    assert bb.getFormat().equals("text/plain");

                    InputStream bbis = bb.getInputStream();
                    assert bbis != null;

                    byte[] retrieved = new byte[bytes.length];
                    bbis.read(retrieved);
                    assert Arrays.equals(retrieved, bytes);
                }
            }
        }
    }

    @Test
    public void testReadReal()
            throws Exception
    {
        String workingDir = System.getProperty("user.dir") + "/src/test/resources/working/testreadbag";
        this.cleanup.add(workingDir);

        String zipPath = System.getProperty("user.dir") + "/src/test/resources/bags/21.zip";

        File f = new File(workingDir);
        if (!f.exists())
        {
            f.mkdirs();
        }

        DANSBag db = new DANSBag(zipPath, workingDir);

        assert db.getWorkingDir() != null;
        assert db.getMD5() != null;
        assert db.getZipPath() != null;
        assert db.getZipName() != null;
        assert db.getInputStream() != null;
        assert db.getSegmentIterator(1000, true) != null;
        assert db.size() > -1;

        DIM dsDim = db.getDatasetDIM();
        assert dsDim != null;

        Set<String> idents = db.dataFileIdents();
        assert idents.size() == 3;

        for (String ident : idents)
        {
            DIM dfDim = db.getDatafileDIM(ident);
            assert dfDim != null;

            Set<String> bundles = db.listBundles(ident);
            assert bundles.contains("ORIGINAL");

            for (String bundle : bundles)
            {
                Set<BaggedBitstream> bbs = db.listBitstreams(ident, bundle);
                // assert bbs.size() == 1;

                for (BaggedBitstream bb : bbs)
                {
                    // assert bb.getDescription() != null;
                    assert bb.getBundle() != null;
                    assert bb.getDataFileIdent() != null;
                    assert bb.getFilename() != null;
                    assert bb.getFormat() != null;

                    InputStream bbis = bb.getInputStream();
                    assert bbis != null;
                }
            }
        }
    }

    @Test
    public void testDIM()
            throws Exception
    {
        DIM dim = new DIM();
        dim.addDSpaceField("dc.identifier", "10.whatever/1");
        dim.addDSpaceField("dc.title", "my title");
        dim.addDSpaceField("dc.subject", "one");
        dim.addDSpaceField("dc.subject", "two");
        dim.addDSpaceField("dc.description.abstract", "abstract");
        dim.addDSpaceField("dc.description.other", "other");

        String xml = dim.toXML();
        assert xml != null;

        DIM dim2 = DIM.parse(new ByteArrayInputStream(xml.getBytes()));
        assert dim2.toXML().equals(xml);

        Set<String> fields = dim2.listDSpaceFields();
        assert fields.size() == 5;
        assert fields.contains("dc.identifier");
        assert fields.contains("dc.title");
        assert fields.contains("dc.subject");
        assert fields.contains("dc.description.abstract");
        assert fields.contains("dc.description.other");

        for (String field : fields)
        {
            List<String> values = dim2.getDSpaceFieldValues(field);
            if (field.equals("dc.identifier"))
            {
                assert values.size() == 1;
                assert values.get(0).equals("10.whatever/1");
            }
            if (field.equals("dc.title"))
            {
                assert values.size() == 1;
                assert values.get(0).equals("my title");
            }
            if (field.equals("dc.subject"))
            {
                assert values.size() == 2;
            }
            if (field.equals("dc.description.abstract"))
            {
                assert values.size() == 1;
                assert values.get(0).equals("abstract");
            }
            if (field.equals("dc.description.other"))
            {
                assert values.size() == 1;
                assert values.get(0).equals("other");
            }
        }
    }

    ////////////////////////////////////////////////////////////

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
