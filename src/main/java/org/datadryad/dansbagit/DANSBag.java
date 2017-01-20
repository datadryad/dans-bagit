package org.datadryad.dansbagit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Main class which manages interactions with a Dryad/DANS formatted Bag
 *
 * This allows the BagIt packages to be constructed from files on disk, or for a
 * zipped BagIt to be read and opened.
 *
 * The DANS Bag format is as follows:
 *
 * <pre>
 * base directory/
 * |    bagit.txt
 * |    bitstream-description.txt
 * |    bitstream-format.txt
 * |    bitstream-size.txt
 * |    manifest-md5.txt
 * |    tagmanifest-md5.txt
 * \--- metadata
 *      |   dataset.xml
 *      |   files.xml
 * \--- data/
 *      |   metadata.xml
 *      \--- [Data File DOI]
 *          |   metadata.xml
 *          \--- [DSpace Bundle Name]
 *              | [DSpace Bitstreams]
 *</pre>
 */
public class DANSBag
{
    private static final int BUFFER = 8192;

    /**
     * Inner class to provide a reference to a file in the Bag.  Since the file in the bag
     * may have different types, different sources for its input stream, and different tag
     * properties, this allows us to provide a consistent wrapper for internal use.
     */
    class BagFileReference
    {
        public ZipEntry zipEntry = null;
        private File file = null;

        public String fullPath = null;
        public String internalPath = null;

        public String description = null;
        public String format = null;
        public long size = -1;
        public String md5 = null;
        public String dataFileIdent = null;
        public String bundle = null;

        public File getFile()
        {
            if (this.file == null) {
                this.file = new File(fullPath);
            }
            return this.file;
        }
    }

    private File zipFile = null;
    private File workingDir = null;
    private String name = null;
    private List<BagFileReference> fileRefs = new ArrayList<BagFileReference>();
    private DDM ddm = null;
    private DIM dim = null;
    private Map<String, DIM> subDim = new HashMap<String, DIM>();

    /**
     * Create a BagIt object around a directory specified at the zipPath
     *
     * @param zipPath  Path to BagIt zip file.  This may exist or not.
     * @param workingDir    Path to BagIt working directory, where files will be copied to in transition (especially during construction)
     * @throws IOException
     */
    public DANSBag(String name, String zipPath, String workingDir)
            throws IOException
    {
        this(name, new File(zipPath), new File(workingDir));
    }

    /**
     * Create a BagIt object around a file object provided
     *
     * @param zipFile  File object representing the BagIt zip.  This may exist or not
     * @param workingDir  File object representing the BagIt structure
     * @throws IOException
     */
    public DANSBag(String name, File zipFile, File workingDir)
            throws IOException
    {
        this.zipFile = zipFile;
        this.workingDir = workingDir;
        this.name = name;

        if (zipFile.exists())
        {
            // load the bag
            this.loadBag(zipFile);
        }
    }

    public void loadBag(File file)
            throws IOException
    {
        // TODO
    }

    public String getMD5()
            throws Exception
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can calculate the md5");
        }

        FileInputStream fis = new FileInputStream(this.zipFile);
        String md5 = DigestUtils.md5Hex(fis);
        fis.close();
        return md5;
    }

    public String getZipName()
    {
        return this.zipFile.getName();
    }

    public InputStream getInputStream()
            throws Exception
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can read the input stream");
        }

        return new FileInputStream(this.zipFile);
    }

    public FileSegmentIterator getSegmentIterator(long size, boolean md5)
            throws Exception
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can read the segments");
        }

        return new FileSegmentIterator(this.zipFile, size, md5);
    }

    public long size()
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can determine the size");
        }
        return this.zipFile.length();
    }

    public void addBitstream(InputStream is, String filename, String format, String description, String dataFileIdent, String bundle)
        throws IOException
    {
        // escape the dataFileIdent
        String dataFilename = Files.sanitizeFilename(dataFileIdent);

        // get the correct folder/filename for the bitstream
        String internalDir = "data" + File.separator + dataFilename + File.separator + bundle;
        String internalFile = internalDir + File.separator + filename;
        String targetDir = this.workingDir.getAbsolutePath() + File.separator + internalDir;
        String filePath = targetDir + File.separator + filename;

        // ensure that the target directory exists
        (new File(targetDir)).mkdirs();

        // wrap the input stream in something that can get the MD5 as we read it
        MessageDigest md = null;
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
        DigestInputStream dis = new DigestInputStream(is, md);

        // write the input stream to the working directory, in the appropriate folder
        OutputStream os = new FileOutputStream(filePath);
        IOUtils.copy(dis, os);

        // add the bitstream information to our internal data structure
        BagFileReference bfr = new BagFileReference();
        bfr.fullPath = filePath;
        bfr.internalPath = internalFile;
        bfr.md5 = Files.digestToString(md);
        bfr.description = description;
        bfr.format = format;
        bfr.size = (new File(filePath)).length();
        bfr.dataFileIdent = dataFileIdent;
        bfr.bundle = bundle;
        this.fileRefs.add(bfr);
    }

    public void setDDM(DDM ddm)
    {
        this.ddm = ddm;
    }

    public void setDatasetDIM(DIM dim)
    {
        this.dim = dim;
    }

    public void addDatafileDIM(DIM dim, String dataFileIdent)
    {
        this.subDim.put(dataFileIdent, dim);
    }


    public void writeToFile()
    {
        try
        {
            // if this bag was initialised from a zip file, we can't write back to it - just too
            // complicated.
            if (this.zipFile.exists()) {
                throw new RuntimeException("Cannot re-write a modified bag file.  You should either create a new bag file from the source files, or read in the old zip file and pass the components in here.");
            }

            String base = Files.sanitizeFilename(this.name);

            // prepare our zipped output stream
            FileOutputStream dest = new FileOutputStream(this.zipFile);
            ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));

            String descriptions = "";
            String formats = "";
            String sizes = "";
            String manifest = "";
            String tagmanifest = "";

            DANSFiles dfs = new DANSFiles();

            // write the bitstreams, and gather their metadata for the tag files as we go through
            for (BagFileReference bfr : this.fileRefs)
            {
                // update description tag file contents
                if (bfr.description != null && !"".equals(bfr.description))
                {
                    descriptions = descriptions + bfr.description + "\t" + bfr.internalPath + "\n";
                    dfs.addFileMetadata(bfr.internalPath, "dc:description", bfr.description);
                }

                // update format tag file contents
                if (bfr.format != null && !"".equals(bfr.format))
                {
                    formats = formats + bfr.format + "\t" + bfr.internalPath + "\n";
                    dfs.addFileMetadata(bfr.internalPath, "dc:format", bfr.format);
                }

                // update size tag file contents
                if (bfr.size != -1)
                {
                    sizes = sizes + Long.toString(bfr.size) + "\t" + bfr.internalPath + "\n";
                    dfs.addFileMetadata(bfr.internalPath, "dcterms:extent", Long.toString(bfr.size));
                }

                // update the manifest
                if (bfr.md5 != null && !"".equals(bfr.md5))
                {
                    manifest = manifest + bfr.md5 + "\t" + bfr.internalPath + "\n";
                    dfs.addFileMetadata(bfr.internalPath, "premis:messageDigestAlgorithm", "MD5");
                    dfs.addFileMetadata(bfr.internalPath, "premis:messageDigest", bfr.md5);
                }

                this.writeToZip(bfr.getFile(), base + "/" + bfr.internalPath, out);
            }

            // write the DANS files.xml document
            String filesChecksum = this.writeToZip(dfs.toXML(), base + "/metadata/files.xml", out);
            tagmanifest = tagmanifest + filesChecksum + "\t" + "metadata/files.xml" + "\n";

            // write the DANS dataset.xml document
            if (this.ddm != null)
            {
                String datasetChecksum = this.writeToZip(this.ddm.toXML(), base + "/metadata/dataset.xml", out);
                tagmanifest = tagmanifest + datasetChecksum + "\t" + "metadata/dataset.xml" + "\n";
            }

            // write the primary dim file
            if (this.dim != null)
            {
                String dimChecksum = this.writeToZip(this.dim.toXML(), base + "/data/metadata.xml", out);
                manifest = manifest + dimChecksum + "\t" + "data/metadata.xml" + "\n";
            }

            // write the datafile dim files
            for (String ident : this.subDim.keySet())
            {
                String dataDir = Files.sanitizeFilename(ident);
                String zipPath = "data/" + dataDir + "/metadata.xml";
                DIM dim = this.subDim.get(ident);
                String subDimChecksum = this.writeToZip(dim.toXML(), base + "/" + zipPath, out);
                manifest = manifest + subDimChecksum + "\t" + zipPath + "\n";
            }

            // write the custom tag files
            if (!"".equals(descriptions))
            {
                String checksum = this.writeToZip(descriptions, base + "/bitstream-description.txt", out);
                tagmanifest = tagmanifest + checksum + "\tbitstream-description.txt" + "\n";
            }

            if (!"".equals(formats))
            {
                String checksum = this.writeToZip(formats, base + "/bitstream-format.txt", out);
                tagmanifest = tagmanifest + checksum + "\tbitstream-format.txt" + "\n";
            }

            if (!"".equals(sizes))
            {
                String checksum = this.writeToZip(sizes, base + "/bitstream-size.txt", out);
                tagmanifest = tagmanifest + checksum + "\tbitstream-size.txt" + "\n";
            }

            // write the checksum manifests
            if (!"".equals(manifest))
            {
                String manifestChecksum = this.writeToZip(manifest, base + "/manifest-md5.txt", out);
                tagmanifest = tagmanifest + manifestChecksum + "\tmanifest-md5.txt" + "\n";
            }

            // write the bagit.txt
            String bagitfile = "BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8";
            String bagitChecksum = this.writeToZip(bagitfile, base + "/bagit.txt", out);
            tagmanifest = tagmanifest + bagitChecksum + "\tbagit.txt" + "\n";

            // finally write the tag manifest
            if (!"".equals(tagmanifest))
            {
                this.writeToZip(tagmanifest, base + "/tagmanifest-md5.txt", out);
            }

            out.close();
        }
        // we need to conform to the old interface, so can only throw RuntimeExceptions
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
    }


    public void cleanupWorkingDir()
            throws IOException
    {
        if (this.workingDir.exists())
        {
            FileUtils.deleteDirectory(this.workingDir);
        }
    }

    public void cleanupZip()
    {
        if (this.zipFile.exists())
        {
            this.zipFile.delete();
        }
    }

    /**
     * Write the file referenced by the file handle to the given path inside the given zip output stream
     *
     * @param file  The file reference
     * @param path  The path within the zip file to store a copy of the file
     * @param out   The ZipOutputStream to write the file to
     * @return  The MD5 digest of the file
     * @throws FileNotFoundException
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private String writeToZip(File file, String path, ZipOutputStream out)
            throws FileNotFoundException, IOException, NoSuchAlgorithmException
    {
        FileInputStream fi = new FileInputStream(file);
        return this.writeToZip(fi, path, out);
    }

    /**
     * Write a text file containing the supplied string to the given path inside the given zip output stream
     *
     * @param str   The string to write into a file
     * @param path  The path within the zip file to store the resulting text file
     * @param out   The ZipOutputStream to write the file to
     * @return  The MD5 digest of the resulting text file
     * @throws FileNotFoundException
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private String writeToZip(String str, String path, ZipOutputStream out)
            throws FileNotFoundException, IOException, NoSuchAlgorithmException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(str.getBytes());
        return this.writeToZip(bais, path, out);
    }

    /**
     * Write the data from the input stream to the given path inside the given zip output stream
     * @param fi    InputStream to source data from
     * @param path  The path within the zip file to store the resulting file
     * @param out   The ZipOutputStream to write the file to
     * @return  The MD5 digest of the resulting text file
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private String writeToZip(InputStream fi, String path, ZipOutputStream out)
            throws IOException, NoSuchAlgorithmException
    {
        MessageDigest md = MessageDigest.getInstance("MD5");
        BufferedInputStream origin = new BufferedInputStream(fi, BUFFER);
        DigestInputStream dis = new DigestInputStream(origin, md);

        ZipEntry entry = new ZipEntry(path);
        out.putNextEntry(entry);
        int count;
        byte data[] = new byte[BUFFER];
        while((count = dis.read(data, 0, BUFFER)) != -1) {
            out.write(data, 0, count);
        }
        origin.close();

        return Files.digestToString(md);
    }
}