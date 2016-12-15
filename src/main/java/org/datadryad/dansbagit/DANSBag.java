package org.datadryad.dansbagit;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
    /**
     * Inner class to provide a reference to a file in the Bag.  Since the file in the bag
     * may have different types, different sources for its input stream, and different tag
     * properties, this allows us to provide a consistent wrapper for internal use.
     */
    class BagFileReference
    {
        public ZipEntry zipEntry = null;
        public File file = null;

        public String description = null;
        public String format = null;
        public long size = -1;
        String md5 = null;
        public String dataFileIdent = null;
        public String bundle = null;
    }

    File zipFile = null;
    File workingDir = null;

    /**
     * Create a BagIt object around a directory specified at the zipPath
     *
     * @param zipPath  Path to BagIt zip file.  This may exist or not.
     * @param workingDir    Path to BagIt working directory, where files will be copied to in transition (especially during construction)
     * @throws IOException
     */
    public DANSBag(String zipPath, String workingDir)
            throws IOException
    {
        this(new File(zipPath), new File(workingDir));
    }

    /**
     * Create a BagIt object around a file object provided
     *
     * @param zipFile  File object representing the BagIt zip.  This may exist or not
     * @param workingDir  File object representing the BagIt structure
     * @throws IOException
     */
    public DANSBag(File zipFile, File workingDir)
            throws IOException
    {
        this.zipFile = zipFile;
        this.workingDir = workingDir;

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

    public void addBitstream(InputStream is, String filename, String format, String description, String dataFileIdent, String bundle)
        throws IOException
    {
        // escape the dataFileIdent
        String dataFilename = sanitizeFilename(dataFileIdent);

        // get the correct folder/filename for the bitstream
        String targetDir = this.workingDir.getAbsolutePath() + File.separator + "data" + File.separator + dataFilename + File.separator + bundle;
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
        bfr.md5 = this.digestToString(md);
        bfr.description = description;
        bfr.format = format;
        bfr.size = (new File(filePath)).length();
        bfr.dataFileIdent = dataFileIdent;
        bfr.bundle = bundle;
    }

    public void setDDM(DDM ddm) {

    }

    public void setDIM(DIM dim) {

    }

    public void setDIM(DIM dim, String dataFileIdent) {

    }

    private String sanitizeFilename(String inputName) {
        return inputName.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
    }

    private String digestToString(MessageDigest md) {
        byte[] b = md.digest();
        String result = "";
        for (int i=0; i < b.length; i++)
        {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }
        return result;
    }
}