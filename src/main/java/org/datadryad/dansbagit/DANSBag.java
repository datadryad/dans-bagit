package org.datadryad.dansbagit;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
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
        String md5 = null;
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

    File zipFile = null;
    File workingDir = null;
    List<BagFileReference> fileRefs = new ArrayList<BagFileReference>();

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
        bfr.md5 = this.digestToString(md);
        bfr.description = description;
        bfr.format = format;
        bfr.size = (new File(filePath)).length();
        bfr.dataFileIdent = dataFileIdent;
        bfr.bundle = bundle;
        this.fileRefs.add(bfr);
    }

    public void setDDM(DDM ddm) {

    }

    public void setDIM(DIM dim) {

    }

    public void setDIM(DIM dim, String dataFileIdent) {

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

            // prepare our zipped output stream
            FileOutputStream dest = new FileOutputStream(this.zipFile);
            ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));

            String descriptions = "";
            String formats = "";
            String sizes = "";
            String manifest = "";
            String tagmanifest = "";

            // write the bitstreams, and gather their metadata for the tag files as we go through
            for (BagFileReference bfr : this.fileRefs)
            {
                // update description tag file contents
                if (bfr.description != null && !"".equals(bfr.description))
                {
                    descriptions = descriptions + bfr.description + "\t" + bfr.internalPath + "\n";
                }

                // update format tag file contents
                if (bfr.format != null && !"".equals(bfr.format))
                {
                    formats = formats + bfr.format + "\t" + bfr.internalPath + "\n";
                }

                // update size tag file contents
                if (bfr.size != -1)
                {
                    sizes = sizes + Long.toString(bfr.size) + "\t" + bfr.internalPath + "\n";
                }

                // update the manifest
                if (bfr.md5 != null && !"".equals(bfr.md5))
                {
                    manifest = manifest + bfr.md5 + "\t" + bfr.internalPath + "\n";
                }

                this.writeToZip(bfr.getFile(), bfr.internalPath, out);
            }

            // write the custom tag files
            if (!"".equals(descriptions))
            {
                String checksum = this.writeToZip(descriptions, "bitstream-description.txt", out);
                tagmanifest = tagmanifest + checksum + "\tbitstream-description.txt" + "\n";
            }

            if (!"".equals(formats))
            {
                String checksum = this.writeToZip(formats, "bitstream-format.txt", out);
                tagmanifest = tagmanifest + checksum + "\tbitstream-format.txt" + "\n";
            }

            if (!"".equals(sizes))
            {
                String checksum = this.writeToZip(sizes, "bitstream-size.txt", out);
                tagmanifest = tagmanifest + checksum + "\tbitstream-size.txt" + "\n";
            }

            // write the checksum manifests
            if (!"".equals(manifest))
            {
                this.writeToZip(manifest, "manifest-md5.txt", out);
            }

            if (!"".equals(tagmanifest))
            {
                this.writeToZip(tagmanifest, "tagmanifest-md5.txt", out);
            }

            String bagitfile = "BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8";
            this.writeToZip(bagitfile, "bagit.txt", out);

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

        return this.digestToString(md);
    }
}