package org.datadryad.dansbagit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import sun.plugin2.message.Message;

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
    private static Logger log = Logger.getLogger(DANSBag.class);

    /** Buffer size to be used when chunking through input streams */
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
        public String sha1 = null;
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
     * Create a BagIt with the given name, using the provided zip as input or output, and with
     * the given working directory for temporary storage
     *
     * @param name      name to use for the bag.  This will form the top level directory inside the zip
     * @param zipPath   path to read in or output zip content
     * @param workingDir    directory used for temporary storage
     */
    public DANSBag(String name, String zipPath, String workingDir)
    {
        this(name, new File(zipPath), new File(workingDir));
    }

    /**
     * Create a BagIt with the given name, using the provided zip as input or output, and with
     * the given working directory for temporary storage
     *
     * @param name      name to use for the bag.  This will form the top level directory inside the zip
     * @param zipFile  path to read in or output zip content
     * @param workingDir    directory used for temporary storage
     */
    public DANSBag(String name, File zipFile, File workingDir)
    {
        this.zipFile = zipFile;
        this.workingDir = workingDir;
        this.name = name;
        log.debug("Creating DANSBag object around zipfile " + zipFile.getAbsolutePath() + " using working directory " + workingDir.getAbsolutePath() + " with name " + name);

        if (zipFile.exists())
        {
            log.debug("Zipfile " + zipFile.getAbsolutePath() + " exists, loading data from there");
            // load the bag
            this.loadBag(zipFile);
        }
    }

    /**
     * Load state from the given zip file
     *
     * Not yet implemented
     *
     * @param file
     */
    public void loadBag(File file)
    {
        // TODO
    }

    /**
     * Get the full path to the working directory
     *
     * @return path to the working directory
     */
    public String getWorkingDir()
    {
        return this.workingDir.getAbsolutePath();
    }

    /**
     * Get the MD5 of the zip.  The zip must exist for this to happen, so you either need to have
     * created this object around a zip file, or have called writeFile first.  If you try to call
     * it otherwise, you will get a RuntimeException
     *
     * @return  the MD5 hex string for the zip file
     * @throws IOException  if there's a problem reading the file
     */
    public String getMD5()
        throws IOException
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can calculate the md5");
        }

        try
        {
            FileInputStream fis = new FileInputStream(this.zipFile);
            String md5 = Files.md5Hex(fis);
            fis.close();
            return md5;

            // if we could use DigestUtils (which we can't because DSpace) this would be quicker and cleaner this way
            // String md5 = DigestUtils.md5Hex(fis);
        }
        catch(NoSuchAlgorithmException e)
        {
            // this shouldn't happen
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the name of the zip file.  You can only do this once the zip file exists, otherwise you will get a RuntimeException
     *
     * @return  the name of the zip file
     */
    public String getZipName()
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can ask questions about the zip");
        }
        return this.zipFile.getName();
    }

    /**
     * Get the full path to the zip file.  You can only do this once the zip file exists, otherwise you will get a RuntimeException
     *
     * @return the full path to the zip file
     */
    public String getZipPath()
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can ask questions about the zip");
        }
        return this.zipFile.getAbsolutePath();
    }

    /**
     * Get an input stream for the entire zip file.  You can only do this once the zip file exists, otherwise you will get a RuntimeException
     *
     * @return an input stream from which you can retrieve the entire zip file
     *
     * @throws Exception
     */
    public InputStream getInputStream()
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can read the input stream");
        }

        try
        {
            return new FileInputStream(this.zipFile);
        }
        catch (FileNotFoundException e)
        {
            // this can't happen, as we've already checked
            throw new RuntimeException(e);
        }
    }

    /**
     * Get an iterator which will allow you to iterate over input streams for defined size chunks of the zip file.
     *
     * You can only do this once the zip file exists, otherwise you will get a RuntimeException
     *
     * @param size  the size (in bytes) of the chunks (all except the final chunk will be this size)
     * @param md5   whether to calculate the md5 of each chunk as it is read
     * @return  a file segment iterator which can be used to retrieve input streams for subsequent chunks
     * @throws Exception
     */
    public FileSegmentIterator getSegmentIterator(long size, boolean md5)
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can read the segments");
        }

        try
        {
            return new FileSegmentIterator(this.zipFile, size, md5);
        }
        catch (IOException e)
        {
            // shouldn't happen, as we have checked the file's existence already
            throw new RuntimeException(e);
        }

    }

    /**
     * How big is the zip file.  You can only do this once the zip file exists, otherwise you will get a RuntimeException
     *
     * @return size of zip file in bytes
     */
    public long size()
    {
        if (!this.zipFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can determine the size");
        }
        return this.zipFile.length();
    }

    /**
     * Add a bitstream to the bag.  This will stage the file in the working directory.
     *
     * @param is    input stream where the bitstream can be read from
     * @param filename  the filename
     * @param format    the mimetype of the file
     * @param description   a description of the file
     * @param dataFileIdent     an identifier for the data file to which this bitstream belongs
     * @param bundle    the DSpace bundle the bitstream came from
     * @throws IOException
     */
    public void addBitstream(InputStream is, String filename, String format, String description, String dataFileIdent, String bundle)
        throws IOException
    {
        log.info("Adding bitstream to DANSBag: filename= " + filename + "; format= " + format + "; data_file=" + dataFileIdent + "; bundle=" + bundle);

        // escape the dataFileIdent
        String dataFilename = Files.sanitizeFilename(dataFileIdent);
        log.debug("Sanitised dataFileIdent from " + dataFileIdent + " to " + dataFilename);

        // get the correct folder/filename for the bitstream
        String internalDir = "data" + File.separator + dataFilename + File.separator + bundle;
        String internalFile = internalDir + File.separator + filename;
        String targetDir = this.workingDir.getAbsolutePath() + File.separator + internalDir;
        String filePath = targetDir + File.separator + filename;
        log.info("Bistream will be temporarily staged at " + filePath);
        log.info("Bitstream will be written to internal zip path " + internalFile);

        // ensure that the target directory exists
        (new File(targetDir)).mkdirs();

        // wrap the input stream in something that can get the MD5 as we read it
        MessageDigest mdmd5 = null;
        MessageDigest mdsha1 = null;
        try
        {
            mdmd5 = MessageDigest.getInstance("MD5");
            mdsha1 = MessageDigest.getInstance("SHA-1");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
        DigestInputStream inner = new DigestInputStream(is, mdmd5);
        DigestInputStream dis = new DigestInputStream(inner, mdsha1);

        // write the input stream to the working directory, in the appropriate folder
        OutputStream os = new FileOutputStream(filePath);
        IOUtils.copy(dis, os);

        // add the bitstream information to our internal data structure
        BagFileReference bfr = new BagFileReference();
        bfr.fullPath = filePath;
        bfr.internalPath = internalFile;
        bfr.md5 = Files.digestToString(mdmd5);
        bfr.sha1 = Files.digestToString(mdsha1);
        bfr.description = description;
        bfr.format = format;
        bfr.size = (new File(filePath)).length();
        bfr.dataFileIdent = dataFileIdent;
        bfr.bundle = bundle;
        this.fileRefs.add(bfr);
    }

    /**
     * Set the DDM metdata object for this bag
     *
     * @param ddm
     */
    public void setDDM(DDM ddm)
    {
        this.ddm = ddm;
    }

    /**
     * Set the dataset DIM metadata for this bag
     *
     * @param dim
     */
    public void setDatasetDIM(DIM dim)
    {
        this.dim = dim;
    }

    /**
     * Set the data file DIM metadata for the given data file identifier
     *
     * @param dim
     * @param dataFileIdent     the identifier for the data file
     */
    public void addDatafileDIM(DIM dim, String dataFileIdent)
    {
        this.subDim.put(dataFileIdent, dim);
    }

    /**
     * Write the in-memory information and contents of the working directory to the zip file.
     *
     * You can only do this once, and it will refuse to run again if the zip file is already present.  If you want to
     * run it again in the same thread you'll need to call cleanupZip first.  Also, you shouldn't need to do that - only
     * call this when you have finished assembling the bag.
     */
    public void writeToFile()
    {
        try
        {
            // if this bag was initialised from a zip file, we can't write back to it - just too
            // complicated.
            if (this.zipFile.exists()) {
                log.error("Attempt to write bag when zip already exists");
                throw new RuntimeException("Cannot re-write a modified bag file.  You should either create a new bag file from the source files, or read in the old zip file and pass the components in here.");
            }

            log.info("Writing bag to file " + this.zipFile.getAbsolutePath());

            String base = Files.sanitizeFilename(this.name);

            // prepare our zipped output stream
            FileOutputStream dest = new FileOutputStream(this.zipFile);
            ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));

            String descriptions = "";
            String formats = "";
            String sizes = "";
            String md5Manifest = "";
            String sha1Manifest = "";
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

                // update the manifests
                if (bfr.md5 != null && !"".equals(bfr.md5))
                {
                    md5Manifest = md5Manifest + bfr.md5 + "\t" + bfr.internalPath + "\n";
                    dfs.addFileMetadata(bfr.internalPath, "premis:messageDigestAlgorithm", "MD5");
                    dfs.addFileMetadata(bfr.internalPath, "premis:messageDigest", bfr.md5);
                }

                if (bfr.sha1 != null && !"".equals(bfr.sha1))
                {
                    sha1Manifest = sha1Manifest + bfr.sha1 + "\t" + bfr.internalPath + "\n";
                    dfs.addFileMetadata(bfr.internalPath, "premis:messageDigestAlgorithm", "MD5");
                    dfs.addFileMetadata(bfr.internalPath, "premis:messageDigest", bfr.md5);
                }

                this.writeToZip(bfr.getFile(), base + "/" + bfr.internalPath, out);
            }

            // write the DANS files.xml document
            Map<String, String> filesChecksums = this.writeToZip(dfs.toXML(), base + "/metadata/files.xml", out);
            tagmanifest = tagmanifest + filesChecksums.get("md5") + "\t" + "metadata/files.xml" + "\n";

            // write the DANS dataset.xml document
            if (this.ddm != null)
            {
                Map<String, String> datasetChecksums = this.writeToZip(this.ddm.toXML(), base + "/metadata/dataset.xml", out);
                tagmanifest = tagmanifest + datasetChecksums.get("md5") + "\t" + "metadata/dataset.xml" + "\n";
            }

            // write the primary dim file
            if (this.dim != null)
            {
                Map<String, String> dimChecksums = this.writeToZip(this.dim.toXML(), base + "/data/metadata.xml", out);
                md5Manifest = md5Manifest + dimChecksums.get("md5") + "\t" + "data/metadata.xml" + "\n";
                sha1Manifest = sha1Manifest + dimChecksums.get("sha-1") + "\t" + "data/metadata.xml" + "\n";
            }

            // write the datafile dim files
            for (String ident : this.subDim.keySet())
            {
                String dataDir = Files.sanitizeFilename(ident);
                String zipPath = "data/" + dataDir + "/metadata.xml";
                DIM dim = this.subDim.get(ident);
                Map<String, String> subDimChecksums = this.writeToZip(dim.toXML(), base + "/" + zipPath, out);
                md5Manifest = md5Manifest + subDimChecksums.get("md5") + "\t" + zipPath + "\n";
                sha1Manifest = sha1Manifest + subDimChecksums.get("sha-1") + "\t" + zipPath + "\n";
            }

            // write the custom tag files
            if (!"".equals(descriptions))
            {
                Map<String, String> checksums = this.writeToZip(descriptions, base + "/bitstream-description.txt", out);
                tagmanifest = tagmanifest + checksums.get("md5") + "\tbitstream-description.txt" + "\n";
            }

            if (!"".equals(formats))
            {
                Map<String, String> checksums = this.writeToZip(formats, base + "/bitstream-format.txt", out);
                tagmanifest = tagmanifest + checksums.get("md5") + "\tbitstream-format.txt" + "\n";
            }

            if (!"".equals(sizes))
            {
                Map<String, String> checksums = this.writeToZip(sizes, base + "/bitstream-size.txt", out);
                tagmanifest = tagmanifest + checksums.get("md5") + "\tbitstream-size.txt" + "\n";
            }

            // write the checksum manifests
            if (!"".equals(md5Manifest))
            {
                Map<String, String> manifestChecksums = this.writeToZip(md5Manifest, base + "/manifest-md5.txt", out);
                tagmanifest = tagmanifest + manifestChecksums.get("md5") + "\tmanifest-md5.txt" + "\n";
            }

            if (!"".equals(sha1Manifest))
            {
                Map<String, String> manifestChecksums = this.writeToZip(sha1Manifest, base + "/manifest-sha1.txt", out);
                tagmanifest = tagmanifest + manifestChecksums.get("md5") + "\tmanifest-sha1.txt" + "\n";
            }

            // write the bagit.txt
            String bagitfile = "BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8";
            Map<String, String> bagitChecksums = this.writeToZip(bagitfile, base + "/bagit.txt", out);
            tagmanifest = tagmanifest + bagitChecksums.get("md5") + "\tbagit.txt" + "\n";

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

    /**
     * Clean out any cached data in the working directory
     *
     * @throws IOException
     */
    public void cleanupWorkingDir()
            throws IOException
    {
        if (this.workingDir.exists())
        {
            log.debug("Cleaning up working directory " + this.workingDir.getAbsolutePath());
            FileUtils.deleteDirectory(this.workingDir);
        }
    }

    /**
     * Delete the zip file
     */
    public void cleanupZip()
    {
        if (this.zipFile.exists())
        {
            log.debug("Cleaning up zip file " + this.zipFile.getAbsolutePath());
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
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private Map<String, String> writeToZip(File file, String path, ZipOutputStream out)
            throws IOException, NoSuchAlgorithmException
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
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private Map<String, String> writeToZip(String str, String path, ZipOutputStream out)
            throws IOException, NoSuchAlgorithmException
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
    private Map<String, String> writeToZip(InputStream fi, String path, ZipOutputStream out)
            throws IOException, NoSuchAlgorithmException
    {
        MessageDigest mdmd5 = MessageDigest.getInstance("MD5");
        MessageDigest mdsha1 = MessageDigest.getInstance("SHA-1");
        BufferedInputStream origin = new BufferedInputStream(fi, BUFFER);
        DigestInputStream inner = new DigestInputStream(origin, mdmd5);
        DigestInputStream dis = new DigestInputStream(inner, mdsha1);

        ZipEntry entry = new ZipEntry(path);
        out.putNextEntry(entry);
        int count;
        byte data[] = new byte[BUFFER];
        while((count = dis.read(data, 0, BUFFER)) != -1) {
            out.write(data, 0, count);
        }
        origin.close();

        String md5hex = Files.digestToString(mdmd5);
        String sha1hex = Files.digestToString(mdsha1);

        Map<String, String> ret = new HashMap<String, String>();
        ret.put("md5", md5hex);
        ret.put("sha-1", sha1hex);
        return ret;
    }
}