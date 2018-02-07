package org.datadryad.dansbagit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

        public String filename = null;
        public String workingPath = null;
        public String payloadPath = null;
        public String zipPath = null;

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
                this.file = new File(this.workingPath);
                this.file.setWritable(true, false);
            }
            return this.file;
        }

        public BaggedBitstream getBaggedBitstream()
                throws IOException
        {
            InputStream is = null;
            if (this.zipEntry != null)
            {
                is = zipFile.getInputStream(this.zipEntry);
            }
            else
            {
                is = new FileInputStream(this.getFile());
            }

            BaggedBitstream bb = new BaggedBitstream(is, this.filename, this.format, this.description, this.dataFileIdent, this.bundle);
            return bb;
        }
    }

    private ZipFile zipFile = null;
    private File bagFile = null;
    private File workingDir = null;
    private String name = null;
    private List<BagFileReference> fileRefs = new ArrayList<BagFileReference>();
    private DDM ddm = null;
    private DIM dim = null;
    private Map<String, DIM> subDim = new HashMap<String, DIM>();

    private Map<String, String> dataFilePaths = new HashMap<String, String>();

    public DANSBag(String zipPath, String workingDir)
        throws IOException
    {
        this(null, zipPath, workingDir);
    }

    public DANSBag(File bagFile, File workingDir)
        throws IOException
    {
        this(null, bagFile, workingDir);
    }

    /**
     * Create a BagIt with the given name, using the provided zip as input or output, and with
     * the given working directory for temporary storage
     *
     * @param name      name to use for the bag.  This will form the top level directory inside the zip
     * @param zipPath   path to read in or output zip content
     * @param workingDir    directory used for temporary storage
     */
    public DANSBag(String name, String zipPath, String workingDir)
        throws IOException
    {
        this(name, new File(zipPath), new File(workingDir));
    }

    /**
     * Create a BagIt with the given name, using the provided zip as input or output, and with
     * the given working directory for temporary storage
     *
     * @param name      name to use for the bag.  This will form the top level directory inside the zip
     * @param bagFile  path to read in or output zip content
     * @param workingDir    directory used for temporary storage
     */
    public DANSBag(String name, File bagFile, File workingDir)
        throws IOException
    {
        this.bagFile = bagFile;
        this.workingDir = workingDir;
        this.name = name;
        log.debug("Creating DANSBag object around zipfile " + bagFile.getAbsolutePath() + " using working directory " + workingDir.getAbsolutePath() + " with name " + name);

        if (this.bagFile.exists())
        {
            log.debug("Zipfile " + bagFile.getAbsolutePath() + " exists, loading data from there");
            // load the bag
            this.loadBag();
        }
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
        if (!this.bagFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can calculate the md5");
        }

        try
        {
            FileInputStream fis = new FileInputStream(this.bagFile);
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
        if (!this.bagFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can ask questions about the zip");
        }
        return this.bagFile.getName();
    }

    /**
     * Get the full path to the zip file.  You can only do this once the zip file exists, otherwise you will get a RuntimeException
     *
     * @return the full path to the zip file
     */
    public String getZipPath()
    {
        if (!this.bagFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can ask questions about the zip");
        }
        return this.bagFile.getAbsolutePath();
    }

    /**
     * Get an input stream for the entire zip file.  You can only do this once the zip file exists, otherwise you will get a RuntimeException
     *
     * @return an input stream from which you can retrieve the entire zip file
     */
    public InputStream getInputStream()
    {
        if (!this.bagFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can read the input stream");
        }

        try
        {
            return new FileInputStream(this.bagFile);
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
        if (!this.bagFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can read the segments");
        }

        try
        {
            return new FileSegmentIterator(this.bagFile, size, md5);
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
        if (!this.bagFile.exists())
        {
            throw new RuntimeException("You must writeFile before you can determine the size");
        }
        return this.bagFile.length();
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

    public DIM getDatasetDIM()
    {
        return this.dim;
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

    public DIM getDatafileDIM(String dataFileIdent)
    {
        return this.subDim.get(dataFileIdent);
    }


    public Set<String> dataFileIdents()
    {
        Set<String> idents = new HashSet<String>();
        for (BagFileReference bfr : this.fileRefs)
        {
            idents.add(bfr.dataFileIdent);
        }
        return idents;
    }

    public Set<String> listBundles(String dataFileIdent)
    {
        Set<String> bundles = new HashSet<String>();
        for (BagFileReference bfr : this.fileRefs)
        {
            if (dataFileIdent.equals(bfr.dataFileIdent))
            {
                bundles.add(bfr.bundle);
            }
        }
        return bundles;
    }

    public Set<BaggedBitstream> listBitstreams(String dataFileIdent, String bundle)
            throws IOException
    {
        Set<BaggedBitstream> bitstreams = new HashSet<BaggedBitstream>();
        for (BagFileReference bfr : this.fileRefs)
        {
            if (dataFileIdent.equals(bfr.dataFileIdent) && bundle.equals(bfr.bundle))
            {
                bitstreams.add(bfr.getBaggedBitstream());
            }
        }
        return bitstreams;
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
        filename = Files.sanitizeFilename(filename);
        log.debug("sanitized filename to " + filename);
        
        if (this.bagFile.exists())
        {
            log.error("Attempt to add bitstream when zip already exists");
            throw new RuntimeException("You can't add bitstreams to an existing Bag");
        }

        log.info("Adding bitstream to DANSBag: filename= " + filename + "; format= " + format + "; data_file=" + dataFileIdent + "; bundle=" + bundle);

        // escape the dataFileIdent
        Map<String, String> dfPaths = this.paths(true, false, dataFileIdent, null, null);
        String dataFilename = Files.sanitizeFilename(dataFileIdent);
        this.dataFilePaths.put(dfPaths.get("payload"), dataFilename);
        log.debug("Sanitised dataFileIdent, placing " + dataFilename + " at " + dfPaths.get("payload"));

        // get the correct paths to use for the bitstream
        Map<String, String> paths = this.paths(true, false, dataFileIdent, bundle, filename);
        String workingPath = paths.get("working");
        String payloadPath = paths.get("payload");
        log.info("Bistream will be temporarily staged at " + workingPath);
        log.info("Bitstream will be written to internal zip path " + payloadPath);

        // ensure that the target directory exists
        String workingDir = paths.get("workingDir");
        (new File(workingDir)).mkdirs();

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
        OutputStream os = new FileOutputStream(workingPath);
        IOUtils.copy(dis, os);

        // add the bitstream information to our internal data structure
        BagFileReference bfr = new BagFileReference();
        bfr.filename = filename;
        bfr.workingPath = workingPath;
        bfr.payloadPath = payloadPath;
        bfr.zipPath = paths.get("zip");
        bfr.md5 = Files.digestToString(mdmd5);
        bfr.sha1 = Files.digestToString(mdsha1);
        bfr.description = description;
        bfr.format = format;
        bfr.size = (new File(workingPath)).length();
        bfr.dataFileIdent = dataFileIdent;
        bfr.bundle = bundle;
        this.fileRefs.add(bfr);
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
            if (this.bagFile.exists()) {
                log.error("Attempt to write bag when zip already exists");
                throw new RuntimeException("Cannot re-write a modified bag file.  You should either create a new bag file from the source files, or read in the old zip file and pass the components in here.");
            }

            log.info("Writing bag to file " + this.bagFile.getAbsolutePath());

            // String base = Files.sanitizeFilename(this.name);

            // prepare our zipped output stream
            FileOutputStream dest = new FileOutputStream(this.bagFile);
            ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));

            // prep all the metadata/tag files that we're going to need
            TagFile descriptions = new TagFile();
            TagFile formats = new TagFile();
            TagFile sizes = new TagFile();
            TagFile md5Manifest = new TagFile();
            TagFile sha1Manifest = new TagFile();
            TagFile tagmanifest = new TagFile();

            DANSFiles dfs = new DANSFiles();

            // write the bitstreams, and gather their metadata for the tag files as we go through
            for (BagFileReference bfr : this.fileRefs)
            {
                // add the filename to the files.xml metadata
                dfs.addFileMetadata(bfr.payloadPath, "dcterms:title", bfr.filename);

                // add the doi to the files.xml metadata
                dfs.addFileMetadata(bfr.payloadPath, "dcterms:identifier", bfr.dataFileIdent);

                // update description tag file contents
                if (bfr.description != null && !"".equals(bfr.description))
                {
                    descriptions.add(bfr.payloadPath, bfr.description);
                    dfs.addFileMetadata(bfr.payloadPath, "dcterms:description", bfr.description);
                }

                // update format tag file contents
                if (bfr.format != null && !"".equals(bfr.format))
                {
                    formats.add(bfr.payloadPath, bfr.format);
                    dfs.addFileMetadata(bfr.payloadPath, "dcterms:format", bfr.format);
                }

                // update size tag file contents
                if (bfr.size != -1)
                {
                    sizes.add(bfr.payloadPath, Long.toString(bfr.size));
                    dfs.addFileMetadata(bfr.payloadPath, "dcterms:extent", Long.toString(bfr.size));
                }

                // update the manifests
                if (bfr.md5 != null && !"".equals(bfr.md5))
                {
                    md5Manifest.add(bfr.payloadPath, bfr.md5);
                }

                if (bfr.sha1 != null && !"".equals(bfr.sha1))
                {
                    sha1Manifest.add(bfr.payloadPath, bfr.sha1);
                }

                // this.writeToZip(bfr.getFile(), base + "/" + bfr.payloadPath, out);
                this.writeToZip(bfr.getFile(), bfr.zipPath, out);
            }

            // write the primary dim file
            if (this.dim != null)
            {
                Map<String, String> paths = this.paths(true, false, null, null, "metadata.xml");
                String payload = paths.get("payload");
                // Map<String, String> dimChecksums = this.writeToZip(this.dim.toXML(), base + "/data/metadata.xml", out);
                Map<String, String> dimChecksums = this.writeToZip(this.dim.toXML(), paths.get("zip"), out);
                md5Manifest.add(payload, dimChecksums.get("md5"));
                sha1Manifest.add(payload, dimChecksums.get("sha-1"));
                dfs.addFileMetadata(payload, "dcterms:title", payload);
                dfs.addFileMetadata(payload, "dcterms:format", "text/xml");
            }

            // write the datafile dim files
            for (String ident : this.subDim.keySet())
            {
                Map<String, String> paths = this.paths(true, false, ident, null, "metadata.xml");
                String payload = paths.get("payload");
                DIM dim = this.subDim.get(ident);
                // Map<String, String> subDimChecksums = this.writeToZip(dim.toXML(), base + "/" + zipPath, out);
                Map<String, String> subDimChecksums = this.writeToZip(dim.toXML(), paths.get("zip"), out);
                md5Manifest.add(payload, subDimChecksums.get("md5"));
                sha1Manifest.add(payload, subDimChecksums.get("sha-1"));
                dfs.addFileMetadata(payload, "dcterms:title", payload);
                dfs.addFileMetadata(payload, "dcterms:format", "text/xml");
                dfs.addFileMetadata(payload, "dcterms:identifier", ident);
            }

            // write the DANS files.xml document
            if (dfs != null)
            {
                Map<String, String> paths = this.paths(false, true, null, null, "files.xml");
                Map<String, String> filesChecksums = this.writeToZip(dfs.toXML(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), filesChecksums.get("md5"));
            }

            // write the DANS dataset.xml document
            if (this.ddm != null)
            {
                Map<String, String> paths = this.paths(false, true, null, null, "dataset.xml");
                Map<String, String> datasetChecksums = this.writeToZip(this.ddm.toXML(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), datasetChecksums.get("md5"));
            }

            // write the custom tag files
            if (descriptions.hasEntries())
            {
                Map<String, String> paths = this.paths(false, false, null, null, "bitstream-description.txt");
                Map<String, String> checksums = this.writeToZip(descriptions.serialise(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), checksums.get("md5"));
            }

            if (formats.hasEntries())
            {
                Map<String, String> paths = this.paths(false, false, null, null, "bitstream-format.txt");
                Map<String, String> checksums = this.writeToZip(formats.serialise(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), checksums.get("md5"));
            }

            if (sizes.hasEntries())
            {
                Map<String, String> paths = this.paths(false, false, null, null, "bitstream-size.txt");
                Map<String, String> checksums = this.writeToZip(sizes.serialise(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), checksums.get("md5"));
            }

            // write the checksum manifests
            if (md5Manifest.hasEntries())
            {
                Map<String, String> paths = this.paths(false, false, null, null, "manifest-md5.txt");
                Map<String, String> manifestChecksums = this.writeToZip(md5Manifest.serialise(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), manifestChecksums.get("md5"));
            }

            if (sha1Manifest.hasEntries())
            {
                Map<String, String> paths = this.paths(false, false, null, null, "manifest-sha1.txt");
                Map<String, String> manifestChecksums = this.writeToZip(sha1Manifest.serialise(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), manifestChecksums.get("md5"));
            }

            // write the data file mappings tag file
            if (this.dataFilePaths.size() > 0)
            {
                TagFile dfmtf = new TagFile((HashMap) this.dataFilePaths);
                Map<String, String> paths = this.paths(false, false, null, null, "ident-datafiles.txt");
                Map<String, String> dfmtfChecksums = this.writeToZip(dfmtf.serialise(), paths.get("zip"), out);
                tagmanifest.add(paths.get("payload"), dfmtfChecksums.get("md5"));
            }

            // write the bagit.txt
            String bagitfile = "BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8";
            Map<String, String> paths = this.paths(false, false, null, null, "bagit.txt");
            Map<String, String> bagitChecksums = this.writeToZip(bagitfile, paths.get("zip"), out);
            tagmanifest.add(paths.get("payload"), bagitChecksums.get("md5"));

            // write the bag-info.txt
            String baginfofile = "";
	    // The Item's lastModified date becomes the bag-info "Created" date, because that is  
	    // when the current version of the data package's metadata was "created". This number
	    // must be updated with every edit to the item, because it controls the DANS version chain.
            List<String> createdDates = dim.getDSpaceFieldValues("dc.date.lastModified");
            if(createdDates != null && createdDates.size() > 0) {
                String createdDate = createdDates.get(0);
                baginfofile = "Created: " + createdDate + "\n";
            }
	    // If a version of this item has been sent to DANS before,
	    // either this particular item, OR an earlier Dryad version,
	    // it will have a DANSidentifier, and we mark this as a new 
	    // version in the DANS version chain.
            List<String> dansIDs = dim.getDSpaceFieldValues("dryad.DANSidentifier");
            if(dansIDs != null && dansIDs.size() > 0) {
                String dansID = dansIDs.get(0);
                baginfofile = baginfofile + "Is-Version-Of: urn:uuid:" + dansID + "\n";
	    }

            paths = this.paths(false, false, null, null, "bag-info.txt");
            Map<String, String> baginfoChecksums = this.writeToZip(baginfofile, paths.get("zip"), out);
            tagmanifest.add(paths.get("payload"), baginfoChecksums.get("md5"));

            // finally write the tag manifest
            if (tagmanifest.hasEntries())
            {
                paths = this.paths(false, false, null, null, "tagmanifest-md5.txt");
                this.writeToZip(tagmanifest.serialise(), paths.get("zip"), out);
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
     * Load state from the given zip file
     */
    public void loadBag()
            throws IOException
    {
        this.zipFile = new ZipFile(this.bagFile);
        Enumeration e = this.zipFile.entries();

        // some paths we'll want for exact comparison
        // String dimPath = this.paths(true, false, null, null, "metadata.xml").get("zip");

        // tag files we'll want to create
        TagFile descriptions = null;
        TagFile formats = null;
        TagFile sizes = null;
        TagFile dataFileIdents = null;

        List<String> bitstreams = new ArrayList<String>();
        List<String> dfDims = new ArrayList<String>();

        while (e.hasMoreElements())
        {
            ZipEntry entry = (ZipEntry) e.nextElement();
            String path = entry.getName();

            if (this.name == null)
            {
                this.name = this.getRootName(path);
            }

            if (this.pathIsDatasetDIM(path))
            {
                InputStream is = this.zipFile.getInputStream(entry);
                DIM dim = DIM.parse(is);
                this.setDatasetDIM(dim);
            }
            else if (this.pathIsDataFileDIM(path))
            {
                dfDims.add(path);
            }
            else if (this.pathIsDryadTagFile(path))
            {
                InputStream is = this.zipFile.getInputStream(entry);
                if (path.endsWith("bitstream-description.txt"))
                {
                    descriptions = TagFile.parse(is);
                }
                else if (path.endsWith("bitstream-format.txt"))
                {
                    formats = TagFile.parse(is);
                }
                else if (path.endsWith("bitstream-size.txt"))
                {
                    sizes = TagFile.parse(is);
                }
                else if (path.endsWith("ident-datafiles.txt"))
                {
                    dataFileIdents = TagFile.parse(is);
                }
            }
            else if (this.pathIsBitstream(path))
            {
                bitstreams.add(path);
            }
        }

        if (dataFileIdents == null)
        {
            throw new RuntimeException("Bag File does not contain a ident-datafiles.txt - cannot parse");
        }

        for (String dimPath : dfDims)
        {
            String dimPathBit = this.getPayloadDataFileIdent(dimPath);
            String dataFilePath = "data/" + dimPathBit + File.separator;
            String dataFileIdent = dataFileIdents.getValue(dataFilePath);

            ZipEntry entry = this.zipFile.getEntry(dimPath);
            InputStream is = this.zipFile.getInputStream(entry);
            DIM dim = DIM.parse(is);
            this.addDatafileDIM(dim, dataFileIdent);
        }

        for (String bsPath : bitstreams)
        {
            String dfPathBit = this.getPayloadDataFileIdent(bsPath);
            String bundle = this.getPayloadBundle(bsPath);
            String filename = this.getFilename(bsPath);

            String dataFilePath = "data/" + dfPathBit + File.separator;
            String dataFileIdent = dataFileIdents.getValue(dataFilePath);
            this.dataFilePaths.put(dataFilePath, dataFileIdent);

            Map<String, String> paths = this.paths(true, false, dataFileIdent, bundle, filename);
            String payloadPath = paths.get("payload");

            BagFileReference bfr = new BagFileReference();
            bfr.zipEntry = this.zipFile.getEntry(bsPath);
            bfr.filename = filename;
            bfr.payloadPath = payloadPath;
            bfr.zipPath = paths.get("zip");
            bfr.dataFileIdent = dataFileIdent;
            bfr.bundle = bundle;
            if (descriptions != null)
            {
                bfr.description = descriptions.getValue(payloadPath);
            }
            if (formats != null)
            {
                bfr.format = formats.getValue(payloadPath);
            }
            if (sizes != null)
            {
                bfr.size = Integer.parseInt(sizes.getValue(payloadPath));
            }
            this.fileRefs.add(bfr);
        }
    }


    /**
     * Write the file referenced by the file handle to the given path inside the given zip output stream
     *
     * @param file  The file reference
     * @param path  The path within the zip file to store a copy of the file
     * @param out   The ZipOutputStream to write the file to
     * @return  a map of digest formats and their values for the content (md5 and sha-1)
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
     * @return  a map of digest formats and their values for the content (md5 and sha-1)
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
     * @return  a map of digest formats and their values for the content (md5 and sha-1)
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

    private Map<String, String> paths(boolean payloadFile, boolean dansMetadata, String dataFileIdent, String bundle, String filename)
    {
        Map<String, String> p = new HashMap<String, String>();

        // work out the path to the payload file
        StringBuilder payload = new StringBuilder();

        if (payloadFile)
        {
            payload.append("data").append(File.separator);
        }
        else if (dansMetadata)
        {
            payload.append("metadata").append(File.separator);
        }

        if (dataFileIdent != null)
        {
            String dataFileDir = Files.sanitizeFilename(dataFileIdent);
            payload.append(dataFileDir).append(File.separator);
        }
        if (bundle != null)
        {
            payload.append(bundle).append(File.separator);
        }
        String payloadDir = payload.toString();

        if (filename != null)
        {
            payload.append(filename);
        }
        String payloadPath = payload.toString();
        log.debug("payloadPath " + payloadPath); 

        p.put("payload", payloadPath);

        // extend this to the zip path
        String base = Files.sanitizeFilename(this.name);
        p.put("zip", base + File.separator + payloadPath);

        // also calcuate a working path
        String workingFile = this.workingDir.getAbsolutePath() + File.separator + payloadPath;
        p.put("working", workingFile);

        String workingDir = this.workingDir.getAbsolutePath() + File.separator + payloadDir;
        p.put("workingDir", workingDir);

        return p;
    }

    private boolean pathIsBitstream(String path)
    {
        String pattern = "([^/]+)/data/([^/]+)/([^/]+)/.+";
        return this.matches(pattern, path);
    }

    private String getRootName(String path)
    {
        String pattern = "([^/]+)/.+";
        return this.group(pattern, path, 1);
    }

    private String getPayloadDataFileIdent(String path)
    {
        String pattern = "([^/]+)/data/([^/]+)/.+";
        return this.group(pattern, path, 2);
    }

    private String getPayloadBundle(String path)
    {
        String pattern = "([^/]+)/data/([^/]+)/([^/]+)/.+";
        return this.group(pattern, path, 3);
    }

    private String getFilename(String path)
    {
        String pattern = "([^/]+)/data/([^/]+)/([^/]+)/(.+)";
        return this.group(pattern, path, 4);
    }

    private boolean pathIsDatasetDIM(String path)
    {
        String pattern = "([^/]+)/data/metadata\\.xml";
        return this.matches(pattern, path);
    }

    private boolean pathIsDataFileDIM(String path)
    {
        String pattern = "([^/]+)/data/([^/]+)/metadata\\.xml";
        return this.matches(pattern, path);
    }

    private boolean pathIsDryadTagFile(String path)
    {
        String pattern = "([^/]+)/([^/]+\\.txt)";
        return this.matches(pattern, path);
    }

    private boolean matches(String pattern, String string)
    {
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(string);
        return m.matches();
    }

    private String group(String pattern, String string, int group)
    {
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(string);
        if (m.find())
        {
            return m.group(group);
        }
        return null;
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
            FileUtils.forceDelete(this.workingDir);
        }
    }

    /**
     * Delete the zip file
     */
    public void cleanupZip()
    {
        if (this.bagFile.exists())
        {
            log.debug("Cleaning up zip file " + this.bagFile.getAbsolutePath());
            this.bagFile.delete();
        }
    }
}
