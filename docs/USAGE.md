# Usage

This library can be used for creating a bag or reading a bag.  It cannot be used for both actions simultaneously.  You either
begin with no bag and assemble one, or pass in an existing bag and read from it.  Attempts to mix the two behaviours will
result in RuntimeExceptions.


## Creating Bags

### Instantiating

Create a new bag by instantiating the DANSBag class:

    DANSBag bag = new DANSBag(name, zipPath, workingDir);

"name" is the top-level directory that will form the root of the bag.  For example, if you provide "mybag" as the name,
then you will get a resulting zip file which has that as the directory inside the zip:

    mybag /
     | bag-info.txt
     | bagit.txt
     | ... etc
     
"zipPath" is the path to a file which does not yet exist, but is where the zip will be written

"workingDir" is a temporary directory where intermediate files will be stored during the assembly of the bag.
 
### DDM Metadata

DANS requires metadata in their own DDM format to be present in the bag.  You can create an object representing this
metadata thus:

    DDM ddm = new DDM();
    
Then for the standard DDM profile fields (see the DANS documentation for details), you can do:

    ddm.addProfileField("dc:title", "The title");

And for DCMI metadata fields you can do:

    ddm.addDCMIField("dc:identifier", "doi:10.xxxxx", attrs);

Here "attrs" is a Map<String, String> of attributes to attach to the field.

When you have assembled your metadata you can add it to the bag with

    bag.setDDM(ddm);


### DIM Metadata

Dryad includes all the DSpace metadata in its native DIM format to DANS.  You can trivially copy all a DSpace item's
metadata to a DIM document thus:

    DIM dim = new DIM();
    DCValue[] dcvs = item.getMetadata(Item.ANY, Item.ANY, Item.ANY, Item.ANY);
    for (DCValue dcv : dcvs)
    {
        dim.addField(dcv.schema, dcv.element, dcv.qualifier, dcv.value);
    }

Since Dryad Data Packages are made of multiple DSpace Items, you will want to add several DIM documents to the bag.  

For the DIM representing the Data Package itself, you can do:

    bag.setDatasetDIM(dim);

For the DIM representing each of the Data Files, you can do:

    bag.addDatafileDIM(dfDim, dataFileIdentifier);
    
Where "dataFileIdentifier" is your preferred identifier (e.g. the DOI) of the data file.


### Bitstreams

To add a bitstream to the Bag, you simply supply an InputStream and all the relevant metadata:

    bag.addBitstream(inputStream, bitstreamName, format, description, dataFileIdentifier, bundleName);

For example:

    InputStream is = BitstreamStorageManager.retrieve(dspaceContext, bitstream.getID());
    bag.addBitstream(is, "myfile.txt", "text/plain", "a data file", "doi:10.xxxx/1", "ORIGINAL")
    
You should use the same dataFileIdentifier as you use when adding the DIM metadata.


### Writing the Zip file

Once you have added all the metadata and bitstreams you want, you can serialise the bag to a zip file with

    bag.writeToFile()
    
This will write the bag to the "zipFile" provided in the constructor (see above).

Once you have done this you WILL NOT be able to safely modify the bag again, and will generate exceptions.

At this point, though, a number of other functions become possible which would have previously thrown RuntimeExceptions:

* getMD5 - to get the MD5 of the whole zip file
* getZipName
* getZipPath
* getInputStream - get an input stream for reading the Bag (see below)
* getSegmentIterator - get a segment iterator, for reading bags in sections (see below)


### Cleaning up

Once you have created your zip file, you can cleanup the working directory with:

    bag.cleanupWorkingDir();
    
Once you have finished with the zip file, you can clean that up too with:

    bag.cleanupZip();


## Reading Zipped Bags

If you have a DANSBag instance which was created with a zip file, or for which you have called "writeToFile", you can
access the stream of bits that make up that file in a couple of ways.

Most simply, you can get an InputStream for the entire zip:

    InputStream is = bag.getInputStream()

If the zip is large, though, and you want to read it in segments (e.g for continued deposit to DANS) you can get a segment
iterator instead:

    FileSegmentIterator fsi = bag.getSegmentIterator(maxChunkSize, true);
    
Here the "maxChunkSize" is the number of bytes that specify the maximum allowed segment size.  If you wanted to send 100Mb
chunks to DANS, you would use 100000000 as the number here.

The final argument to that method determines whether the md5 checksum is provided for each file segment.

You can then iterate through InputStreams for consecutive portions of the file, thus:

    while (fsi.hasNext())
    {
        FileSegmentInputStream fsis = fsi.next();
        
        long contentLength = fsis.getContentLength();
        
        // if you specified to set the md5 on each file segment
        String md5 = fsis.getMd5();
    }

## Loading Bags

TODO