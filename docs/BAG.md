# DANS/Dryad BagIt Profile

The Bag adheres to the following general structure

    base directory/
    |    bag-info.txt
    |    bagit.txt
    |    bitstream-description.txt
    |    bitstream-format.txt
    |    bitstream-size.txt
    |    manifest-md5.txt
    |    manifest-sha1.txt
    |    tagmanifest-md5.txt
    \--- metadata
       |   dataset.xml
       |   files.xml
    \--- data/
       |   metadata.xml
       \--- [Data File DOI (escaped)]
           |   metadata.xml
           \--- [DSpace Bundle Name]
               | [DSpace Bitstreams]
               
See examples/10.5061_dryad.q447c for a concrete example

For information on the DANS-specific parts of the Bag format, see https://github.com/DANS-KNAW/easy-sword2-dans-examples

The Dryad-specific parts are detailed below:

## bitstream-description.txt

A tag file containing the descriptive text from the DSpace Bitstream description field.  For example

    dataset-file	data/10.5061_dryad.q447c_1/ORIGINAL/scihub_data.zip
    
## bitstream-format.txt

A tag file containing the mimetype of the DSpace Bitstream.  For example:

    application/zip				data/10.5061_dryad.q447c_1/ORIGINAL/scihub_data.zip
    
## bitstream-size.txt

A tag file containing the size (in bytes) of the DSpace Bitstream.  For exmaple:

    684564797	data/10.5061_dryad.q447c_1/ORIGINAL/scihub_data.zip
    
## data/[Data File DOI (escaped)]

This directory goes through a basic escaping process to remove special characters that are not suitable to be used in
directory names.  This is effectively the same as this regular expression substitution:

    "[^a-zA-Z0-9-_\\.]" to  "_"