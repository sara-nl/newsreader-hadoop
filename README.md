newsreader-hadoop
=================

#### Introduction
This project provides a conversion of the newsreader NLP pipeline so that it can run on Apache Hadoop clusters. The pipeline consists of a series of shell scripts where the execution on Hadoop is orchestrated by 
a [Cascading](http://www.cascading.org) flow. The pipeline scripts and their dependencies are distributed over a Hadoop cluster via distributed cache. The implementation strives to make use of data locality as much as possible by storing the input and output documents as sequence files on HDFS.

For more information on the newsreader project, please visit the project site: [http://www.newsreader-project.eu](http://www.newsreader-project.eu)

#### Building

Ant build scripts and Ivy dependency resolution scripts are included with the project. How you use these is dependent on your programming environment the 
following works for eclipse:

1. Clone the project
2. Open eclipse and create a new Java project; Do not use the default location but select the directory you just cloned into as location for the project
3. The source files should now appear in your project. Make sure to install the IvyDE package and Ivy Ant tasks available here: [http://ant.apache.org/ivy/ivyde/download.cgi](http://ant.apache.org/ivy/ivyde/download.cgi). 
4. With the Ivy packages installed the ivy.xml can be used for resolving the dependencies: right click the file and 'Add Ivy Library...'.
5. Make a subdirectory within the project: newsreader-hadoop-components and download the accompanying components from [beehub](https://beehub.nl/Newsreader/newsreader-hadoop-components) to this directory. 
6. Use the Ant build tasks (Ant view in eclipse) to build the jar and zip the components.

#### Running the pipeline

Once the component zip file and the newsreader-hadoop.jar have been created running the pipeline should be straightforward:

##### Upload documents and components to HDFS. 

For the components simply place the zip in a location on HDFS using the Hadoop command line tools. For the documents you can use the loader tool supplied by the newsreader-hadoop.jar: 

		yarn -jar newsreader-hadoop.jar loader load [local directory with NAF files] [destination path on HDFS] [number of documents per sequence file]
    
Keep in mind that the amount of mappers is determined by either the size of the sequence file(s) on HDFS or the amount of separate files (if the separate files are smaller than the HDFS block size). In other words the documents per file setting can be used to control parallelism of the pipeline run. The documents
are stored in the sequence files as key,value = Text,Text where the key is the original file name (you must use unique file names) and the value is the NAF xml text.

##### Run the pipeline on the documents on HDFS.

You can use the pipeline tool supplied by the newsreader-hadoop.jar: 

		yarn -jar newsreader-hadoop.jar pipeline [input documents on HDFS] [output path on HDFS] [path for failed documents on HDFS] [path to components zipfile on HDFS]

Optionally you can monitor the pipeline using [Driven](http://www.cascading.org/2014/02/14/driven-for-cascading/). In order to do so add the driven jar to the Hadoop classpath:

		export HADOOP_CLASSPATH=[path to driven jar file]

##### Retrieve the output documents from HDFS. 

You can use the loader tool supplied by the newsreader-hadoop.jar: 

		yarn -jar newsreader-hadoop.jar loader unload [documents on HDFS] [path to local file system]
    
#### Extending the pipeline

Extending the pipeline will require some minor adaptations to the Java code in the newsread-hadoop project. The components where slightly altered for running on Hadoop. The convention is that 
components should implement a run.sh scrip that reads NAF input from standard in and output the annotated NAF to standard out. In addition components receive two extra arguments: an absolute path to the
component location on the Hadoop slave nodes and an absolute path to a location that can be used as temporary scratch on the Hadoop slave nodes (unique for each attempt). Components can implement and use extra arguments after these two 
(see for example the implementation of the FBK-time component).

If a new component only requires the default two arguments mentioned above most of the Java code is already in place. Only two steps need to be taken:

1. Add an element to the ModuleFactory enumeration for the new component. The arguments are: component name, implementing class (GenericNewsreaderModule in this case), module timeout and number of lines in standard error if successful.
2. Add the newly added module to the Cascading pipe assembly defined in the NewsReaderFlow class.

If a new component requires extra arguments to the run.sh script. One needs to follow the previous two steps but instead of using a GenericNewsreaderModule as implementing class one should create one for this module specifically. See the FBKTime class as an example.

Finally some notes on error handling. As you may have noticed a timeout and linecount for the standard error stream should be provided for each module. The timeout is used to stop modules that take longer to execute on a single documents. That is documents that take longer than this threshold will fail on that specific module. Documents
that produce more lines in the standard error than the threshold will also fail. These documents will be stored on HDFS in the path supplied as path for failed documents on HDFS.