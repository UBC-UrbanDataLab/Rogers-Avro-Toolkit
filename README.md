# Extracting Insights From the Rogers Mobile Dataset 
Intro 
The Urban Data Lab has access to about 80 GB of geospatial data ([in the Avro format](https://en.wikipedia.org/wiki/Apache_Avro)) from Rogers which contains timestamps and geographic coordinates. The timestamp and latitude and longitude can be combined to extract information from the datasets like what areas are more popular than others and at what time of day. 

Python was used as the programming language of choice to work with the data because of its ease of use stemming from pre-existing libraries for [working with Avro files](https://avro.apache.org/docs/current/gettingstartedpython.html) and [manipulating spatial objects](https://shapely.readthedocs.io/en/stable/manual.html). Processing speed was a concern for working with a large amount of data; however, since most of the computations were performed just once, it did not make sense to devote more than a day’s time into optimizing performance. Choosing proper data structures like [R-trees](https://stackoverflow.com/questions/53224490/how-can-i-look-up-a-polygon-that-contains-a-point-fast-given-all-polygons-are-a) and [implementing parallelism](https://docs.python.org/3/library/multiprocessing.html) was enough to get the compute time to be at most several hours, an acceptable time where one can work on other tasks in the background. 

# The Process 
Removing data not needed. 
The process starts on the Azure VM in the “rogersdata” directory. The first step is to purge unwanted rows and columns. This is done by extracting the schemas from each unique file type and removing any columns that are not needed, and then feeding back those schema names to “dcols.py”. This will put all the cleaned files in a named output folder. 

# Searching the data
Once the data has been cleaned, the insights can be searched for, “search_all.py” will do this for you. It will look over all of the data and output a JSON file with all of the days and counts, which can then be converted to a GeoJSON that has the counts of everything. 

# Displaying the data 
The GeoJSON file can then be displayed using the ArcGIS API for JavaScript. 

https://github.com/UBC-UrbanDataLab/Rogers-Avro-Toolkit
 
