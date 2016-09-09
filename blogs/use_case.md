
# Exploring geospatial in-database analytics with IBM dashDB and iPython notebooks using ibmdbpy


Traditional approaches to data analysis require data to be moved out of the database or files into a separate
analytics environment for processing, and then back to the database or files, which is an expensive process.
Doing the analysis in a database, where the data resides, eliminates the costs, time and security
issues associated with the old approach by doing the processing in the data warehouse itself.

We have used the Python package [ibmdbpy](https://github.com/ibmdbanalytics/ibmdbpyhttps://github.com/ibmdbanalytics/ibmdbpy)
 to enable the process of in-database analytics with dashDB
and use it in Pandas-like syntax from Interactive Python notebooks.
The ibmdbpy package translates Pandas-like syntax into SQL and uses a middleware API
(pypyodbc/JayDeBeApi) to send it to an ODBC or JDBC-connected database for execution.
These SQL statements are translated to database queries at runtime as SQL pushdowns. 
The result is retrieved as a memory instance in the form of dataframes, which are
easy to manipulate for further exploratory analysis. It reduces execution time for reading
data and running complex queries on the data compared to fetching the entire dataset into
memory. It, therefore, avoids network overload and crashing of the notebook due to missing local memory.


![png](ibmdbpy.png)

## Getting started

Here we describe a simple example on how to use ibmdbpy with dashDB from notebooks especially with geospatial data.

1. Create a [Bluemix account.](http://www.ibm.com/developerworks/cloud/library/cl-bluemix-fundamentals-start-your-free-trial/)

2. Launch a new Jupyter notebook from the [apache spark service.](https://console.ng.bluemix.net/docs/services/AnalyticsforApacheSpark/index.html)

3. Import the package ibmdbpy ( If not installed, install it using pip command )

```python
!pip install --user ibmdbpy
```

4. The first step is to setup a connection with the data source, which is dashDB in our case.
   It can be done in two ways either with jdbc (For Linux and MAc users) or with odbc (For Windows users)

   In order to setup a JDBC connection , the connection parameters from dashDB can be used along
   with the login credentials.  For a dashDB instance on Bluemix, the JDBC URL string can be found 
   on the dashDB Connection Information page.

```python
import ibmdbpy
from ibmdbpy import IdaDataBase
jdbc = "jdbc:db2://host_name:50001/BLUDB:user=user_name;password=password"
idadb = IdaDataBase(jdbc)
```


That' all you have to do!
 
Using our previously opened IdaDataBase instance named ‘idadb’, we can open one or several IdaDataFrame objects. 
They behave like pointers to remote tables.

Let us open the iris data set, assuming it is stored in the database under the name ‘IRIS’.

```python
from ibmdbpy import IdaDataFrame
idadf = IdaDataFrame(idadb, 'IRIS')
```

We can very easily explore the data in the IdaDataFrame by using built in functions using pandas-like syntax ``IdaDataFrame.head()``

```python
idadf.head()
   sepal_length  sepal_width  petal_length  petal_width species
0           5.1          3.5           1.4          0.2  setosa
1           4.9          3.0           1.4          0.2  setosa
2           4.7          3.2           1.3          0.2  setosa
3           4.6          3.1           1.5          0.2  setosa
4           5.0          3.6           1.4          0.2  setosa
```

With the addition of the geospatial extension, we can also explore geospatial data with an IdaGeoDataFrame object.
These objects can understand a special attribute called geometry which contain geocoded information like a location,
a trajectory or a region in standard geospatial data formats like POINT, LINESTRING or POLYGON.

Let's try to read a sample data from dahsDB called 'GEO_CUSTOMER' which contains location of a customer in the form of 
(X,Y) coordinates.

```python
from ibmdbpy import IdaGeoDataFrame
idageodf = IdaDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER')
idageodf.head()
 	OBJECTID 	SHAPE 	                               NAME 	              INSURANCE_VALUE
0 	1 	      POINT (-80.5561002596 40.1528103049) 	Felice Dicarlo 	    155263
1 	2 	      POINT (-80.6569863704 40.0406902830) 	Aurelia Hussein 	   201204
2 	3 	      POINT (-80.6247752421 40.1320339439) 	Hildegard Kittrell 	260550
3 	4 	      POINT (-80.7158029630 40.1151442910) 	Arletta Henne 	     278992
4 	5 	     POINT (-80.6682444120 40.1808573446) 	 Elvia Shadrick 	    190152
```

## Usecase New York crime analysis

Let us now try out a use case for using this package to analyze crimes committed in the city of New York.
The New York city police department has gathered a huge amount of data over a period
of 10 years and more and categorized the7major crime types (felonies) committed in the city of New York.

The dataset is available in NYC Open Data provided by New York City police department and can be downloaded
[here.](https://data.cityofnewyork.us/Public-Safety/NYPD-7-Major-Felony-Incidents/hyij-8hr7/data)
We can analyze this huge dataset efficiently with `ibmdbpy` to gain meaningful insights from
the data. The major crime hotspots can be then visualized with the help of `folium` and `matplotlib`.

The data was available as a csv file so it had to be geocoded and transformed to a new spatial reference system WGS84
and converted to a *.shp format before loading to dashDB.

Once the NYC crime data is available on dashDB, we can take a look at it from within our notebook:

```python
from IPython.display import IFrame
IFrame("https://dashdb-entry-yp-dal09-07.services.dal.bluemix.net:8443/", width=950, height=450)
```

![png](dashDB.png)


The crime data is already geo-coded (TODO: explain why?) and stored as ST_Point in dashDB. Along with it additional geospatial data for
defining the New York city boroughs are also loaded in dashDB, which will be used for further analysis.


1. To enable our analysis let's first load all required libraries into our notebook:


```python
!pip install --user ibmdbpy
# Import packages needed for analysis
import ibmdbpy 
from ibmdbpy import IdaDataFrame, IdaDataBase, IdaGeoDataFrame
import matplotlib as mpl
import folium,ggplot,mplleaflet
import matplotlib.pyplot as plt
%matplotlib inline
print('All libraries imported!')
```

    All libraries imported!


2. Let us now connect to our dashDB datasource with a JDBC connection.


```python
import getpass,jaydebeapi,jpype
uid = raw_input('Enter Username:')
pwd = getpass.getpass('Enter password:')
jdbc = 'jdbc:db2://dashdb-entry-yp-dal09-07.services.dal.bluemix.net:50000/BLUDB:user=' + uid + ';password=' + pwd
idadb = IdaDataBase(dsn = jdbc)
print('Connection to dashDB successful!')
```

    Enter Username:dash5548
    Enter password:········
    Connection to dashDB successful!


3. The crime data is retrieved as an `IdaGeoDataFrame` which is similar to a `pandas` data frame. 
The process of data retrieval and spatial analysis is much faster with `ibmdbpy` when compared to some well known
spatial analysis libraries like `shapely` and `geopandas`, which usually need an additonal installation of GDAL and
reads the data directly into memory which makes the process of data load very slow.




FIXME: dont think we need the time here, because we dont compare to geopandas. 
Alternatively you can do one analysis in geopandas first, then we can compare the times.


```python
import numpy as np
%time nyc_crime_geo  = IdaGeoDataFrame(idadb,'NYC_CRIME_DATA',indexer = 'OBJECTID')
%time robberies_2015 = nyc_crime_geo[nyc_crime_geo['Offense']=='ROBBERY']
%time robberies_2015 = robberies_2015[robberies_2015['Occrr_Y'] == 2015]
%time robberies2015_brooklyn  = len(robberies_2015[robberies_2015['Borough']=='BROOKLYN'])
%time robberies2015_bronx     = len(robberies_2015[robberies_2015['Borough']=='BRONX'])
%time robberies2015_manhattan = len(robberies_2015[robberies_2015['Borough']=='MANHATTAN'])
%time robberies2015_queens    = len(robberies_2015[robberies_2015['Borough']=='QUEENS'])
%time robberies2015_staten    = len(robberies_2015[robberies_2015['Borough']=='STATEN ISLAND'])
%time robberies_count = [robberies2015_bronx,robberies2015_brooklyn,robberies2015_manhattan,
                         robberies2015_queens,robberies2015_staten]
x = np.array([0,1,2,3,4])
y = np.array(robberies_count)
my_yticks = ['Bronx','Brooklyn','Manhattan','Queens','Staten Island']
plt.yticks(x, my_yticks)
%time plt.barh(x, y)
plt.title('Frequency Distribution of Robberies by Borough in 2015')
plt.xlabel('No. of Robberies')
plt.ylabel('Boroughs')
```

    Wall time: 302 ms
    Wall time: 1.89 s
    Wall time: 624 ms
    Wall time: 1.45 s
    Wall time: 1.45 s
    Wall time: 1.45 s
    Wall time: 1.42 s
    Wall time: 1.42 s
    Wall time: 0 ns
    Wall time: 4 ms





    <matplotlib.text.Text at 0xf577f390>




![png](output_18_2.png)


4. We can further analyze the spatial distribution of crimes over a period of past
decade as a scatterplot .

```python
idadf = IdaGeoDataFrame(idadb,'NYC_CRIME_DATA',indexer = 'OBJECTID',geometry = 'GEO_DATA')
idadf = idadf[idadf['Occrr_Y'] == 2015]
idadf= idadf[idadf['Offense'] == "BURGLARY"]
idadf['X'] = idadf.x() # Using the spatial function ST_X and ST_Y to extract the coordinates
idadf['Y'] = idadf.y()
df = idadf[['Identfr','Occrrnc_Dt','Offense','Precnct','Borough','X','Y']].as_dataframe()
df.plot(kind='scatter', x='X', y='Y', title = 'Spatial Distribution of Burglaries in 2015', figsize=[10,7])
```




    <matplotlib.axes._subplots.AxesSubplot at 0x397f6dd8>




![png](output_20_1.png)


5. Since the crime data is geo-coded(?), we can use the geospatial functions from the python library geopandas
to analyse the geometry and then retrieve the results in the form of a choropleth map based upon the
variation of crime density of each borough. In order to achieve this, we first use the ST_Area function
of dashDB spatial to obtain the area of each borough in square kilometers. Following this, we find the number
of crimes of type __"ROBBERY"__ in each borough in the year __2015__ using the ST_Within function and
finally compute the density for each borough and try to visualise the results with __Leaflet__ library.
FIXME: I would start with the number of crimes, and then area - it is more intuitive

FIXME: please use loops instead of one variables per borough

```python
# Read the data from dahsDB using ibmdbpy
import ibmdbpy
from ibmdbpy import IdaDataBase,IdaDataFrame,IdaGeoDataFrame,IdaGeoSeries
#idadb = IdaDataBase('jdbc:db2://dashdb-entry-yp-dal09-07.services.dal.bluemix.net:50000/' +
                     'BLUDB:user=dash5548;password=Yc3HLDkUY2Ky')
boros = IdaGeoDataFrame(idadb,'NYC_BOROUGHS',indexer = 'OBJECTID')
felonies = IdaGeoDataFrame(idadb,'NYC_CRIME_DATA',indexer = 'OBJECTID')

#Set the geometry attribute and calculate area of the boroughs
boros.set_geometry('GEO_DATA')
felonies.set_geometry('GEO_DATA')
boros['area_in_sq_km'] = boros.area(unit = 'KILOMETER')
boros_df = boros[['BoroName','BoroCode','area_in_sq_km']].as_dataframe()


# Find the count of robberies in each borough fo 2015
felonies = felonies[felonies['Offense']=='ROBBERY']
robberies_2015 = felonies[felonies['Occrr_Y']==2015]
bronx = boros[boros['BoroName']=='Bronx']
brooklyn = boros[boros['BoroName']=='Brooklyn']
manhattan = boros[boros['BoroName']=='Manhattan']
queens = boros[boros['BoroName']=='Queens']
staten = boros[boros['BoroName']=='Staten Island']

result_bronx = robberies_2015.within(bronx)
result_brooklyn = robberies_2015.within(brooklyn)
result_manhattan = robberies_2015.within(manhattan)
result_queens = robberies_2015.within(queens)
result_staten = robberies_2015.within(staten)

bronx_count = result_bronx[result_bronx['RESULT']==1].shape[0]
manhattan_count = result_manhattan[result_manhattan['RESULT']==1].shape[0]
brooklyn_count = result_brooklyn[result_brooklyn['RESULT']==1].shape[0]
queens_count = result_queens[result_queens['RESULT']==1].shape[0]
staten_count = result_staten[result_staten['RESULT']==1].shape[0]
boros_df = boros[['OBJECTID','BoroCode','BoroName','area_in_sq_km']].as_dataframe()
boros_df['robberies_2015'] = [staten_count,queens_count,brooklyn_count,manhattan_count,bronx_count]

#Calculate the crime density
boros_df['crime_density'] = (boros_df['robberies_2015']/boros_df['area_in_sq_km'])*0.01
#Generate choropleth map with folium
import folium, json
with open('NYCboros.json', 'r') as f:
     boros_geo = json.load(f)
robberies = boros_df[['BoroCode','crime_density']]
map1 = folium.Map(location= (40.709475, -74.00275),
                  zoom_start=10, tiles = 'cartodbpositron')
map1.choropleth(geo_str = boros_geo,
                data = robberies,
                columns = ['BoroCode', 'crime_density'],
                key_on = 'feature.properties.BoroCode',
                fill_color='YlGn',
                fill_opacity = 0.7,
                line_weight = 2,
                legend_name = 'Robbery densities by Borough')
map1
```

![png](plot2.png)

As you can see `ibmdbpy` greatly facilitates your data analysis in python as it can analyze a huge and complex datasets
in an efficient manner using in-database analytics.


Hope you find this approach useful!

TODO: links 
