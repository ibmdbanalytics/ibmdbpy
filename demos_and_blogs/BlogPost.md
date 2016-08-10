# Geospatial analyses of crime hotspots with IBM dashDB using iPython notebooks

The importance of crime data analysis has played an important role in public safety.
The New York city police department has gathered a huge amount of data over a period of 10 years and more and
categorized the7major felonies committed in the city of New York. We can analyze this huge dataset with efficient
IBM enabled tools and services to gain meaningful insights from the data. The major crime hotspots can be found with
the help of in-database analytics package for IBM dashDB developed in Python and further spatio-temporal analyses can
be performed on the data using open source Python packages like __‘geopandas’__ which depends on ‘shapely’ and 'pandas'
for converting geospatial data into a dataframe like structure for easier analyses and visualisation.
Here we describe a use case for spatio-temporal analyses of crime data using dashDB, in-database analytics package
called __‘ibmdbpy’__ and visualizing the results with the help of a few open source python packages – __‘folium’__ and
 __‘matplotlib’__.


```python
import ibmdbpy
from ibmdbpy import IdaDataFrame, IdaDataBase, IdaGeoDataFrame
import pandas as pd
import matplotlib as mpl
import folium,ggplot,mplleaflet
import matplotlib.pyplot as plt
import plotly.plotly as py
from pylab import rcParams
rcParams['figure.figsize'] = (25,10)
%matplotlib inline
from ggplot import *
from IPython.display import display
print('All libraries imported!')
```

##Analysis with geopandas - elementwise operations on geometry, slow!

```python
import geopandas as gp
from geopandas import GeoSeries,GeoDataFrame
dsn = 'C:/Users/IBM_ADMIN/Documents/IBM_Internship/crimeDataAnalyses/'
%time nyc_boroughs = GeoDataFrame.from_file(dsn + 'shapefiles4326/nyc_boroughs.shp')
nyc_boroughs.set_index('BoroName', inplace=True)
%time felonies = GeoDataFrame.from_file(dsn + 'nypd7majorfelonies/nypd7majorfelonies1.shp')
felonies.set_index('Identfr')
robberies_2015 = felonies.loc[felonies['Occrr_Y']==2015]
robberies_2015 = robberies_2015.loc[robberies_2015['Offense']== 'ROBBERY',('Precnct','Sector','geometry')]
robberies_2015.shape
```

Find the number of robberies and area of each borough using shapely operations within and area with Geopandas

```python
%time nyc_boroughs['boro_area'] = nyc_boroughs.geometry.area
# Find the count of each type of crimes within each borough
% time robberies_2015['within_SI'] = robberies_2015.geometry.apply(lambda x: x.within(nyc_boroughs.geometry.iloc[0]))
% time robberies_2015['within_QN'] = robberies_2015.geometry.apply(lambda x: x.within(nyc_boroughs.geometry.iloc[1]))
% time robberies_2015['within_BK'] = robberies_2015.geometry.apply(lambda x: x.within(nyc_boroughs.geometry.iloc[2]))
% time robberies_2015['within_MH'] = robberies_2015.geometry.apply(lambda x: x.within(nyc_boroughs.geometry.iloc[3]))
% time robberies_2015['within_BR'] = robberies_2015.geometry.apply(lambda x: x.within(nyc_boroughs.geometry.iloc[4]))
si = len(robberies_2015.loc[robberies_2015['within_SI']== True])
qn = len(robberies_2015.loc[robberies_2015['within_QN']== True])
bk = len(robberies_2015.loc[robberies_2015['within_BK']== True])
mh = len(robberies_2015.loc[robberies_2015['within_MH']== True])
br = len(robberies_2015.loc[robberies_2015['within_BR']== True])
count = [si,qn,bk,mh,br]
nyc_boroughs['no_of_robberies'] = count
nyc_boroughs.loc[:,('no_of_robberies','boro_area')]
```

Now lets use __ibmdbpy__ package to use spatial queries as SQL with the ida_query function. For this, we already have
the necessary data loaded into dashDB as spatial tables,with location information geocoded as ST_Point.
The user can just use the spatial functions provided by the Spatial Extender to perform the operations required.

Let's connect to dashDB first using __ibmdbpy__ with a JDBC connection.


```python
import getpass,jaydebeapi,jpype
uid = raw_input('Enter Username:')
pwd = getpass.getpass('Enter password:')
jdbc = 'jdbc:db2://dashdb-entry-yp-dal09-07.services.dal.bluemix.net:50000/BLUDB:user=' + uid + ';password=' + pwd
idadb = IdaDataBase(dsn = jdbc)
print ('Connection to dashDB successful!')
```

Now we can find the number of robberies in 2015 within each borough using the ida_query operation and fetch the result
as a pandas dataframe into our Python environment.

```python
robberies_2015_by_boro = idadb.ida_query("select b1.\"BoroName\",a1.\"Identfr\", " +
                                         db2gse.st_area(b1.geo_data,'KILOMETER') " +
                                         "as area_in_sq_km from nyc_crime_data a1, nyc_boroughs b1 " +
                                         "where db2gse.st_within(a1.geo_data,b1.geo_data) = 1 " +
                                         "and a1.\"Offense\" == 'ROBBERY' and a1.\"Occrr_Y\" = 2015")
robberies_2015_by_boro.head()
```