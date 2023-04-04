# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# This script converts MOSAiC snowpit data into case studies.
#
# Author : Virginie Guemas
# Created in April 2023
# 
# """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
#
import numpy as np
import xarray as xr
import datetime
import xlrd
import os
import sys

# Path ending in /SnowObs where the database is stored
rootpath = os.getcwd()[0:-14]

# Tunable parameters 
maxheight = 36  # Maximum snow height along the record 
maxtime = 50    # Maximum number of time steps in record 
heightcoord = np.linspace(0,maxheight,maxheight*2+1) 

# List of files to be converted for each case studies 
lstfiles = ['temperature/metadata_Temperature.xlsx','density/metadata_DensityCutter_removedOvalues.xlsx','depth/metadata_snow_depth_removedOvalues.xlsx','swe/metadata_SWE.xlsx']
outfiles = {lstfiles[0]:'temperature.nc',lstfiles[1]:'density.nc',lstfiles[2]:'height.nc',lstfiles[3]:'swe.nc'}
# Columns containing coordinates are not always at the same position 
timecol = {lstfiles[0]:2,lstfiles[1]:4,lstfiles[2]:0,lstfiles[3]:0}
loncol = {lstfiles[0]:4,lstfiles[1]:3,lstfiles[2]:0,lstfiles[3]:0}
latcol = {lstfiles[0]:3,lstfiles[1]:2,lstfiles[2]:0,lstfiles[3]:0}
datacol = {lstfiles[0]:6,lstfiles[1]:7,lstfiles[2]:0,lstfiles[3]:0}

# Loop over the files to be converted
for filename in lstfiles[1:3]:
  path = (rootpath + '/MOSAiC/snowpit/'+ filename)
  xls_book = xlrd.open_workbook(path)
  table = xls_book.sheet_by_index(0)
  # xls file opened and converted into a table

  # Empty matrix to store the data from the table
  data = np.empty((maxtime,maxheight*2+1))*np.nan
  t2m = np.empty((maxtime))*np.nan
  lon = np.empty((maxtime))*np.nan
  lat = np.empty((maxtime))*np.nan
  timecoord = []
  ds = xr.Dataset()

  jtime = -1 # Time indice in output file 
  # Scans the row to find those corresponding to our case (ex: snow1)
  for jrow in range(table.nrows):
    if table.col(1)[jrow].value[0:5] == 'snow1':
      tsttime = datetime.datetime(*xlrd.xldate_as_tuple(table.col(timecol[filename])[jrow].value,xls_book.datemode))
      # Convert the date information for this row into datetime date
      if (tsttime not in timecoord):
        timecoord.append(tsttime)
        # Include time into the time coordinate
        jtime = jtime + 1
      print(tsttime, jtime)
      if filename == lstfiles[0]:
        height = table.col(5)[jrow].value # Measurement height
      elif filename == lstfiles[1]:
        toph = table.col(5)[jrow].value  # Range of measurement heights
        bottomh = table.col(6)[jrow].value
        height = 0.5*(toph+bottomh) # To avoid next line to fail
      # In the temperature file, 2m temperature are sometimes stored
      if height == 200:
        t2m[jtime] = table.col(datacol[filename])[jrow].value  
      else:
        if filename == lstfiles[0]:
          # Find the location on the height axis matching the measured height
          jheight = np.where(heightcoord == height)[0]
          data[jtime,jheight] = table.col(datacol[filename])[jrow].value 
          # Measured data stored in output
        elif filename == lstfiles[1]:
          jheighttop = np.where(heightcoord == toph)[0]
          jheightbot = np.where(heightcoord == bottomh)[0]
          # Snow density is measured over a range of heights
          tmp = data[jtime,jheightbot.max():(jheighttop.max()+1)] 
          data[jtime,jheightbot.max():(jheighttop.max()+1)] = np.where(np.isnan(tmp),table.col(datacol[filename])[jrow].value,(tmp+table.col(datacol[filename])[jrow].value)/2)
          # Sometimes 2 samples overlap in their range of heights so we need to average the information from the two measurements
        lon[jtime] = table.col(loncol[filename])[jrow].value
        lat[jtime] = table.col(latcol[filename])[jrow].value


  if filename == lstfiles[0]:
    temparray = xr.DataArray(data[0:len(timecoord),],dims=['time','height'],attrs={'long_name':'snow temperature','units':'Celsius degrees'})
    ds['temp'] = temparray
    t2marray = xr.DataArray(t2m[0:len(timecoord),],dims=('time'),attrs={'long_name':'2m air temperature','units':'Celsius degrees'})
    ds['t2m']=t2marray
  elif filename == lstfiles[1]:
    densarray = xr.DataArray(data[0:len(timecoord),],dims=['time','height'],attrs={'long_name':'snow density','units':'kg.m-3'})
    ds['density'] = densarray
  # Measured data stored in an Xarray and then into the Dataset
  
  heightarray = xr.DataArray(heightcoord,dims=('height'),attrs={'long_name':'snow height','units':'cm'})
  latarray = xr.DataArray(lat[0:len(timecoord),],dims=('time'),attrs={'long_name':'Latitude','units':'degrees'})
  lonarray = xr.DataArray(lon[0:len(timecoord),],dims=('time'),attrs={'long_name':'Longitude','units':'degrees'})
  # Create Xarrays for the coordinates
 
  ds['time']=timecoord
  ds['height']=heightarray
  ds['lat']=latarray
  ds['lon']=lonarray
  # Add the coordinates into the Dataset

  ds.to_netcdf('snow1_' + outfiles[filename])
  # Output Dataset to a netcdf file
