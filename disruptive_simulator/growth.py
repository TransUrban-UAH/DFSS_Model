# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Wed Jan 13 14:05:23 2021
@Version: 1.1
@Description: Disruptive vetorial cellullar automata simulation module.
              Extra module that calculates the growth values based on the 
              scenarios changes between current (2018) use and the 
              workshopped one (2050).
"""

###############################################################################

from geopandas import read_file
from pandas import DataFrame
from os import chdir
from statistics import mean

#-----------------------------------------------------------------------------#

# variables that will store the area of the parcels that changed their use
# in the period studied
din_co = 0.0
din_in = 0.0
din_si = 0.0
din_mu = 0.0
din_mi = 0.0
din_va = 0.0

# variable to store the number of vacant parcels that changed their use
num_parcel_va = 0

# variables to store the total area of each use at the starting year (2018)
area_co_2018 = 0.0
area_in_2018 = 0.0
area_si_2018 = 0.0
area_mu_2018 = 0.0
area_mi_2018 = 0.0
area_va_2018 = 0.0

# variables to store the total area of each use at the workshop year (2050)
area_co_2050 = 0.0
area_in_2050 = 0.0
area_si_2050 = 0.0
area_mu_2050 = 0.0
area_mi_2050 = 0.0
area_va_2050 = 0.0

# lists to save every parcel size (area) of each use, in order to find the 
# average size
list_co = []
list_in = []
list_si = []
list_mu = []
list_mi = []
list_va = []

# set working directory of the scenario to evaluate
wd = "C:\\OneDrive - Universidad de Alcala\\UAH\\niko\\sample_Los_santos"

shp = "scenario_1.shp"

chdir(wd)

# read the file as GDF
gdf = read_file(shp)

# iterate over all the rows of the GDF
for index, row in gdf.iterrows():
    
    # store area of the parcel
    area = row["AREA"]
    
    # if the row changed, add its area value to the corresponded variable
    if row["DYNAMIC"] == "commerce_utilities":
        din_co += area
    if row["DYNAMIC"] == "industrial":
        din_in += area
    if row["DYNAMIC"] == "single_family":
        din_si += area
    if row["DYNAMIC"] == "multi_family":
        din_mu += area
    if row["DYNAMIC"] == "mixed":
        din_mi += area
    if row["DYNAMIC"] == "vacant":
        din_va += area
        num_parcel_va += 1
    
    # summarize the areas based con the current and future use of the parcel
    # to find the absolute growth per use
    if row["USE_2018"] == "commerce_utilities":
        area_co_2018 += area
        list_co.append(area)
    if row["USE_2018"] == "industrial":
        area_in_2018 += area
        list_in.append(area)
    if row["USE_2018"] == "single_family":
        area_si_2018 += area
        list_si.append(area)
    if row["USE_2018"] == "multi_family":
        area_mu_2018 += area
        list_mu.append(area)
    if row["USE_2018"] == "mixed":
        area_mi_2018 += area
        list_mi.append(area)
    if row["USE_2018"] == "vacant":
        area_va_2018 += area
        list_va.append(area)
        
    if row["USE_2050"] == "commerce_utilities":
        area_co_2050 += area
        list_co.append(area)
    if row["USE_2050"] == "industrial":
        area_in_2050 += area
        list_in.append(area)
    if row["USE_2050"] == "single_family":
        area_si_2050 += area
        list_si.append(area)
    if row["USE_2050"] == "multi_family":
        area_mu_2050 += area
        list_mu.append(area)
    if row["USE_2050"] == "mixed":
        area_mi_2050 += area
        list_mi.append(area)
    if row["USE_2050"] == "vacant":
        area_va_2050 += area
        list_va.append(area)
    
# compute the absolute growth for each use
demand_co = area_co_2050 - area_co_2018
demand_in = area_in_2050 - area_in_2018
demand_si = area_si_2050 - area_si_2018
demand_mu = area_mu_2050 - area_mu_2018
demand_mi = area_mi_2050 - area_mi_2018
demand_va = area_va_2050 - area_va_2018

# get the average size of parcel for each use
mean_co = mean(list_co)
mean_in = mean(list_in)
mean_si = mean(list_si)
mean_mu = mean(list_mu)
mean_mi = mean(list_mi)
mean_va = mean(list_va)

# generate lists with each value
ls_uses = ["commerce_utilities", "industrial", "single_family", "multi_family", "mixed", "vacant"]
ls_total_areas = [demand_co, demand_in, demand_si, demand_mu, demand_mi, din_va]
ls_annual_areas = [demand_co/32, demand_in/32, demand_si/32, demand_mu/32, demand_mi/32, din_va/32]
ls_avg_size = [mean_co, mean_in, mean_si, mean_mu, mean_mi, mean_va]

# create a DF to store the values
df_growth = DataFrame()

# add values
df_growth["USE"] = ls_uses
df_growth["TOTAL_DEMAND"] = ls_total_areas
df_growth["ANNUAL_DEMAND"] = ls_annual_areas
df_growth["AVG_SIZE"] = ls_avg_size

# save the DF as xlsx format
df_growth.to_excel(shp[:-4] + "_growth" + ".xlsx")



