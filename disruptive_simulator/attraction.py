# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Thu Apr 8 20:05:44 2021
@Version: 2.2
@Description: Disruptive vetorial cellullar automata simulation module.
              Encharged of computing the attraction values of parcels stored
              in a GeoDataFrame. Certain format of the GDF is required.
              It is based on parallel computation using Dask library.
"""

###############################################################################

# global imports
from pandas import DataFrame
from math import sqrt
from dask import dataframe as dd
from dask.distributed import Client

###############################################################################

def lambda_row_att(row, gdf_use_loc, use_list, df_coef):
    '''
    Compute the attraction values of a row. As input it requires a list with 
    the different uses to compute the values, a DataFrame with the location
    of the centroid of each parcel and a DataFrame with the coefficent values
    of the regression used.
    '''
    
    if row["DO_ATT"] == 1:
        
        # the neighbours list of the parcel (row) is stored
        neighbours = row["NBRS"]
            
        # iterates through each possible future use of the parcel
        for use in use_list:
            
            # if the parcel has any neighbour
            if neighbours:
                
                # attraction variable storing is declared
                att = 0
                
                # iterates through all the neighbours
                for nb in neighbours:
                        
                    # evalueate the use of the neighbout parcel
                    neighbour_use = gdf_use_loc.loc[nb, "SIM_USE"]
                    
                    # coefficents between both uses are extracted from the DF
                    coefs = df_coef.loc[(df_coef["origin_use"] == use) &\
                                    (df_coef["evaluated_use"] == neighbour_use)].values.tolist()
                    # it is possible, with small buffers, that a use pairs dont 
                    # have any value
                    if coefs:
                        
                        # list with the coefficents
                        coefs = coefs[0][2:6]
                        
                        # distance is calculated
                        x = row["COOR_X"] - gdf_use_loc.loc[nb, "COOR_X"]
                        y = row["COOR_Y"] - gdf_use_loc.loc[nb, "COOR_Y"]
                        d = sqrt(x**2 + y**2)
                        
                        # attraction value is calculated with the distance and
                        # the coefficents
                        #value = d**3*coefs[0] + d**2*coefs[1] + d*coefs[2] + coefs[3]
                        
                        if d != 0:
                            value = coefs[0] * (d ** coefs[1])
                        else:
                            value = 0
                        
                        # adding up for each use
                        att += value
                        
                # value of the attraction is stored
                row["ATR_" + use[0:2].upper() + "_UNST"] = att          
        
    return row

###############################################################################

def attraction(gdf, df_coef, n_threads):
    '''
    Add to a GeoDataFrame with parcels the attraction values. Each row will have
    this values based on the diferent uses that are stated on the DataFrame with
    the coefficents previously obtained. A location GDF will be generated based
    on the input GDF. All computations are done using parallel computing with
    Dask, so a number of threads to use has to be declared.
    '''
    
    # the use list is the one that coefficients were previously obtaned, not 
    # taking in account the vacant land
    use_list = list(set(df_coef["origin_use"]))
    if "vacant" in use_list:
        use_list.remove("vacant")
    
    # generating a GDF that stores the centroid location of each parcel, as well
    # as the use and the ID
    gdf_use_loc = DataFrame()
    gdf_use_loc["ID"] = gdf["ID"]
    gdf_use_loc["SIM_USE"] = gdf["SIM_USE"]
    gdf_use_loc["COOR_X"] = gdf["COOR_X"]
    gdf_use_loc["COOR_Y"] = gdf["COOR_Y"]
    
    
    # generating a dask dataframe, using a number partition based on the 
    # number of threads that it has to be computed on
    ddgdf = dd.from_pandas(gdf, npartitions = n_threads)

    # It will be applied the lambda_row_pot function to each partition. So the
    # arguments needed for this functions are needed. In this case Dask will
    # not be able to predict the output data structure, so that it is needed to
    # declare it in the meta variable, being that the output format is the same
    # as the input one
    with Client(n_workers = n_threads, threads_per_worker = 1) as client: #defining the cluster of processing
            
        result = ddgdf.map_partitions(lambda ddf: ddf.apply(lambda_row_att, axis = 1, 
                                                           raw = False, result_type = None,
                                                           args = (gdf_use_loc, use_list, df_coef)),
                                     meta = ddgdf).compute(scheduler = "distributed")
        client.shutdown()
    
    return result
