# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Mon Apr 12 14:05:23 2021
@Version: 2.2
@Description: Disruptive vetorial cellullar automata simulation module.
              Extra module that compute and generate a xlsx with the initial
              attraction values for each parcel and buffer distance.
"""

###############################################################################

# global imports
from os import chdir, makedirs, getcwd
from os.path import join
from geopandas import read_file
from pandas import DataFrame, read_excel
from time import strftime, localtime, time
from glob import glob

# local imports
from attraction import attraction
from writeLog import writeLog

#-----------------------------------------------------------------------------#

def attraction_2(neighbours_df, dist_list, df_coef):
    '''
    Interim function to organize the automation of the computing process
    '''
    # set the list with the different uses
    use_list = list(set(df_coef["origin_use"]))
    if "vacant" in use_list:
        use_list.remove("vacant")
    
    # iterate over the different buffer sizes
    for dist in dist_list:
        
        writeLog("Working on buffer distance: " + str(dist), outFile)
        
        df = DataFrame()
        
        # add relevant fields
        df["ID"] = neighbours_df["ID"]
        df["NBRS"] = neighbours_df["NBRS_" + str(dist)]
        df["SIM_USE"] = neighbours_df["USE_2018"]
        df["COOR_X"] = neighbours_df["COOR_X"]
        df["COOR_Y"] = neighbours_df["COOR_Y"]
        df["DO_ATT"] = 1
        
        # add the fields
        for use in use_list:
            df["ATR_" + use[0:2].upper() + "_UNST"] = 0.0
        
        # execute the calculations
        df = attraction(df, df_coef, n_threads)
        
        # add results to the neighbours GDF
        for use in use_list:
            neighbours_df["ATR_" + str(dist) + "_" + use[0:2].upper()] =\
                                        df["ATR_" + use[0:2].upper() + "_UNST"]
    
    return neighbours_df

###############################################################################


def main():
    '''
    Main function that organize and set te data to perform the attraction
    values cumputation
    '''
    
    try:
        # open the scenario shapefile as GeoPandas Dataframe
        shp = "scenario_1.shp"
        gdf = read_file(shp)

        writeLog("Opening the scenario shapefile " + shp + "\n", outFile)
        
        #######################################################################

        # list with the buffer distance to compute
        #dist_list = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]
        dist_list = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]

        writeLog("Opening the coefficent values related to the regression " +\
                 "dataframes\n", outFile)
        
        # get the xlsx files from the directory with the coefficient values
        wd = getcwd()
        wd_coef = wd + "\\coefficients"
        chdir(wd_coef)
        
        xlsx_list = glob("*coefficients.xlsx")
                
        # make a dictionary to store, for each metric, the correspondent DF
        d_metric_neigh = dict.fromkeys(xlsx_list, None)
        
        for key in d_metric_neigh.keys():
            d_metric_neigh[key] = read_excel(join(wd_coef, key))
        
        # back to main dw
        chdir(wd)
        
        #######################################################################

        writeLog("Opening the neighbours dataframe", outFile)
        
        # read the neighbours xlsx
        neighbours_df = read_excel(join(wd, "neighbours.xlsx"))
        
        # force the index to accord the ID field
        neighbours_df["Index"] = neighbours_df["ID"]
        neighbours_df = neighbours_df.set_index("Index")
        
        # get the list of the distances values
        neighbours_dist_list = list(neighbours_df.columns)[1:]
        
        # transform the normalized strings to list, so that the neighbours
        # can be iterable
        for l in neighbours_dist_list:
        
            neighbours_df[l] = neighbours_df[l].apply(lambda x: x.split(",")\
                                                if isinstance(x, str) == True\
                                                else [])
          
            neighbours_df[l] = neighbours_df[l].apply(lambda x: list(map(int, x))\
                                                if x else [])
        
        # add to the neighbours DF the use and coordinates of each parcel
        neighbours_df["USE_2018"] = gdf["USE_2018"]
        neighbours_df["COOR_X"] = gdf["COOR_X"]
        neighbours_df["COOR_Y"] = gdf["COOR_Y"]
                
        #######################################################################
        
        # generate a folder where save the results
        wd_att = wd + "\\initial_attraction"
        try:
            makedirs(wd_att)
            chdir(wd_att)
        except:
            chdir(wd_att)
        
        writeLog("Setting the location to save the results in: "+\
                 wd_att + "\n", outFile)
            
        #######################################################################
        del gdf, neighbours_dist_list, shp, wd_coef, wd_att
        #######################################################################
        
        # iterate over the different metric DFs
        for key in d_metric_neigh.keys():
            
                writeLog("Working on the metric " + key[:-18], outFile)
            
                # get the DF
                metric_df = d_metric_neigh[key]
    
                # compute the attraction via paralel computing funcions
                final_df = attraction_2(neighbours_df, dist_list, df_coef = metric_df)
                
                unnecesary_fields = ["COOR_X", "COOR_Y", "NBRS_25", "NBRS_50",
                                     "NBRS_75", "NBRS_100", "NBRS_125",
                                     "NBRS_150", "NBRS_175", "NBRS_200",
                                     "NBRS_250", "NBRS_300", "NBRS_500"]
                
                # delete the unnecesary fields
                for field in unnecesary_fields:
                    if field in final_df.columns:
                        final_df = final_df.drop(field, axis = 1)

                # save the results
                saving_name = "att_" + key[:-18] + ".xlsx"
                
                writeLog("Saving xlsx: " + saving_name, outFile)
                
                final_df.to_excel(saving_name, index = False)
                
                writeLog("Done.", outFile)
        
        #######################################################################
        
    except ValueError:
        writeLog("\n---> ERROR: " + ValueError, outFile)
        
###############################################################################

# set the working directory
wd = "C:\\OneDrive - Universidad de Alcala\\UAH\\niko\\sample_Los_santos"
chdir(wd)

# number of threads to be used for the computation (to be used in parallel)
n_threads = 12

# save initial time
t_start = time()  

# create the LOG file
elLog = strftime(wd + "\LOG_generation_initial_att_values_%d%m%Y_%H%M.log", 
                 localtime())
outFile = open(elLog, "w")

writeLog("Executing the script in charge of the generation of the initial " +\
         "attraction-repulsion values\n", outFile)
           
writeLog("Setting working directory in: " + wd + "\n", outFile)

###############################################################################

# execute the  main function
if __name__ == "__main__":
    main()
    
###############################################################################
# save final time
t_finish = time()

# show the total time that took the script to generate the results
t_process = (t_finish - t_start) / 60
writeLog("Process time: " + str(round(t_process, 2)) + "minutes", outFile)

outFile.close()