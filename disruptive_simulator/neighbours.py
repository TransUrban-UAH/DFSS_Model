# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Thu Dec 10 17:15:09 2020
@Version: 1.1
@Description: Disruptive vetorial cellullar automata simulation module.
              Encharged of the generation of neighbours for each parcel based
              on different selected buffer distances.
"""

###############################################################################

# global imports
from os import chdir
from geopandas import read_file, GeoDataFrame, sjoin
from time import strftime, localtime, time

# local imports
from writeLog import writeLog

#-----------------------------------------------------------------------------#

def neighbours (gdf, dist_list):
    '''
    Generate a DF where for each ID of an inputted GDF generate the IDs of the
    neighbours that it has based on different buffer size that are in the list
    os distances that are given to the function
    ''' 
    
    # sort the GDF using the ID filed
    gdf = gdf.sort_values("ID", ascending = True)
    
    # generate a new GDF, copy the ID of the input one, and set it as index
    gdf_ID = GeoDataFrame()
    gdf_ID["ID"] = gdf["ID"]
    gdf_ID = gdf_ID.set_index("ID")
    
    # generate a new GDF copying spatial relevant fields
    gdf_geom = gdf[{"ID", "COOR_X", "COOR_Y","USE_2018", "geometry"}]
    
    # delete from memory the input GDF
    gdf = None
    
    # iterate over the different buffer distances selected
    for dist in dist_list:
        
        writeLog("Working with the buffer of " + str(dist) + " meters.", outFile)
        
        # create a new field for the distance and fill it with a list for
        # each parcel
        gdf_ID['NBRS_' + str(dist)] = \
            [list() for x in range(len(gdf_ID.index))]
        
        # create a copy GDF of the geometry one
        gdf_geom_buffer = gdf_geom.copy(deep = True)
        
        # perform a buffer to each parcel of the copied GDF
        gdf_geom_buffer["geometry"] = gdf_geom.geometry.buffer(dist)
        
        writeLog("Executing the Spatial Join...", outFile)
        
        # perform a spatial join, so that each ID of the first GDF of 
        # geometries will have multiple ID of the copied one
        spatial_join = sjoin(gdf_geom_buffer, gdf_geom, how="inner", op="intersects", 
                                 lsuffix="0", rsuffix="1")
        
        writeLog("Done!\n", outFile)
        writeLog("Adding the neighbours to each parcel...", outFile)
        
        # iterate over all the combinations of the spatial join results
        for index, row in spatial_join.iterrows():
            
            # if the ID is not itself
            if row["ID_0"] != row["ID_1"]:

                # append to each ID the neighboured one
                gdf_ID.loc[row["ID_0"], "NBRS_" +\
                           str(dist)].append(row["ID_1"])
                    
                
        writeLog("Done!\n", outFile)
        writeLog("Converting fields to normalized strings.", outFile)
                
        # transform the field to a format that can be saved as excel (since a
        # list object cant be saved, it is needed to transform it to a string)
        gdf_ID["NBRS_" + str(dist)] = \
            gdf_ID["NBRS_" + str(dist)].apply(lambda x: ','.join(map(str, x)))
            
    return gdf_ID

#-----------------------------------------------------------------------------#

def main():
    '''
    Main function that will set up the data and organize it so that the 
    processes are automatized
    '''
    
    try:
        # opening the main file as GeoDataFrame
        shp = "scenario_1.shp"
        gdf = read_file(shp)
        
        # make sure the datatypes are it to reduce memory usage
        gdf = gdf.astype({"COOR_X": int, "COOR_Y": int})
    
        writeLog("Opening the scenario shapefile: " + shp + "\n", outFile)
    
        #######################################################################
        
        # list with the different buffer distances to be used
        
        #dist_list = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]
        dist_list = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]
        
        # run the function
        gdf_final = neighbours(gdf, dist_list)
        
        writeLog("Saving Excel file", outFile)
        
        # save the final file as Excel file
        gdf_final.to_excel("neighbours.xlsx")
        
        #######################################################################
        
    except ValueError:
        writeLog("\n---> ERROR: " + ValueError, outFile)

#-----------------------------------------------------------------------------#

# save initial time
t_start = time()  

# establish the working directory
wd = "C:\\OneDrive - Universidad de Alcala\\UAH\\niko\\sample_Los_santos"
chdir(wd)

# create the LOG file
elLog = strftime(wd + "\LOG_neighbourhood_generation_%d%m%Y_%H%M.log", 
                 localtime())
outFile = open(elLog, "w")

writeLog("Executing the script encharged of the generation of neighbouts of" 
           " each parcel." + "\n", outFile)
           
writeLog("Setting working directory in: " + wd + "\n", outFile)

# executing main function
if __name__ == "__main__":
    main()

# save the final time
t_finish = time()

# show the execution time elapsed
t_process = (t_finish - t_start) / 60
writeLog("Process time: " + str(round(t_process, 2)) + "minutes", outFile)

# close LOG file
outFile.close()






            
    


