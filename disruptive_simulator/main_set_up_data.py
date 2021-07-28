# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Fri Apr 23 20:05:44 2021
@Version: 4.0
@Description: Disruptive vetorial cellullar automata simulation module.
              Main function that initialize and organize the rest of the
              modules. Given the variables of interes of the user, it set
              up each scenario data so that the simulation can be runned.
              Several loops run al the possible simulations that the user
              defined.
"""

###############################################################################

# global imports
from pandas import read_excel
from geopandas import GeoDataFrame, read_file
from os.path import join
from os import chdir
from os import makedirs
from copy import deepcopy
from time import strftime, localtime, time
from gc import collect

# local imports
from normalization import norm
from simulation import simulation
from accuracy import accuracy_assessment
from writeLog import writeLog

#-----------------------------------------------------------------------------#

def set_up_data(wd, scenario_list, z_default, z_urban, z_nonurban, z_protected,
                vec_list, dist_list, frc_list, alfa_list, cicle, YYYY,
                n_threads):
    '''
    Main function that will be in cahrge of setting up all the data necessary
    to run the simulation. As input it requieres all the data that the user
    will define in the main variable setting script.
    '''
    
    print("Seeting up all the necessary files to execute the simulations...")
    # open the DF with the neighbourhoods, if it dont exist try to find it 
    # inside scenario directory
    try:
        neighbours = read_excel("neighbours.xlsx", engine = "openpyxl")
    except:
        wd_scenario = join(wd, scenario_list[0])
        chdir(wd_scenario)
        neighbours = read_excel("neighbours.xlsx", engine = "openpyxl")
    
    # force the index to accord the ID field
    neighbours["Index"] = neighbours["ID"]
    neighbours = neighbours.set_index("Index")
    
    neighbours_dist = list(neighbours.columns)[1:]
    
    # neighbouthood excel is saved in a format that allow a list be saved
    # as string. For each column(distance of buffer) this string is reverted
    # to its for as a list, to de able to be iterated later
    for d in neighbours_dist:
    
        neighbours[d] = neighbours[d].apply(lambda x: x.split(",")\
                                            if isinstance(x, str) == True\
                                            else [])
      
        neighbours[d] = neighbours[d].apply(lambda x: list(map(int, x))\
                                            if x else [])
    
    # make sure it is ordered using ID field
    neighbours = neighbours.sort_values("ID", ascending = True)
    
    for scenario in scenario_list:
                    
        # set the scenario working directory
        wd_scenario = join(wd, scenario)
        chdir(wd_scenario)
        
        # open the excel with the growth per use demands
        growth = read_excel(scenario + "_growth" + ".xlsx", engine = "openpyxl")
                
        # open the shapefile of the scenario
        shp = scenario + ".shp"
        gdf = read_file(shp)
        
        #---------------------------------------------------------------------#
        
        # create a new geodataframe to store only the necessary fields of the
        # scenario GDF
        working_gdf = GeoDataFrame()
        
        field_of_interest_list = ["ID", "AREA", "COOR_X", "COOR_Y", "ZONIF",
                                  "USE_2018", "USE_2050", "S_CO", "S_IN",
                                  "S_SI", "S_MU", "S_MI", "ACCES", "DIST",
                                  "DYN_BOOL", "LP", "PR_CORR", "geometry"]
    
        for field in field_of_interest_list:
            if field in gdf.columns:
                working_gdf[field] = gdf[field]
        
        # add rows related to the develop use and the iteration when it occurs
        working_gdf["DEVELOP"] = None
        working_gdf["ITERATION"] = None
        
        # normalize the distance to network field
        working_gdf = norm (working_gdf, "DIST")

        # setting the zoning values 
        if z_default == True:
            
            # if no zoning (default = True) a proportion of each type will be
            # calculated and will be established as probability
            zonif_1 = len(gdf[(gdf["DYN_BOOL"] == 1) & (gdf["ZONIF"] == 1)])\
                    /len(gdf[gdf["DYN_BOOL"] == 1])
            zonif_2 = len(gdf[(gdf["DYN_BOOL"] == 1) & (gdf["ZONIF"] == 2)])\
                    /len(gdf[gdf["DYN_BOOL"] == 1])
            zonif_3 = len(gdf[(gdf["DYN_BOOL"] == 1) & (gdf["ZONIF"] == 3)])\
                    /len(gdf[gdf["DYN_BOOL"] == 1])
            
            for index, row in working_gdf.iterrows():
                
                if row["ZONIF"] == 1:
                    working_gdf.loc[index, "ZONIF"] = zonif_1
                if row["ZONIF"] == 2:
                    working_gdf.loc[index, "ZONIF"] = zonif_2  
                if row["ZONIF"] == 3:
                    working_gdf.loc[index, "ZONIF"] = zonif_3
        
        else:
            
            # use custom values of zoning based on users definition
            for index, row in working_gdf.iterrows():
                
                if row["ZONIF"] == 1:
                    working_gdf.loc[index, "ZONIF"] = z_urban
                if row["ZONIF"] == 2:
                    working_gdf.loc[index, "ZONIF"] = z_nonurban  
                if row["ZONIF"] == 3:
                    working_gdf.loc[index, "ZONIF"] = z_protected
                
        # list with potential uses
        potential_use_list = ["commerce_utilities", "single_family",
                              "multi_family", "industrial", "mixed"]
        del gdf, shp
        
        # make sure it is sorted using ID field
        working_gdf = working_gdf.sort_values("ID", ascending = True)
        
        #---------------------------------------------------------------------#
        # working directory of the coeficcents of the regression
        wd_coef = join(wd_scenario, "coefficients")
        # wd to save the results
        wd_result = join(wd_scenario, "output")
        # wd with the initial attraction values (calculated before, to speed
        # up preliminatory tests and prelimilatory calibration)
        wd_att = join(wd_scenario, "initial_attraction")
        
        try:
            makedirs(wd_result)
            chdir(wd_result)
        except:
            chdir(wd_result)
            
        print ("Done.\n")
        
        # iterate over different neighbourhood metrics (vF, vNI)
        for coef_vec in vec_list:
            
            # open the coefficents excel values
            df_coef = read_excel(join(wd_coef, (coef_vec + "_coefficients.xlsx")),
                                 engine = "openpyxl")
            # open the initial attraction values
            df_att = read_excel(join(wd_att, ("att_" + coef_vec + ".xlsx")),
                                     engine = "openpyxl")
            
            df_att = df_att.sort_values("ID", ascending = True)

            # iterate over the different buffer distances selected
            for dist in dist_list:

                # make a copy of the working GDF, so it will be reusable on 
                # each iteration
                work_gdf_copy = deepcopy(working_gdf)

                # add the neighbours to the working GDF
                work_gdf_copy["NBRS"] = deepcopy(neighbours["NBRS_" + str(dist)])
                
                # add the attraction values to the WGDF
                for ls in potential_use_list:
                    work_gdf_copy["ATR_" + ls[0:2].upper()] =\
                        df_att["ATR_" + str(dist) + "_" + ls[0:2].upper()]
                
                # iterate over the list of frc user selected
                for frc in frc_list:
                    
                    # iterate over the list of alfa user selected
                    for alfa in alfa_list:
                        
                        # declaring the name that final output files will have
                        sim_name = scenario[0:3] + scenario[-1:] + "_" +\
                                   coef_vec + "_BUFFER_" + str(dist) + "_" +\
                                   str(frc) + "_" + str(alfa) + "_C" + str(cicle)
                        
                        # differentiating every output with the time it was
                        # generated
                        wd_each_sim = wd_result + "\\" + strftime(sim_name +\
                                                                  "_%d-%m-%Y_%H-%M",
                                                                  localtime())
                        # create a folder that will contain all the simulation
                        # output files
                        makedirs(wd_each_sim)
                        chdir(wd_each_sim)

                        # create a log file
                        elLog = strftime(wd_each_sim + "\\LOG_" + sim_name +\
                                         "_##_%d-%m-%Y_%H-%M.log", localtime())
                        
                        with open(elLog, "w") as outFile:
                                   
                            # save initial time of process
                            t_start = time()
                            
                            # execute simulation
                            sim = simulation(work_gdf_copy, growth, df_coef, YYYY, frc,
                                             alfa, cicle, outFile, n_threads)

                            #---------------------------------------------------------#
                            writeLog("\nCalculating the confussion matrix and accuracy metrics of the simulation...",
                                     outFile)
                            # execute the accuracy assesment
                            conf_matrix = accuracy_assessment(sim)
                            writeLog("Done.\n", outFile)
                          
                            # save the GDF as shapefile
                            sim_result_name = sim_name + ".shp"
                                
                            writeLog("Saving the shapefile: " + sim_result_name,
                                     outFile)
                            
                            sim.to_file(sim_result_name)
                            
                            #---------------------------------------------------------#
                            
                            # save the accuracy assesment as a excel file
                            conf_matrix_name = sim_name + ".xlsx"
                                
                            writeLog("Saving the Excel: " + conf_matrix_name,
                                     outFile)
                            conf_matrix.to_excel(conf_matrix_name)
                            
                            #---------------------------------------------------------#
                            
                            # saving all the configuration used for current 
                            # current simulation on the LOG file
                            
                            fmt = "| {{:<{}s}} | {{:>{}s}} | {{:>{}s}} |".format(42, 8, 11)
                            parametros = "\n\n" + "#"*79 + "\n" + "#"*79 + "\n\n" +\
                            "PARAMETERS USED FOR THE SIMULATION:\n\n" +\
                            fmt.format("DESCRIPTION", "VARIABLE", "VALUE") + "\n" +\
                            "-"*71 + "\n" +\
                            fmt.format("Year to simulate", "YYYY", str(YYYY)) + "\n" +\
                            fmt.format("Type of neighbourhood", "coef_vec", coef_vec) + "\n" +\
                            fmt.format("Buffer size", "dist", str(dist)) + "\n" +\
                            fmt.format("Resistance to change factor", "frc", str(frc)) + "\n" +\
                            fmt.format("Degree of randomness", "alfa", str(alfa)) + "\n" +\
                            fmt.format("Priority of urban parcels over rustic ones", "urb_prio", "0.98") + "\n" +\
                            fmt.format("Lapse of time that each intern simualting", "", "") + "\n" +\
                            fmt.format("iteration represents(years)", "ciclo", str(cicle)) + "\n\n" +\
                            "#"*79 + "\n" + "#"*79 +  "\n\n"
                                                        
                            writeLog(parametros, outFile)
                            
                            # save the time after the simulation
                            t_finish = time()
                            
                            # save the time used by the script to run the 
                            # simulation
                            t_process = (t_finish - t_start) / 60
                            writeLog("Process time: " + str(round(t_process, 2)) +\
                                     "minutes", outFile)
                        
                        del sim, conf_matrix, sim_name, conf_matrix_name
                        
                        collect()
        
                del work_gdf_copy
                            
        del working_gdf
        
