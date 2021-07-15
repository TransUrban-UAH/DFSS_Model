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
from os.path import join
from os import chdir, makedirs
from pandas import read_excel, DataFrame
from geopandas import read_file
from networkx import DiGraph
from tqdm import tqdm
from time import strftime, localtime, time

# local imports
from writeLog import writeLog

#-----------------------------------------------------------------------------#

def create_network(df):
    '''
    Function that transforms a DF into graph
    '''
    
    writeLog("\nGenerating the working graph...\n", outFile)
    
    # open the xlsx that contain the neighbourhood relations
    neighbours_df = read_excel(join(wd, "neighbours.xlsx"))
    neighbours_distances = list(neighbours_df.columns)[1:]
    
    # force the index to accord the ID field
    neighbours_df["Index"] = neighbours_df["ID"]
    neighbours_df = neighbours_df.set_index("Index")
    
    # iterate over the distances(fields) to transform from normalized strings
    # to lists
    for l in neighbours_distances:
    
        neighbours_df[l] = neighbours_df[l].apply(lambda x: x.split(",")\
                                            if isinstance(x, str) == True\
                                            else [])
      
        neighbours_df[l] = neighbours_df[l].apply(lambda x: list(map(int, x))\
                                            if x else [])
    # Node creation
    G = DiGraph()
    
    # iterate over the DF by rows and add a node for each row, using the
    # fields of interest
    for index, row in df.iterrows():
        G.add_node(node_for_adding = row["ID"], ID = row["ID"], AREA = row["AREA"],
                   USE = row["USE_2050"], DYNAMIC = row["DYN_BOOL"])
    writeLog("Done!\n", outFile)
    
    writeLog("Adding neighbours to each node according to the buffer size...",
             outFile)
    
    # add the neighbours of each node using the DF of neighbourhood and the 
    # different buffer sizes
    for node in G.nodes():
        for col in neighbours_distances:
            G.nodes[node][col] = neighbours_df.loc[node, col]
        
    return G

###############################################################################

def calc_vF_all(G, dist, evaluar_usos, d_total_areas):
    '''
    Function that applies the vF function to all parcels based on the
    workshopped (2050) use. As input needs the graph, the list with the pair
    use-use tuple and the dictionary with the total area values
    '''
    
    writeLog("\nExecuting the vF_all computation", outFile)
    
    # create a dictionary from the pair use tuples, and assign the keys 0 value
    d_nbrs_area = dict.fromkeys(evaluar_usos, 0) 
    d_mean_num = dict.fromkeys(evaluar_usos, 0)
    
    # progress bar to keep track of the process state and its evolution
    bar1 = tqdm(total = G.number_of_nodes())

    # iterate over the nodes of the graph
    for n in G.nodes():
        
        # definition of the variable that will store the total area of each 
        # buffer
        total_area_buffer = G.nodes[n]["AREA"]
        
        # copy of the previous dictionary
        d_values = {x: 0 for x in d_nbrs_area}
        
        # list with the neighbours that the node has
        neighbours_list = G.nodes[n]["NBRS_" + str(dist)]
        
        # update progress bar
        bar1.update()

        # continue if there is no neighbours
        if not neighbours_list:
            continue
        else:
            # iterate over the list of neighbours
            for v in neighbours_list:
                
                # adding up the areas to the total of the buffer
                total_area_buffer += G.nodes[v]["AREA"]
                
                # iterate over the dictionary
                for d in d_values.keys():
                    
                    # evaluate if the use of the node evaluated match the first
                    # element(use) of the tuple evaluate
                    if G.nodes[n]["USE"] == d[0]:
                        
                        # evaluate if the neighbour's use match the second 
                        # element of the tuple. This allow to check all 
                        # combinatios of pair of uses in order to store each
                        # possibility in the dictionary
                        if G.nodes[v]["USE"] == d[1]:

                            # each combination of neighbourhood is stored in
                            # form of area in the dictionary, as well as the 
                            # number of that type of neighbourhood
                            d_values[d] += G.nodes[v]["AREA"]
                            d_mean_num[d] += 1
                            
            # iterate over the first dictionary, that is still empty
            for y in d_nbrs_area.keys():
                
                # iterate over the total areas dictionary
                for k in d_total_areas:
                    
                    # if the first element of the tuple that defines each key
                    # match the key of the total areas dictionary, the formula
                    # is applied
                    if y[1] == k:
                        
                        # the formula si applied. It picks up the accumulated
                        # area that is stored in the second dictionary, divides
                        # it by the total buffer area, and divides it by the 
                        # division between the total area per use in the study
                        # area and the total absolute area
                        if d_total_areas[k] > 0:
                            normalized_area_total_buffer = \
                                (d_values[y] / total_area_buffer) \
                                / (d_total_areas[k] / d_total_areas["total"])
                        
                            # store the value in the first dictionary, that will
                            # be updated with the values after the aplication
                            # of the formula
                            d_nbrs_area[y] += normalized_area_total_buffer
               
    # lastly, iterate over the first dictionary, updating the values by dividing
    # each value by the number of parcels that were in the buffer
    for area in d_nbrs_area.keys():
        if d_mean_num[area] > 0:
            d_nbrs_area[area] = d_nbrs_area[area] / d_mean_num[area]
        
    writeLog("\nCalculations finished", outFile)
    
    # close progress bar
    bar1.close(); del bar1
                  
    # return a list with the use pairs values
    return list(d_nbrs_area.values())
    
###############################################################################

def calc_vF_changes(G, dist, evaluar_usos, d_areas_tot_changes):
    '''
    Function that applies the vF formula to just the parcels that suffered 
    any change in the 2018-2050 period. As input requires the graph, the 
    list with the pair use-use tuple, dictionary with the number of parcels
    that changed their use and the dictionary with the total area values
    '''

    writeLog("\nExecuting the vF_changes computation", outFile)
    
    # create a dictionary from the pair use tuples, and assign the keys 0 value
    d_nbrs_area = dict.fromkeys(evaluar_usos, 0)
    d_mean_num = dict.fromkeys(evaluar_usos, 0)
    
    # progress bar to keep track of the process state and its evolution
    bar2 = tqdm(total = G.number_of_nodes())

    # iterate over the nodes of the graph
    for n in G.nodes():
        
        # definition of the variable that will store the total area of each 
        # buffer
        total_area_buffer = G.nodes[n]["AREA"]
        
        # copy of the previous dictionary
        d_values = {x: 0 for x in d_nbrs_area}
        
        # update progress bar
        bar2.update()
    
        # evaluate if the node suffered changes in the studied period
        if G.nodes[n]["DYNAMIC"] == 1:
            
            # list with the neighbours that the node has
            neighbours_list = G.nodes[n]["NBRS_" + str(dist)]
            
            # continue if there is no neighbours
            if not neighbours_list:
                continue
            else:
                                
                # iterate over the list of neighbours
                for v in neighbours_list:
                    
                    # adding up the areas to the total of the buffer
                    total_area_buffer += G.nodes[v]["AREA"]
                    
                    # iterate over the dictionary
                    for d in d_values.keys():
                        
                        # evaluate if the use of the node evaluated match the first
                        # element(use) of the tuple evaluate
                        if G.nodes[n]["USE"] == d[0]:
                            
                            # evaluate if the neighbour's use match the second 
                            # element of the tuple. This allow to check all 
                            # combinatios of pair of uses in order to store each
                            # possibility in the dictionary
                            if G.nodes[v]["USE"] == d[1]:
                                
                                # each combination of neighbourhood is stored in
                                # form of area in the dictionary, as well as the 
                                # number of that type of neighbourhood
                                d_values[d] += G.nodes[v]["AREA"]
                                d_mean_num[d] += 1
      
                # iterate over the first dictionary, that is still empty
                for y in d_nbrs_area.keys():
                    
                    # iterate over the total areas dictionary
                    for k in d_areas_tot_changes:
                        
                        # if the first element of the tuple that defines each key
                        # match the key of the total areas dictionary, the formula
                        # is applied
                        if y[1] == k:
                            
                            # the formula si applied. It picks up the accumulated
                            # area that is stored in the second dictionary, divides
                            # it by the total buffer area, and divides it by the 
                            # division between the total area per use in the study
                            # area and the total absolute area
                            if d_areas_tot_changes[k] > 0:
                                
                                normalized_area_total_buffer = \
                                    (d_values[y] / total_area_buffer) \
                                    / (d_areas_tot_changes[k] / d_areas_tot_changes["total"])
                                
                                # store the value in the first dictionary, that will
                                # be updated with the values after the aplication
                                # of the formula  
                                d_nbrs_area[y] += normalized_area_total_buffer

    # lastly, iterate over the first dictionary, updating the values by dividing
    # each value by the number of parcels that were in the buffer
    for area in d_nbrs_area.keys():
        if d_mean_num[area] > 0:
            d_nbrs_area[area] = d_nbrs_area[area] / d_mean_num[area]
                    
    writeLog("\nCalculations finished", outFile) 
    
    # close progress bar
    bar2.close(); del bar2
    
    # return a list with the use pairs values
    return list(d_nbrs_area.values())

###############################################################################

def calc_vNI_all(G, dist, evaluar_usos):
    '''
    Function that apply the vNI formula to all parcels based on the workshopped
    (2050) use. As input requires the graph and the list with the pair use-use
    tulples
    '''
    
    writeLog("\nExecuting the vNI_all computation", outFile)
    
    # create a dictionary from the pair use tuples, and assign the keys 0 value
    d_nbrs_area = dict.fromkeys(evaluar_usos, 0)
    d_mean_num = dict.fromkeys(evaluar_usos, 0)
    
    # progress bar to keep track of the process state and its evolution
    bar3 = tqdm(total = G.number_of_nodes())

    # iterate over the nodes of the graph
    for n in G.nodes():
        
        # definition of the variable that will store the total area of each 
        # buffer
        total_area_buffer = G.nodes[n]["AREA"]
        
        # copy of the previous dictionary
        d_values = {x: 0 for x in d_nbrs_area}
    
        # list with the neighbours that the node has
        neighbours_list = G.nodes[n]["NBRS_" + str(dist)]
        
        # update progress bar
        bar3.update()
        
        # continue if there is no neighbours
        if not neighbours_list:
            continue
        else:
            # iterate over the list of neighbours
            for v in neighbours_list:
                
                # adding up the areas to the total of the buffer
                total_area_buffer += G.nodes[v]["AREA"]
                
                # iterate over the dictionary
                for d in d_values.keys():
                    
                    # evaluate if the use of the node evaluated match the first
                    # element(use) of the tuple evaluate
                    if G.nodes[n]["USE"] == d[0]:
                        
                        # evaluate if the neighbour's use match the second 
                        # element of the tuple. This allow to check all 
                        # combinatios of pair of uses in order to store each
                        # possibility in the dictionary
                        if G.nodes[v]["USE"] == d[1]:
                            
                            # each combination of neighbourhood is stored in
                            # form of area in the dictionary, as well as the 
                            # number of that type of neighbourhood
                            d_values[d] += G.nodes[v]["AREA"]
                            d_mean_num[d] += 1
             
            # iterate over the first dictionary, that is still empty   
            for y in d_nbrs_area.keys():
                
                # apply the formula, that pick the accumulated area stored in
                # the second dictionary and divides it by the total area
                # of the buffer
                normalized_buffer_area = d_values[y]/total_area_buffer
                
                # store the value in the first dictionary, that will
                # be updated with the values after the aplication
                # of the formula
                d_nbrs_area[y] += normalized_buffer_area
      

    # lastly, iterate over the first dictionary, updating the values by dividing
    # each value by the number of parcels that were in the buffer
    for area in d_nbrs_area.keys():
        if d_mean_num[area] > 0:
            d_nbrs_area[area] = d_nbrs_area[area] / d_mean_num[area]
                    
    writeLog("\nCalculations finished", outFile) 
    
    # close progress bar
    bar3.close(); del bar3
    
    # return a list with the use pairs values
    return list(d_nbrs_area.values())
    
###############################################################################

def calc_vNI_changes(G, dist, evaluar_usos):
    '''
    Function that applies the vNI formula to just the parcels that suffered
    any change in the 2018-2050 period. As input requires the graph, the list
    with the pair use-use tuple and the dictionary with the number of parecels 
    that changed their use
    '''
    
    writeLog("\nExecuting the vNI_changes computation", outFile)
    
    # create a dictionary from the pair use tuples, and assign the keys 0 value
    d_nbrs_area = dict.fromkeys(evaluar_usos, 0)   
    d_mean_num = dict.fromkeys(evaluar_usos, 0)
    
    # progress bar to keep track of the process state and its evolution
    bar4 = tqdm(total = G.number_of_nodes())

    # iterate over the nodes of the graph
    for n in G.nodes():
        
        # definition of the variable that will store the total area of each 
        # buffer
        total_area_buffer = G.nodes[n]["AREA"]
        
        # copy of the previous dictionary
        d_values = {x: 0 for x in d_nbrs_area}
        
        # update progress bar
        bar4.update()
        
        # evaluate if the node suffered changes in the studied period
        if G.nodes[n]["DYNAMIC"] == 1:
            
            # list with the neighbours that the node has
            neighbours_list = G.nodes[n]["NBRS_" + str(dist)]
            
            # continue if there is no neighbours
            if not neighbours_list:
                continue
            else:
                
                # iterate over the list of neighbours
                for v in neighbours_list:
                    
                    # adding up the areas to the total of the buffer
                    total_area_buffer += G.nodes[v]["AREA"]
                    
                    # iterate over the dictionary
                    for d in d_values.keys():
                        
                        # evaluate if the use of the node evaluated match the first
                        # element(use) of the tuple evaluate
                        if G.nodes[n]["USE"] == d[0]:
                            
                            # evaluate if the neighbour's use match the second 
                            # element of the tuple. This allow to check all 
                            # combinatios of pair of uses in order to store each
                            # possibility in the dictionary
                            if G.nodes[v]["USE"] == d[1]:
                                
                                # each combination of neighbourhood is stored in
                                # form of area in the dictionary, as well as the 
                                # number of that type of neighbourhood
                                d_values[d] += G.nodes[v]["AREA"]
                                d_mean_num[d] += 1
                
                # iterate over the first dictionary, that is still empty
                for y in d_nbrs_area.keys():
                    
                    # apply the formula, that pick the accumulated area stored in
                    # the second dictionary and divides it by the total area
                    # of the buffer
                    normalized_buffer_area = d_values[y]/total_area_buffer
                    
                    # store the value in the first dictionary, that will
                    # be updated with the values after the aplication
                    # of the formula
                    d_nbrs_area[y] += normalized_buffer_area
    
    # lastly, iterate over the first dictionary, updating the values by dividing
    # each value by the number of parcels that were in the buffer
    for area in d_nbrs_area.keys():
        if d_mean_num[area] > 0:
            d_nbrs_area[area] = d_nbrs_area[area] / d_mean_num[area]
                  
    writeLog("\nCalculations finished", outFile) 
    
    # close progress bar               
    bar4.close(); del bar4
    
    # return a list with the use pairs values
    return list(d_nbrs_area.values())
    
###############################################################################

def main():
    '''
    Main function that organize all the processes
    '''
    
    try:
        # opening the shapefile of the scenario as Geopandas dataframe
        shp = "scenario_1.shp"
        gdf = read_file(shp)
        
        writeLog("Opnening the main file: " + shp + "\n", outFile)
        
        #######################################################################
        
        # list with the different uses that parcels can have
        uses_list = ["commerce_utilities", "industrial", "single_family",
                     "multi_family", "mixed", "vacant"]
        
        # list with the distances to use
        #dist_list = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]
        dist_list = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]
        
        # list that will contain the tuples with pair uses
        evaluate_uses = []
        
        # iterate over the list of different uses
        for use in uses_list:
            
            # iterate again 
            for use2 in uses_list:
                
                # make each combination
                pair_uses_tuple = (use, use2)
                
                # add each pair tuple to the list
                evaluate_uses.append(pair_uses_tuple)
                
        # create a dictionary that will have as keys the tuples of each pair of
        # uses. Assign 0 value. It will be used to caompute the total area and
        # the area for each use
        d_total_area = dict.fromkeys(uses_list, 0)
        
        # total area just for the parcels that changed
        d_total_area_changes = dict.fromkeys(uses_list, 0)
        
        # add a total key to store the total area of the studied area
        d_total_area["total"] = 0
        d_total_area_changes["total"] = 0
        
        # iterate over the GDF (shapefile of the studied area), each row is a 
        # parcel
        for index, row in gdf.iterrows():
            
            # area of each parcel
            area = row["AREA"]
            
            # iterate over the keys of the total areas dictionary
            for key1 in d_total_area.keys():
                
                # if the use of the parcel match the key, the area is being
                # added
                if row["USE_2050"] == key1:
                    d_total_area[key1] += area
                    d_total_area["total"] += area
                    
            # same but only taking into account the parcels that changed their
            # use (dynamic = 1)
            for key2 in d_total_area_changes.keys():
                if row["DYNAMIC"] == key2:
                    d_total_area_changes[key2] += area
                    d_total_area_changes["total"] += area
        
        writeLog("The overall data of the studied area are: " + " \n", outFile)
        
        # iterate ober the total area buffer to show basic characteristics of 
        # the studied area
        for use in d_total_area.keys():
            
            writeLog(("Area " + str(use) + ": ").ljust(20) \
                       + str(d_total_area[use]) + " m2.", outFile)
            
        
        writeLog("\nGenerating the DataFrames to store the results", outFile)
        
        # generate the DFs that will contain the values of att-repulsion of 
        # each metric type and for each buffer distance
        DF_vF_all = DataFrame(data = evaluate_uses, columns = 
                                 ["origin_use", "evaluated_use"])
        DF_vF_changes = DF_vF_all.copy(deep = True)
        DF_vNI_all = DF_vF_all.copy(deep = True)
        DF_vNI_changes = DF_vF_all.copy(deep = True)
        
        
        # list with the fields that are needed to carry out the process
        fields_of_interest = ["ID", "AREA", "USE_2050", "DYN_BOOL", "geometry"]
        
        # delete all fields that are unnecesary
        for field in list(gdf.columns):
            if field not in fields_of_interest:
                gdf.drop(field, axis = 1, inplace = True)
        
        #######################################################################
        
        # generate the graph
        G = create_network(gdf)
        
        # se itera sobre la lista de distancias que se quieren usar para el 
        # calculo de los buffer
        for d in dist_list:
            
            writeLog("Working with the distance: " + str(d), outFile)

            # se ejecuta cada funcion y se guardan los resultados (la lista)
            values_vF_all = calc_vF_all(G, d, evaluate_uses, d_total_area)
            
            values_vF_changes = calc_vF_changes(G, d, evaluate_uses, d_total_area_changes)
            
            values_vNI_all = calc_vNI_all(G, d, evaluate_uses)
            
            values_vNI_changes = calc_vNI_changes(G, d, evaluate_uses)
            
            writeLog("\nFilling the DF with the values with the buffer distance"+ 
                       " of " + str(d) + " meters", outFile)
                
            # se le aniade a cada dataframe la lista generada por las funciones
            # por columnas segun la distancia del buffer
            DF_vF_all[d] = values_vF_all
            DF_vF_changes[d] = values_vF_changes
            DF_vNI_all[d] = values_vNI_all
            DF_vNI_changes[d] = values_vNI_changes
        
        # directory to save the results
        dir_att = wd + "\\attraction_values"
        try: 
            makedirs(dir_att)
            chdir(dir_att)
        except:
            chdir(dir_att)
        
        # se guardan los dataframes como archivos excel
        writeLog("\nSaving the DFs with .xlsx format", outFile)
        
        DF_vF_all.to_excel("vF_all.xlsx", index = False)  
        DF_vF_changes.to_excel("vF_changes.xlsx", index = False)
        DF_vNI_all.to_excel("vNI_all.xlsx", index = False)
        DF_vNI_changes.to_excel("vNI_changes.xlsx", index = False)
        
    except ValueError:
        writeLog("\n---> ERROR: " + ValueError,outFile)

###############################################################################

# save initial time
t_start = time()  

# setting working directory
wd = "C:\\OneDrive - Universidad de Alcala\\UAH\\niko\\sample_Los_santos"
chdir(wd)

# create a LOG file
elLog = strftime(wd + "\LOG_attraction_repulsion_values_%d%m%Y_%H%M.log", 
                 localtime())
outFile = open(elLog, "w")

writeLog("Setting working directory in: " + wd + "\n", outFile)

writeLog("Executing attraction-repulsion values computation" + "\n", outFile)
           
# execute main function
if __name__ == "__main__":
    main()

# save the time at the end of the execution
t_finish = time()

# show the time that took the script perform all the operations
t_process = (t_finish - t_start) / 60
writeLog("Process time: " + str(round(t_process, 2)) + "minutes", outFile)
    
outFile.close()      


    



    