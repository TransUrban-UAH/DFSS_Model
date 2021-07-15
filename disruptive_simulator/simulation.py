# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Mon May 17 12:32:21 2021
@Version: 4.0
@Description: Disruptive vetorial cellullar automata simulation module.
              Core function of the simulation. Performs the simulation process,
              including setting up, iteratios and transition rules. Given a 
              GDF and the variables, gives as output the GDF with the 
              simulated uses.
"""

###############################################################################

# standard library imports
from copy import deepcopy
from gc import collect

# local imports
from writeLog import writeLog
from attraction_v2 import attraction
from potentials_v2 import potentials
from normalization import norm2

#-----------------------------------------------------------------------------#

def simulation(gdf, growth, df_coef, YYYY, frc, alfa, cicle, outFile, n_threads):
    '''
    # Funcion that performs the simulation
    #-------------------------------------------------------------------------#
    # Input parameters requirements:
    # - gdf: GeoDataFrame of the parcels
    # - growth: DataFrame with the growth values (anual or total)
    # - df_coef: DataFrame with the regression coefficents to compute attraction
    #            values
    # - YYYY: Final year to simulate
    # - frc: Resistance to change factor (0.9, 0.8, 0.7 ...)
    # - alfa: Random factor (0.9, 0.8, 0.7 ...)
    # - cicle: How many year represent each iteration (1, 2, 5, 10 ...)
    # - outFile: File to write the Logs on
    #-------------------------------------------------------------------------#
    # Output:
    #    GeoDataFrame with a columns of the simulated use for each parcel
    '''
    
    # starting year
    k = 2018

    writeLog("Preparing the simulation...", outFile)
    
    # set the total demand values for each use, using the growth DF
    demand_co_tot = growth.loc[0, "TOTAL_DEMAND"]
    demand_in_tot = growth.loc[1, "TOTAL_DEMAND"]
    demand_si_tot = growth.loc[2, "TOTAL_DEMAND"]
    demand_mu_tot = growth.loc[3, "TOTAL_DEMAND"]
    demand_mi_tot = growth.loc[4, "TOTAL_DEMAND"]

    # stating variables to store the surplusses/deficits of areas after each
    # iteration
    surplus_co = 0.0
    surplus_in = 0.0
    surplus_mi = 0.0
    surplus_mu = 0.0
    surplus_si = 0.0

    # add a field to store the results of the simulation. Use as base the 
    # current use of each parcel
    writeLog("Generating a field with the simulated use changes...", outFile)
    gdf["SIM_USE"] = gdf["USE_2018"]
    writeLog("Done.\n", outFile)
    
    # list with the possible uses that each parcel can have, based on the 
    # coefficent dataframe
    use_list = list(set(df_coef["origin_use"]))
    
    # list with the uses that a parcel can change to, excluding the vacant
    potential_use_list = deepcopy(use_list)
    potential_use_list.remove("vacant")
    
    # copying the atraccion values to set new fields with non normalized values
    for uso in potential_use_list:
        gdf["ATR_" + uso[0:2].upper() + "_UNST"] = gdf["ATR_" + uso[0:2].upper()]
        gdf["POT_" + uso[0:2].upper()] = 0.0

    # creating a field that will be a list of the sorted possible uses of each
    # parcel, from best to worst possible use
    gdf["ORDER_POT"] = [list() for x in range(len(gdf.index))] 
    
    # state a variable that will store all the developed parcels on each iteration
    total_developed = []
    
    #-------------------------------------------------------------------------#
    # setting restriccions related to scenario 2 characteristics
    
    corridor_restriction = []
    
    if "PR_CORR" in gdf.columns:
        for index, row in gdf.iterrows():
            if row["PR_CORR"] == 0:
                if row["USE_2018"] != "military" and\
                     row["USE_2018"] != "green_zones":
                        
                    gdf.loc[index, "DEVELOP"] = "vacant"
                    corridor_restriction.append(index)
    
    #-------------------------------------------------------------------------#
    # setting restriccions related to scenario 3 characteristics
    
    ls_loss = []
    ls_develop_loss = []
    
    if "LP" in gdf.columns:
        total_loss = growth.loc[5, "TOTAL_DEMAND"]

        gdf = gdf.sort_values("LP", ascending = False)
        ls_loss = gdf["ID"].tolist()
        gdf = gdf.sort_values("ID", ascending = True)
        
    else:
        total_loss = 0
        
    #-------------------------------------------------------------------------#
    
    writeLog("Executing the simulation...", outFile)
    
    # execute the simulation, iterating over the range of years using the cicle
    # value
    while k < YYYY:
        
        # fix iteration value to adjust to the year value on last iteration
        if YYYY - k < cicle:
            iteration = YYYY - k

        else:
            iteration = cicle
        
        # iteration range on each iteration
        iteration_range = str(k) + " - " + str(k + iteration)
        writeLog("\nIteration (Year): " + iteration_range, outFile)
        
        writeLog("\nUpdating the attraction values for each parcel...", outFile)
        
        # if there is any developed parcel 
        if total_developed:
            gdf["DO_ATT"] = 0
            
            # check if a parcel have any neighbout that changed its use
            for index,row in gdf.iterrows():
                vecinos = row["NBRS"]
                
                if any (item in vecinos for item in total_developed):
                    # if the used changed, mark it with a do_att 1
                    gdf.loc[index, "DO_ATT"] = 1
            # compute the attraction values just of the parcels whose 
            # neighbours changed their uses
            gdf = attraction(gdf, df_coef, n_threads)
        
        writeLog("Standarizing the values...\n", outFile)   
        
        # normalize the attraction values using the second method
        gdf = norm2 (gdf, potential_use_list)

        writeLog("Done.\n", outFile)
        
        #######################################################################
        
        # compute the potential of transition of each parcel
        writeLog("Calculating the potential uses values for each parcel...", outFile)
        
        gdf = potentials(gdf, use_list, frc, alfa, n_threads)
                    
        writeLog("Done.\n", outFile)
        
        #######################################################################
        
        # calculate the demand to be supplied on each iteration
        writeLog("Calculating the demand for each use on this iteration...", outFile)            
        demand_co = (demand_co_tot/32 * iteration) + surplus_co
        demand_in = (demand_in_tot/32 * iteration) + surplus_in
        demand_si = (demand_si_tot/32 * iteration) + surplus_si
        demand_mu = (demand_mu_tot/32 * iteration) + surplus_mu
        demand_mi = (demand_mi_tot/32 * iteration) + surplus_mi
        loss = (total_loss/32 * iteration)
        
        # setting the margins to surpass the demand
        medium_size_co = (growth.loc[0, "AVG_SIZE"]) * iteration
        medium_size_in = (growth.loc[1, "AVG_SIZE"]) * iteration
        medium_size_si = (growth.loc[2, "AVG_SIZE"]) * iteration
        medium_size_mu = (growth.loc[3, "AVG_SIZE"]) * iteration
        medium_size_mi = (growth.loc[4, "AVG_SIZE"]) * iteration
        
        writeLog("Done.\n", outFile)
        
        # setting variables to store the area developed on each iteration
        area_co = 0.0
        area_in = 0.0
        area_si = 0.0
        area_mu = 0.0
        area_mi = 0.0
        loss_area = 0.0

        # setting variables to store the ID of the parcels that develop each use
        develop_co_2 = []
        develop_in_2 = []
        develop_si_2 = []
        develop_mu_2 = []
        develop_mi_2 = []
        
        # resetting the value of total parcels developped on current iteration
        total_developed = []
        
        # check the demand values to decide to build or not
        # -------------------------
        
        if demand_co_tot > 0:
            build_co = True
        else:
            build_co = False
            
        # -------------------------
        if demand_in_tot > 0:
            build_in = True
        else:
            build_in = False
            
        # -------------------------
            
        if demand_si_tot > 0:
            build_si = True
        else:
            build_si = False
            
        # -------------------------
        
        if demand_mu_tot > 0:
            build_mu = True
        else:
            build_mu = False
            
        # -------------------------
            
        if demand_mi_tot > 0:
            build_mi = True
        else:
            build_mi = False
            
        # -------------------------
            
        if total_loss > 0:
            compute_loss = True
        else:
            compute_loss = False
    
        #######################################################################
        #######################################################################
        
        writeLog("Executing the simulation...", outFile)

        # if there is loss, compute them
        while compute_loss == True:

                parcel = ls_loss[0]
                ls_loss.pop(0)
                
                if len(ls_loss) > 0:

                    parcel_area = gdf.loc[parcel, "AREA"]
    
                    if ((loss_area + parcel_area) < loss):
                        loss_area += parcel_area
                        ls_develop_loss.append(parcel)
                        total_developed.append(parcel)
                        
                    else:
                        compute_loss = False
                        break
                else:
                    break
                
        #######################################################################
        
        # for each order priority (the order_pot field of each parcel) evaluate 
        # in descending order the potentials of each parcel. First check the 
        # best possible use for each parcel and use thas ones to develop. 
        # If there is none for a certain use, check the second priority order, 
        # to develop the remaining possible parcels that can fit better on 
        # the demand requirements
        
        for order in range(0,5):
          
            writeLog("\nFinding the potential parcels to be developed based on" +\
                     " the current priority (" + str(order) + ") [0 = best for" +\
                     " that use, 1 = second best, and so on] for each use.\n", outFile)
                
            # candidates of each use to develop
            develop_co = []
            develop_in = []
            develop_si = []
            develop_mu = []
            develop_mi = []
            
            # value of potencial of each candidate
            potential_co = []
            potential_in = []
            potential_si = []
            potential_mu = []
            potential_mi = []
            
            # number of candidates of each use and the total
            num_developed_co = len(develop_co_2)
            num_developed_in = len(develop_in_2)
            num_developed_si = len(develop_si_2)
            num_developed_mu = len(develop_mu_2)
            num_developed_mi = len(develop_mi_2)
            num_developed_total = len(total_developed)
            
            # choose the parcels that fits within the specifications previously
            # mentioned (using order priority to select for each use the 
            # parcels that have that use as theis best)
            for index, row in gdf.iterrows():
                
                if len(row["ORDER_POT"]) > 0 and \
                (index not in ls_develop_loss) and\
                (index not in corridor_restriction):
                    
                    if (row["ORDER_POT"][order] == "commerce_utilities") and\
                        (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                        (row["POT_CO"] != 0):
                        develop_co.append(index)
                        potential_co.append(row["POT_CO"])
                        
                    if (row["ORDER_POT"][order] == "industrial") and\
                        (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                        (row["POT_IN"] != 0):
                        develop_in.append(index)
                        potential_in.append(row["POT_IN"])
                        
                    if (row["ORDER_POT"][order] == "single_family") and\
                        (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                        (row["POT_SI"] != 0):
                        develop_si.append(index)
                        potential_si.append(row["POT_SI"])
                        
                    if (row["ORDER_POT"][order] == "multi_family") and\
                        (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                        (row["POT_MU"] != 0):
                        develop_mu.append(index)
                        potential_mu.append(row["POT_MU"])
                        
                    if (row["ORDER_POT"][order] == "mixed") and\
                        (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                        (row["POT_MI"] != 0):
                        develop_mi.append(index)
                        potential_mi.append(row["POT_MI"])
                        
            #-----------------------------------------------------------------#
            #-----------------------------------------------------------------# 
            
            # Aplication of the transition rules. Are the same for each use so 
            # only one will be explained

            # DEVELOPMENT OF COMMERCE AND UTILITIES
            while build_co == True:
                # if there is no parcels that have their best uses on current
                # priority, wait to the next order priority
                if len(develop_co) == 0:
                    
                    writeLog("There is no parcel whose max potential fits" +\
                             " current order priority (" + str(order) +\
                             ") for commerce and utilities. Will look for best parcels" +\
                             " to develop on next priority order.", outFile)

                    break
                
                else:
                    # choose the max potential value of the list of candidate
                    # parcels
                    x = max(potential_co)
                    
                    # look for the ID of the parcel
                    x_index = potential_co.index(x)
                    parcel = develop_co[x_index]
                    
                    # remove it from the lists
                    potential_co.remove(x)
                    develop_co.remove(parcel)
                    
                    # if the parcel haven´t been already developed on this
                    # iteration, select its area value
                    if parcel not in total_developed:
                        parcel_area = gdf.loc[parcel, "AREA"]

                        # if the current built area plus the area of the evaluated
                        # parcel do not exceed the demand value, use it to develop
                        # and store it in the pertinent lists
                        if ((area_co + parcel_area) < demand_co):
                            area_co += parcel_area
                            develop_co_2.append(parcel)
                            total_developed.append(parcel)
                        
                        # if the current area plus the area of the parcel is 
                        # equal or exceeds the the demand but in lesser
                        # amount that the medium  size plus demand, 
                        # build it and interrupt the building process
                        elif ((area_co + parcel_area) < (demand_co + medium_size_co))\
                            or ((area_co + parcel_area) == demand_co):
                                
                            area_co += parcel_area
                            develop_co_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_co = demand_co - area_co #compute the surplus
                            build_co = False
                            writeLog("The demand of commerce and facilities" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_co) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                        
                        # if there is no build area yet, but the area of the 
                        # parcel already exceeds the demand plus the medium 
                        # size threshold, build it and interrupt the process
                        elif (area_co == 0) and (parcel_area > (demand_co + medium_size_co)):
                            
                            area_co += parcel_area
                            develop_co_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_co = demand_co - area_co #compute the surplus
                            build_co = False
                            writeLog("The firts parcel with the higest potential" +\
                                     " to develop commerce and facilities is" +\
                                     " is bigger than demand. The demand will" +\
                                     " accumulate. " + str(surplus_co) +\
                                     " will be used " + " on next iteration.\n", outFile)
                            break
                        
                        # in any other case (I.E. not the first parcel too big)
                        # interrupt the process, and calculate the deficit
                        else:
                            surplus_co = demand_co - area_co #compute the deficit
                            build_co = False
                            writeLog("The demand of commerce and facilities" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_co) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                          
            #-----------------------------------------------------------------#

            # DEVELOPMENT OF INDUSTRIAL
            while build_in == True:
                
                if len(develop_in) == 0:
                    
                    writeLog("There is no parcel whose max potential fits" +\
                             " current order priority (" + str(order) +\
                             ") for industrial use. Will look for best parcels" +\
                             " to develop on next priority order.", outFile)

                    break

                else:
                    x = max(potential_in)
                    x_index = potential_in.index(x)
                    parcel = develop_in[x_index]
                    potential_in.remove(x)
                    develop_in.remove(parcel)
                    
                    if parcel not in total_developed:
                        parcel_area = gdf.loc[parcel, "AREA"]

                        if ((area_in + parcel_area) < demand_in):
                            area_in += parcel_area
                            develop_in_2.append(parcel)
                            total_developed.append(parcel)
                            
                        elif ((area_in + parcel_area) < (demand_in + medium_size_in))\
                            or ((area_in + parcel_area) == demand_in):
                                
                            area_in += parcel_area
                            develop_in_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_in = demand_in - area_in
                            build_in = False
                            writeLog("The demand of industrial use" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_in) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                        
                        elif (area_in == 0) and (parcel_area > (demand_in + medium_size_in)):
                            
                            area_in += parcel_area
                            develop_in_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_in = demand_in - area_in
                            build_in = False
                            writeLog("The first parcel with the higest potential" +\
                                     " to develop industrial use is" +\
                                     " is bigger than demand. The demand will" +\
                                     " accumulate. " + str(surplus_in) +\
                                     " will be used " + " on next iteration.\n", outFile)
                            break
                        
                        else:
                            surplus_in = demand_in - area_in
                            build_in = False
                            writeLog("The demand of industrial use" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_in) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                          
            #-----------------------------------------------------------------#
            
            # DEVELOPMENT OF SINGLE-FAMILITY RESIDENTIALS
            while build_si == True:
                
                if len(develop_si) == 0:
                    
                    writeLog("There is no parcel whose max potential fits" +\
                             " current order priority (" + str(order) +\
                             ") for single-family residentials. Will look for best parcels" +\
                             " to develop on next priority order.", outFile)

                    break
                
                else:
                    x = max(potential_si)
                    x_index = potential_si.index(x)
                    parcel = develop_si[x_index]
                    potential_si.remove(x)
                    develop_si.remove(parcel)
                    
                    if parcel not in total_developed:
                        parcel_area = gdf.loc[parcel, "AREA"]

                        if ((area_si + parcel_area) < demand_si):
                            area_si += parcel_area
                            develop_si_2.append(parcel)
                            total_developed.append(parcel)
                            
                        elif ((area_si + parcel_area) < (demand_si + medium_size_si))\
                            or ((area_si + parcel_area) == demand_si):
                                
                            area_si += parcel_area
                            develop_si_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_si = demand_si - area_si
                            build_si = False
                            writeLog("The demand of single-family residential" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_si) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                        
                        elif (area_si == 0) and (parcel_area > (demand_si + medium_size_si)):
                            
                            area_si += parcel_area
                            develop_si_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_si = demand_si - area_si
                            build_si = False
                            writeLog("The first parcel with the higest potential" +\
                                     " to develop single-family residential is" +\
                                     " is bigger than demand. The demand will" +\
                                     " accumulate. " + str(surplus_si) +\
                                     " will be used " + " on next iteration.\n", outFile)
                            break
                        
                        else:
                            surplus_si = demand_si - area_si
                            build_si = False
                            writeLog("The demand of single-family residential" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_si) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                          
            #-----------------------------------------------------------------#
            
            # DEVELOPMENT OF MULTI-FAMILITY RESIDENTIALS
            while build_mu == True:
                
                if len(develop_mu) == 0:
                    
                    writeLog("There is no parcel whose max potential fits" +\
                             " current order priority (" + str(order) +\
                             ") for multi-family residentials. Will look for best parcels" +\
                             " to develop on next priority order.", outFile)

                    break
                
                else:
                    x = max(potential_mu)
                    x_index = potential_mu.index(x)
                    parcel = develop_mu[x_index]
                    potential_mu.remove(x)
                    develop_mu.remove(parcel)
                    
                    if parcel not in total_developed:
                        parcel_area = gdf.loc[parcel, "AREA"]

                        if area_mu + parcel_area < demand_mu:
                            area_mu += parcel_area
                            develop_mu_2.append(parcel)
                            total_developed.append(parcel)
                            
                        elif ((area_mu + parcel_area) < (demand_mu + medium_size_mu))\
                            or ((area_mu + parcel_area) == demand_mu):
                                
                            area_mu += parcel_area
                            develop_mu_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_mu = demand_mu - area_mu
                            build_mu = False
                            writeLog("The demand of multi-family residential" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_mu) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                        
                        elif (area_mu == 0) and (parcel_area > (demand_mu + medium_size_mu)):
                            
                            area_mu += parcel_area
                            develop_mu_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_mu = demand_mu - area_mu
                            build_mu = False
                            writeLog("The first parcel with the higest potential" +\
                                     " to develop multi-family residential is" +\
                                     " is bigger than demand. The demand will" +\
                                     " accumulate. " + str(surplus_mu) +\
                                     " will be used " + " on next iteration.\n", outFile)
                            break
                        
                        else:
                            surplus_mu = demand_mu - area_mu
                            build_mu = False
                            writeLog("The demand of multi-family residential" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_mu) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                          
            #-----------------------------------------------------------------#
            
            # DEVELOPMENT OF MIXED RESIDENTIALS
            while build_mi == True:
                
                if len(develop_mi) == 0:

                    writeLog("There is no parcel whose max potential fits" +\
                             " current order priority (" + str(order) +\
                             ") for mixed residentials. Will look for best parcels" +\
                             " to develop on next priority order.", outFile)

                    break
                
                else:
                    x = max(potential_mi)
                    x_index = potential_mi.index(x)
                    parcel = develop_mi[x_index]
                    potential_mi.remove(x)
                    develop_mi.remove(parcel)
                    
                    if parcel not in total_developed:
                        parcel_area = gdf.loc[parcel, "AREA"]

                        if ((area_mi + parcel_area) < demand_mi):
                            area_mi += parcel_area
                            develop_mi_2.append(parcel)
                            total_developed.append(parcel)
                            
                        elif ((area_mi + parcel_area) < (demand_mi + medium_size_mi))\
                            or ((area_mi + parcel_area) == demand_mi):
                                
                            area_mi += parcel_area
                            develop_mi_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_mi = demand_mi - area_mi
                            build_mi = False
                            writeLog("The demand of mixed residential" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_mi) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                        
                        elif (area_mi == 0) and (parcel_area > (demand_mi + medium_size_mi)):
                            
                            area_mi += parcel_area
                            develop_mi_2.append(parcel)
                            total_developed.append(parcel)
                            surplus_mi = demand_mi - area_mi
                            build_mi = False
                            writeLog("The firdt parcel with the higest potential" +\
                                     " to develop mixed residential is" +\
                                     " is bigger than demand. The demand will" +\
                                     " accumulate. " + str(surplus_mi) +\
                                     " will be used " + " on next iteration.\n", outFile)
                            break   
                        
                        else:
                            surplus_mi = demand_mi - area_mi
                            build_mi = False
                            writeLog("The demand of mixed residential" +\
                                     " areas has been suplied, with a deficit/surplus" +\
                                     " of " + str(surplus_mi) + " that will be used" +\
                                     " on next iteration.\n", outFile)
                            break
                    
                
            #-----------------------------------------------------------------#
            
            
            writeLog("\n" + ("-")*100, outFile)
                
            writeLog("\nSUMMARY OF THE PARCELS THAT HAS CHANGED THEIR USE " +\
                     "BASED ON THE BEST FIT PRIORITY:\n", outFile) 
                
            writeLog("For the priority order " + str(order) + ", of the " +\
                     " total of " + str(len(total_developed) - num_developed_total) +\
                     " developed parcels, there have been developed:\n", outFile)

            # summary of the development results
            writeLog(str(len(develop_co_2) - num_developed_co) +\
                       " parcels destined to " + "commerce and facilities.", outFile)
            writeLog(str(len(develop_in_2) - num_developed_in) +\
                       " parcels destined to " + "industrial use.", outFile)
            writeLog(str(len(develop_si_2) - num_developed_si) +\
                       " parcels destined to " + "single-family residentials.", outFile)            
            writeLog(str(len(develop_mu_2) - num_developed_mu) +\
                       " parcels destined to " + "multi-family residentials.", outFile)
            writeLog(str(len(develop_mi_2) - num_developed_mi) +\
                       " parcels destined to " + "mixed residentials.", outFile)   

         
            writeLog("\n" + ("-")*100, outFile)
            
            # if all demand has been supplied or all order priorities have 
            # been evaluated, show finish message
            if build_co == False and build_in == False and build_si == False\
               and build_mu == False and build_mi == False:
                   
                   
                   writeLog("\nThe demand of all uses has been suplied, with " +\
                            "the order " + str(order) + " as the last " +\
                            "priority used. Showing the results.", outFile)
                   break
        #---------------------------------------------------------------------#      
             
        # update the new use and the iteration range on which each parcel
        # changed its use 
        for s in develop_co_2:
            gdf.loc[s, "DEVELOP"] = "commerce_utilities"
            gdf.loc[s, "ITERATION"] = iteration_range

        for s in develop_in_2:
            gdf.loc[s,"DEVELOP"] = "industrial"
            gdf.loc[s,"ITERATION"] = iteration_range

        for s in develop_si_2:
            gdf.loc[s,"DEVELOP"] = "single_family"
            gdf.loc[s,"ITERATION"] = iteration_range

        for s in develop_mu_2:
            gdf.loc[s,"DEVELOP"] = "multi_family"
            gdf.loc[s,"ITERATION"] = iteration_range

        for s in develop_mi_2:
            gdf.loc[s,"DEVELOP"] = "mixed"
            gdf.loc[s,"ITERATION"] = iteration_range
            
        if ls_develop_loss:
            for s in ls_develop_loss:
                gdf.loc[s,"DEVELOP"] = "vacant"
                gdf.loc[s,"ITERATION"] = iteration_range
            
        writeLog("\n" + ("-")*100, outFile)
        
        # Show final statistics
        writeLog("\nITERATION " + iteration_range + ":\n", outFile)
        writeLog("AREA DESTINED TO COMMERCE AND FACILITIES: " + str(area_co), outFile)
        writeLog("Number of parcels: " + str(len(develop_co_2)) + "\n", outFile)
        writeLog("AREA DESTINED TO USO INDUSTRIAL USE: " + str(area_in), outFile)
        writeLog("Number of parcels: " + str(len(develop_in_2)) + "\n", outFile)
        writeLog("AREA DESTINED TO SINGLE-FAMILY RESIDENTIALS: " + str(area_si), outFile)
        writeLog("Number of parcels: " + str(len(develop_si_2)) + "\n", outFile)        
        writeLog("AREA DESTINED TO MULTI-FAMILY RESIDENTIALS: " + str(area_mu), outFile)
        writeLog("Number of parcels: " + str(len(develop_mu_2)) + "\n", outFile)        
        writeLog("AREA DESTINED TO USO MIXED RESIDENTIALS: " + str(area_mi), outFile)
        writeLog("Number of parcels: " + str(len(develop_mi_2)) + "\n", outFile)
        
        # if there is loss to be computed
        if ls_develop_loss:
            writeLog("AREA LOST: " + str(loss_area), outFile)
            writeLog("Number of parcels: " + str(len(ls_develop_loss)) + "\n", outFile)  
        
        writeLog("\n" + ("-")*100, outFile)
        
        # evaluate the conversion. If a parcel is conveted, its a net loss of
        # that use for that iteration, and it counts as a deficit, that has to
        # be suplied in the next iteration
        for desarrolla in total_developed:
            uso_d = gdf.loc[desarrolla, "SIM_USE"]
            area_d = gdf.loc[desarrolla, "AREA"]
            
            if uso_d == "commerce_utilities":
                surplus_co += area_d
            if uso_d == "industrial":
                surplus_in += area_d
            if uso_d == "single_family":
                surplus_si += area_d
            if uso_d == "multi_family":
                surplus_mu += area_d
            if uso_d == "mixed":
                surplus_mi += area_d
                
        writeLog("\nLOST AREA FOR EACH USE (WHICH INCLUDE DEFICIT/SURPLUS" +\
                 " RELATED TO THE FULFILMENT OF DEMAND REQUIREMENTS):\n", outFile)
        
        writeLog("AREA OF COMMERCE AND FACILITIES: " + str(surplus_co), outFile)
        writeLog("AREA OF USO INDUSTRIAL USE: " + str(surplus_in), outFile)
        writeLog("AREA OF SINGLE-FAMILY RESIDENTIALS: " + str(surplus_si), outFile)
        writeLog("AREA OF MULTI-FAMILY RESIDENTIALS: " + str(surplus_mu), outFile)
        writeLog("AREA OF USO MIXED RESIDENTIALS: " + str(surplus_mi), outFile)
        
        # applying a similar but simpler rules as the previous ones to the last
        # iteration. Since the will not be next iteration, the deficits will
        # not be able to supply, so for this last one, an exhaustion GDF is
        # generated, where there is only vacant parcels. The parcels chosen 
        # this time are just the right area to ensure that the final loss
        # will be under the medium size parcel of each use
        if (k + iteration) == YYYY:
            
            writeLog("\n" + ("*")*100, outFile)
            
            writeLog("\nLast iteration reached, so that the remaining area" +\
                     " that has not been develop yep has to do it to reach" +\
                     " threshold established for each use. Any parcel that" +\
                     " fits in each order priority and that dont surpass remaining" +\
                     " amount of area lost will be used to develop.\n", outFile)
            
            # generate a GDF with only vacant land
            gdf_exhaust = gdf[gdf["SIM_USE"] == "vacant"]
            
            develop_co_2 = []
            develop_in_2 = []
            develop_si_2 = []
            develop_mu_2 = []
            develop_mi_2 = []
        
            total_developed2 = []
            
            # same rules as before
            for order in range(0,5):
                                    
                develop_co = []
                develop_in = []
                develop_si = []
                develop_mu = []
                develop_mi = []
                
                potential_co = []
                potential_in = []
                potential_si = []
                potential_mu = []
                potential_mi = []
                
                for index, row in gdf_exhaust.iterrows():
                    
                    if len(row["ORDER_POT"]) > 0 and\
                        ((index not in total_developed) and\
                         (index not in ls_develop_loss)) and\
                            (index not in corridor_restriction):
                        
                        if (row["ORDER_POT"][order] == "commerce_utilities") and\
                            (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                            (row["POT_CO"] != 0):
                            develop_co.append(index)
                            potential_co.append(row["POT_CO"])
                            
                        if (row["ORDER_POT"][order] == "industrial") and\
                            (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                            (row["POT_IN"] != 0):
                            develop_in.append(index)
                            potential_in.append(row["POT_IN"])
                            
                        if (row["ORDER_POT"][order] == "single_family") and\
                            (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                            (row["POT_SI"] != 0):
                            develop_si.append(index)
                            potential_si.append(row["POT_SI"])
                            
                        if (row["ORDER_POT"][order] == "multi_family") and\
                            (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                            (row["POT_MU"] != 0):
                            develop_mu.append(index)
                            potential_mu.append(row["POT_MU"])
                            
                        if (row["ORDER_POT"][order] == "mixed") and\
                            (row["SIM_USE"] != row["ORDER_POT"][order]) and\
                            (row["POT_MI"] != 0):
                            develop_mi.append(index)
                            potential_mi.append(row["POT_MI"])
                            
                #-------------------------------------------------------------#
                #-------------------------------------------------------------# 
                
                # simpler transition rules, just using parcels that will not 
                # exceed demans, util the medium size parcel per use is 
                # achieved
                while surplus_co > (medium_size_co / iteration):
                    
                    if len(develop_co) == 0:
                        break
                    else:
                        x = max(potential_co)
                        x_index = potential_co.index(x)
                        parcel = develop_co[x_index]
                        potential_co.remove(x)
                        develop_co.remove(parcel)
                        
                        area = gdf_exhaust.loc[parcel, "AREA"]
                        
                        if (parcel not in total_developed2) and (area < surplus_co):
                            
                            surplus_co -= area
                            develop_co_2.append(parcel)
                            total_developed2.append(parcel)

                while surplus_in > (medium_size_in / iteration):
                    
                    if len(develop_in) == 0:
                        break
                    else:
                        x = max(potential_in)
                        x_index = potential_in.index(x)
                        parcel = develop_in[x_index]
                        potential_in.remove(x)
                        develop_in.remove(parcel)
                        
                        area = gdf_exhaust.loc[parcel, "AREA"]
                        
                        if (parcel not in total_developed2) and (area < surplus_in):
                            
                            surplus_in -= area
                            develop_in_2.append(parcel)
                            total_developed2.append(parcel)                            

                while surplus_si > (medium_size_si / iteration):
                    
                    if len(develop_si) == 0:
                        break
                    else:
                        x = max(potential_si)
                        x_index = potential_si.index(x)
                        parcel = develop_si[x_index]
                        potential_si.remove(x)
                        develop_si.remove(parcel)
                        
                        area = gdf_exhaust.loc[parcel, "AREA"]
                        
                        if (parcel not in total_developed2) and (area < surplus_si):
                            
                            surplus_si -= area
                            develop_si_2.append(parcel)
                            total_developed2.append(parcel)                


                while surplus_mu > (medium_size_mu / iteration):
                    
                    if len(develop_mu) == 0:
                        break
                    else:
                        x = max(potential_mu)
                        x_index = potential_mu.index(x)
                        parcel = develop_mu[x_index]
                        potential_mu.remove(x)
                        develop_mu.remove(parcel)
                        
                        area = gdf_exhaust.loc[parcel, "AREA"]
                        
                        if (parcel not in total_developed2) and (area < surplus_mu):
                            
                            surplus_mu -= area
                            develop_mu_2.append(parcel)
                            total_developed2.append(parcel)

                while surplus_mi > (medium_size_mi / iteration):
                    
                    if len(develop_mi) == 0:
                        break
                    else:
                        x = max(potential_mi)
                        x_index = potential_mi.index(x)
                        parcel = develop_mi[x_index]
                        potential_mi.remove(x)
                        develop_mi.remove(parcel)
                        
                        area = gdf_exhaust.loc[parcel, "AREA"]
                        
                        if (parcel not in total_developed2) and (area < surplus_mi):
                            
                            surplus_mi -= area
                            develop_mi_2.append(parcel)
                            total_developed2.append(parcel)
                
            # update the last parcels with their new uses, and mark them
            for s in develop_co_2:
                gdf.loc[s, "DEVELOP"] = "commerce_utilities"
                gdf.loc[s, "ITERATION"] = iteration_range + (" (*)")
    
            for s in develop_in_2:
                gdf.loc[s,"DEVELOP"] = "industrial"
                gdf.loc[s,"ITERATION"] = iteration_range + (" (*)")
    
            for s in develop_si_2:
                gdf.loc[s,"DEVELOP"] = "single_family"
                gdf.loc[s,"ITERATION"] = iteration_range + (" (*)")
    
            for s in develop_mu_2:
                gdf.loc[s,"DEVELOP"] = "multi_family"
                gdf.loc[s,"ITERATION"] = iteration_range + (" (*)")
    
            for s in develop_mi_2:
                gdf.loc[s,"DEVELOP"] = "mixed"
                gdf.loc[s,"ITERATION"] = iteration_range + (" (*)")
                
            writeLog("Total extra of " + str(len(total_developed2)) +\
                       " parcels has been developed to ensure minimum total" +\
                       " demand loss.\n" , outFile)
                
            writeLog("THE FINAL LOST AREA FOR EACH USE IS:\n", outFile)
        
            writeLog("AREA OF COMMERCE AND FACILITIES: " + str(surplus_co), outFile)
            writeLog("AREA OF USO INDUSTRIAL USE: " + str(surplus_in), outFile)
            writeLog("AREA OF SINGLE-FAMILY RESIDENTIALS: " + str(surplus_si), outFile)
            writeLog("AREA OF MULTI-FAMILY RESIDENTIALS: " + str(surplus_mu), outFile)
            writeLog("AREA OF MIXED RESIDENTIALS: " + str(surplus_mi), outFile)
            
            # extend the total developed with the ones on last iteration
            total_developed.extend(total_developed2)
        
        # update all the parcels that developed
        for n in total_developed:
            gdf.loc[n, "SIM_USE"] = gdf.loc[n, "DEVELOP"]
        
        # next iteration
        k += iteration
        
        # clean memory
        collect()
    #-------------------------------------------------------------------------#
    
    # uptade the parcels that had the corridor restriction
    for n in corridor_restriction:
        gdf.loc[n, "SIM_USE"] = gdf.loc[n, "DEVELOP"]
    
    # list with the fields that are not needed, and will be deleted
    # before exporting the final shapefile
    unnecesary_fields = ["ATR_CO", "ATR_MU", "ATR_IN", "ATR_SI","ATR_MI",
                         "ATR_CO_UNST", "ATR_MU_UNST", "ATR_IN_UNST",
                         "ATR_SI_UNST","ATR_MI_UNST","POT_CO", "POT_MU",
                         "POT_IN", "POT_SI","POT_MI","ORDER_POT", "NBRS"]
    if "LP" in gdf.columns:
        unnecesary_fields.append("LP")
    elif "PR_CORR" in gdf.columns:
        unnecesary_fields.append("PR_CORR")
        
    gdf = gdf.drop(unnecesary_fields, axis = 1)
    
    # delete all variables...
    del k, demand_co ,demand_in, demand_mi, demand_mu, demand_si, demand_co_tot,\
        demand_in_tot, demand_mi_tot, demand_mu_tot, demand_si_tot,\
        unnecesary_fields, total_developed, total_developed2, total_loss,\
        surplus_co, surplus_in, surplus_mi, surplus_mu, surplus_si,\
        x, x_index, develop_co, develop_co_2, develop_in, develop_in_2,\
        develop_mi, develop_mi_2, develop_mu, develop_mu_2, develop_si,\
        develop_si_2, parcel, potential_co, potential_in, potential_mi,\
        potential_mu, potential_si, potential_use_list, iteration, iteration_range,\
        index, row, ls_develop_loss, ls_loss, build_co, build_in, build_mi,\
        build_mu, build_si, area, area_d, area_co, area_in, area_mi, area_mu,\
        area_si, loss, loss_area, s, n, num_developed_co, num_developed_in,\
        num_developed_mi, num_developed_mu, num_developed_total, num_developed_si,\
        
    collect()

    return gdf
