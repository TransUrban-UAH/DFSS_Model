# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Thu Apr 8 20:05:44 2021
@Version: 2.2
@Description: Disruptive vetorial cellullar automata simulation module.
              Encharged of computing the potential values of parcels stored
              in a GeoDataFrame. Certain format of the GDF is required.
              It is based on parallel computation using Dask library.
"""

###############################################################################

# global imports
from random import uniform
from math import log
from copy import deepcopy
import dask.dataframe as dd
from dask.distributed import Client

#-----------------------------------------------------------------------------#

def lambda_row_pot(row, use_list, potential_use_list, frc, alfa):
    '''
    Compute the potential values of a row. As input it requires a list with 
    the different uses to compute the values and a list of potential uses that
    a row can have. Lastly it need the resistance to change factor value, frc,
    and the randomness factor, alfa
    '''
    # if the use of the parcel is inside of the group of the ones that are wanted
    # to simulate (military use, p.e. is excluded)
    if (row["SIM_USE"] in use_list):
        
        # list with the potential values
        ls_potentials = [] 
        
        # iterate over all potential uses (5)
        for use in potential_use_list:
            
            # making names abbreviation
            abrv = use[0:2].upper()
            
            # application of the potential formula
            pot = (row["DIST"] * row["S_" + abrv] *\
                   row["ATR_" + abrv] * row["ZONIF"]) *\
                  (1 + (-log(uniform(0, 1)))**alfa)
            
            # if the row is vacant or the same usa as the evaluated it keeps 
            # its value
            if row["SIM_USE"] == "vacant" or row["SIM_USE"] == use:
                value = pot
            # if not, a resistance to change factor is applied
            else:
                value = pot * frc 
                
            # value of the use is added to the row and to the list as well
            row["POT_" + abrv] = value
            ls_potentials.append(value)
        
        # the list with all the values is sorted, from max to min
        ls_pot_sorted = deepcopy(ls_potentials)
        ls_pot_sorted.sort(reverse = True)
        
        # position of the maximum is looked for and based on that new list with
        # the name in descending order of each use is stored to the row
        for item in ls_pot_sorted:
            use_position = ls_potentials.index(item)

            row["ORDER_POT"].append(potential_use_list[use_position])
                    
    return row

###############################################################################

def potentials(gdf, use_list, frc, alfa, n_threads):
    '''
    Add to a GeoDataFrame with parcels the potential values. Each row will have
    this values based on the list provided. With this, the resistance to change
    factor value, frc, and the randomness factor, alfa are needed. All
    computations are done using parallel computing with Dask, so a number of
    threads to use has to be declared.
    '''
    # generation a list without the vacant land
    potential_use_list = deepcopy(use_list)
    potential_use_list.remove("vacant")
    
    # generating a dask dataframe, using a number partition based on the 
    # number of threads that it has to be computed on
    ddgdf = dd.from_pandas(gdf, npartitions = n_threads)
    
    # It will be applied the lambda_row_pot function to each partition. So the
    # arguments needed for this functions are needed. In this case Dask will
    # not be able to predict the output data structure, so that it is needed to
    # declare it in the meta variable, being that the output format is the same
    # as the input one
    with Client(n_workers = n_threads, threads_per_worker = 1) as client:
       
       result = ddgdf.map_partitions(lambda ddf: ddf.apply(lambda_row_pot, axis = 1, 
                                                           raw = False, result_type = None,
                                                           args = (use_list, potential_use_list,
                                                                   frc, alfa)),
                                      meta = ddgdf).compute(scheduler = "distributed")
       client.shutdown()
           
    return result