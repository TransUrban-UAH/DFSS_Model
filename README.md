# Simulation of disruptive scenarios using vector-based cellular automata

## Summary

The simulation software is divided into eight main modules, plus different extra data preparation modules. Each module is encharged of some part of the process. Mainly, to carry out the simulation it is needed a vetorial file in shapefile format. This SHP must have the accesibility, suitability and zoning values for each parcel. Firstly 
it is needed to execute the neighborhood script, that will generate a DataFrame that will be stored as Excel (xlsx format) with the neighborhoods relations that
each parcel have based on different buffer sizes. After that, the metrics script will be executed to compute the attraction-repulsion
values for each parcel and each buffer distance, generating four DataFrames that will be saved again in xlsx format. With this values the regressions
sript will generate ecuations that better fit the data, as well as generate the plotted data. At that point, all the necessary processes are done
and simulations can be carried out. However, the initial attraction values (of the initial-current parcels state) will be always the same, so there
is an extra script that can run them in order to accelerate the first iteration, which can also be the only one (if it represents a 32 years) so preliminary
results can be obtained using this initial values. The growth per use values can be obtained via growth module, that use the scenary shapefile, where workshopped uses are stated. 

## Installation

The scripts have been built under python version 3.8.6 using Spyder as the IDE and Anaconda as the environment manager. All the required packages are
stated in the requirements.txt file.

## How to Run
As stated in the summary, firstly neighbourhood scipt has to be runned, then metrics and regressions. It is strongly recommended to run the 
initial_attraction but not mandatory. At that point simulations can be carried out. Growth values can be obtained via growth module, using workshopped data.

**A dataset of the municipality of Alcalá de Henares is given to work as an example of how to perform the whole process. This dataset includes all the necessary data to directly run the simulation but surely user can run all the required processes starting with the shapefile.**

## Files

* ``main_run.py``: The main script that user should open, modify according its needs and execute.
* ``main_set_up_data.py``: Skeleton of the simulation, that organize the rest of the modules and set up the data so the simulations can be carried out.
* ``attraction.py``: Module echarged of updating the attraction values of each parcel, making use of dask library to achieve maximum efficiency via paralel computing.
* ``potentials.py``: Module echarged of computing the potential values of each parcel, making use of dask library to achieve maximum efficiency via paralel computing.
* ``simulation.py``: Core module that performs the simulation itself, where the transition rules are established.
* ``accuracy.py``: Module that perform the accuracy assesment of the simulation results.
* ``normalization.py``: Module encharged of the normalization of the different data of the input shapefile.
* ``metrics.py``: Module encharged of the aplication of different neighborhood metrics and generation of attraction values.
* ``neighbours.py``: Module encharged of the generation of neighbourhood relations, using different buffer sizes.
* ``regressions.py``: Module encharged of fitting attraction values to different ecuations, with its related coefficents.
* ``initial_attraction.py``: Module encharged of the computation of the initial attraction values for each parcel and buffer size.
* ``growth.py``: Module encharged of the growth per use calculation, based on workshopped data.
* ``writeLog.py``: Small module for the LOG generation.
