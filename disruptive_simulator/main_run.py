# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Tue May 25 17:25:00 2021
@Version: 1.1
@Description: Disruptive vetorial cellullar automata simulation module.
              Main script to be executed. It works as the interaction with 
              the user, where he can define all the variables values to be 
              used for the simulations. Call the main function passing it the
              variables values.
"""

###############################################################################

def main():
    
    try:
        
        #---------------------------------------------------------------------#
        
        # global imports
        from os.path import dirname
        from os import chdir
        from sys import exc_info
        
        # local import
        import main_set_up_data as msud
        
        #---------------------------------------------------------------------#
        
        # setting the working directory. If no explicit wd it will make by
        # default the wd in which the script folder is saved
        
        wd = 0
        #wd = "C:\\comp\\sample_Los_santos\\scenario_1"
        
        if wd == "" or wd == 0 or wd == None:
            
            # establishing the working directory basing on the ubication of the
            # file
            sim_folder = dirname(__file__)
            wd = dirname(sim_folder)
            chdir(wd)
            
            scenario_list = ["scenario_3"]
        else:
            scenario_list = wd.split("\\")[-1:]
            wd = dirname(wd)
            chdir(wd)
        
        #######################################################################
        
        # fixed variables across simulations
        #---------------------------------------------------------------------#
        
        YYYY = 2050
        cicle = 2
        n_threads = 61
        
        #######################################################################
        
        # zoning factor options
        #---------------------------------------------------------------------#
        z_default = True
        z_urban = 2
        z_nonurban = 0.1
        z_protected = 0
        
        #######################################################################
        
        # list of different values of variables to use on simulations
        #---------------------------------------------------------------------#
        
        #dist_list = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]
        dist_list = [150, 200]
        #dist_list = [50]
        
        vec_list = ["vF_all", "vF_changes", "vNI_all", "vNI_changes"]
        #vec_list = ["vNI_all"]
        
        #frc_list = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
        frc_list = [0.1, 0.3, 0.5, 0.7, 0.9]
        #frc_list = [0.1]
        
        #alfa_list = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40]
        alfa_list =  [0]
        
        #---------------------------------------------------------------------#

        msud.set_up_data(wd, scenario_list, z_default, z_urban, z_nonurban,
                         z_protected, vec_list, dist_list, frc_list, alfa_list,
                         cicle, YYYY, n_threads)
    
    except: # catch *all* exceptions
        e = exc_info()[0]
        print("<p>Error: %s</p>" % e)

#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    main()
