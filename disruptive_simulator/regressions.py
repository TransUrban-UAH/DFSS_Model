# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov - Ramón Molinero Parejo
@Date: Mon Apr 12 14:05:23 2021
@Version: 2.0
@Description: Disruptive vetorial cellullar automata simulation module.
              Extra module that compute and generate a xlsx with the initial
              attraction values for each parcel and buffer distance.
"""

###############################################################################

# general imports
from numpy import log, exp, array, var
from pandas import read_excel
from os import chdir, makedirs
from os.path import join
from glob import glob
from copy import deepcopy
from statistics import mean

# imports to make plots
from matplotlib import pyplot as plt
from matplotlib.font_manager import FontProperties

# imports to perform regressions
from scipy.optimize import curve_fit
from sklearn.metrics import mean_squared_error as MSE

# imports to linearize the exponential and potential functions
from sklearn.preprocessing import FunctionTransformer as FT

#-----------------------------------------------------------------------------#

# definition of different types of ecuations to use regressions with

def f_poly_g1(x, a, b):
    return a*x + b

def f_poly_g2(x, a, b, c):
    return a*x**2 + b*x + c

def f_poly_g3(x, a, b, c, d):
    return a*x**3 + b*x**2 +c*x + d

#def f_poly_g4(x, a, b, c, d, e):
    #return a*x**4 + b*x**3 + c*x**2 + d*x + e

def f_log_0(x, a, b):
    return a * log(x) + b

def f_log_P(x, a, b, c):
    return a * log(b * x) + c

def f_exponential(x, a, b):
    return a * exp(b * x)

def f_potential(x, a, b):
    return a * (x ** b)

#def f_exponential(x, a, b, c):
    #return a * exp(b * x) + c

#def f_potential(x, a, b, c):
    #return a * (x ** b) + c

def f_exponential_lin(x, a, b):
    return log(a) + (b * x)

def f_potential_lin(x, a, b):
    return log(a) + (b * log(x))

#-----------------------------------------------------------------------------#
def name(function):
    '''
    Returns the name of a function inputted
    '''
    return function.__name__

#-----------------------------------------------------------------------------#
def compute_r2(y_true, y_pred):
    '''
    Basic function to evaluate the R2 of a data set. List/array with values of
    observed Y and then ones generated with the regression function.
    '''
    sse = sum((y_true - y_pred)**2)
    tse = (len(y_true) - 1) * var(y_true, ddof=1)
    r2_score = 1 - (sse / tse)
    return r2_score

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

# transformation function to linearize
transformer = FT(log, validate = True)

# base working directory
base = "C:\scenarios\scenario_3"

# folder with the raw attraction-repulsion dataframes in xlsx format
att_rep_folder = "attraction_values"

wd = join(base, att_rep_folder)
chdir(wd)

# list with the atraction-repulsion dataframes
xlsx_list = glob("*.xlsx")

# making a list with the functions to perform regressions 
functions = [f_poly_g1, f_poly_g2, f_poly_g3, f_log_0, f_exponential,\
             f_potential]

# list with the names of the functions 
function_names = list(map(name, functions))

# construction of a dictionary to store names as keys and function objects as 
# items
d_functions = dict(list(zip(list(map(name, functions)), functions)))

# names of the coefficents
coef_names = ["a", "b", "c", "d", "e"]

# dictionaries to store the results of the regressions
d_all_correlations = dict.fromkeys(xlsx_list, None) # all data
d_best_correlations = dict.fromkeys(xlsx_list, None) # just best regressions

# variable to decide if use linear version of the potencial and exponential
# functions
lin = False

# iterate over the attraction values DFs
for xlsx in xlsx_list:
    
    # dictionary to store all information related to current regression
    d_correlation_study = dict.fromkeys(function_names, None)
    
    # read the attraction DF
    df = read_excel(join(wd, xlsx))
    
    # delete any row that do not have value in any field. Applied only to 
    # "change" type functions if there will not be linearization. If linear 
    # version will be selected, it is required to delete any 0 value, since it
    # cannot  be done the logarithm
    if xlsx == xlsx_list[1] or xlsx == xlsx_list[3] or lin == True:
        
        df = df[(df.all(axis = 1))]
    
    # X values are the fields of the input file (the xlsx with the attraction
    # values). First 2 fields are avoided since they store the combination
    # names of evaluated and origin use
    array_x = array(df.columns[2:], dtype=float)
        
    # iterate over the different ecuations to use regression with
    for f in d_correlation_study.keys():
        
        # create a DF with the same structure as the input one
        d_correlation_study[f] = deepcopy(df[{"origin_use", "evaluated_use"}])
                
        # iterate over the current attraction values DF
        for index, row in df.iterrows():
            
            # Y will be an array with the row values of the read DF, again
            # avoinding the origin and evaluated use 
            array_y = deepcopy(array(row[2:], dtype=float)   )      
            
            if lin == True:
                
                # linearize the exponential and potential formulas
                if f == "f_exponential" or f == "f_potential":
                    
                    # each has to be tread separately
                    if f == "f_exponential":
                        
                        # reshape is needed so the transformer can accept
                        # the format
                        y_reshape = deepcopy(array_y.reshape(-1, 1))
                        
                        # compute the transformed version
                        y_trans = transformer.fit_transform(y_reshape)
                        

                        # compute the coefficients (need to do the inverse reshape)
                        # curve_fit function of scipy library is executed:
                        # input: function to apply, values of x and values of y.
                        # maxfev indicates the number of calls to the regression
                        # function. Default is 600, but with this set of data it 
                        # is needed to increase it significantly
                        popt, pcov = curve_fit(f_exponential_lin, array_x,\
                                               y_trans.reshape(11,), maxfev = 15000)
                        
                    else:
                        # analogous to the previous
                        y_reshape = deepcopy(array_y.reshape(-1, 1))
                        y_trans = transformer.fit_transform(y_reshape)
                        
                        popt, pcov = curve_fit(f_potential_lin, array_x,\
                                               y_trans.reshape(11,), maxfev = 15000)
                    
                else:
                    # not needed linearize for the rest of ecuations
                    popt, pcov = curve_fit(d_functions[f], array_x, array_y,
                                           maxfev=15000)
            else:
                

                # non linear version just requires to give an initial guess of
                # the local minimum
                if f == "f_exponential":
                    popt, pcov = curve_fit(d_functions[f], array_x, array_y,\
                                           p0=[0, 0], maxfev = 15000)
                
                else:
                    popt, pcov = curve_fit(d_functions[f], array_x, array_y,\
                                           maxfev = 15000)
                
            # add the variables
            for variable in range (0, len(popt)):
                
                d_correlation_study[f].loc[index,\
                                             coef_names[variable]] = popt[variable]
            
            # calculate of the predicted y to compute the error
            y_pred = []
                    
            for p in array_x:
                
                valor_pred = deepcopy(d_functions[f](p, *popt))
                y_pred.append(valor_pred)
            
            # errors computation
            R_SQRT = compute_r2(array_y, y_pred)
            v_MSE = MSE(array_y, y_pred)
            
            # add the error values
            d_correlation_study[f].loc[index, "R_SQRT"] = R_SQRT
            d_correlation_study[f].loc[index, "MSE"] = v_MSE
            
    # add the dictionary to the main one
    d_all_correlations[xlsx] = deepcopy(d_correlation_study)
    
    # create another DF to store the best regression
    df_neighbours_correlation =  deepcopy(df[{"origin_use", "evaluated_use"}])
       
    # iterate over the rows
    for index, row in df_neighbours_correlation.iterrows():
        
        # list to save R2 values
        R2_list = []
        
        # list to save MSE values
        MSE_list = []
        
        # iterate over the main dictionary
        for key in d_correlation_study.keys():
            
            # search every value
            R2_value = deepcopy(d_correlation_study[key].loc[index, "R_SQRT"])
            MSE_value = deepcopy(d_correlation_study[key].loc[index, "MSE"])
            
            # add to the list
            R2_list.append(R2_value)
            MSE_list.append(MSE_value)
        
        if len (R2_list) > 0:
             
            # search for the maximum value, which indicates the name of the 
            # applied formula that fits better
            best_R2_fit = R2_list.index(max(R2_list))
            function_name = name(functions[best_R2_fit])
            
            # create the fields and add the values
            df_neighbours_correlation.loc[index, "best_based_on_R2"] = function_name
            df_neighbours_correlation.loc[index, "R2_value"] = max(R2_list)
            
        if len (MSE_list) > 0:
            
            # same as before but using MSE instead
            best_MSE_fit = MSE_list.index(min(MSE_list))
            function_name = name(functions[best_MSE_fit])
            df_neighbours_correlation.loc[index, "best_based_on_MSE"] = function_name
            df_neighbours_correlation.loc[index, "MSE_value"] = max(MSE_list)
        
    # add to the main dictionary each result
    d_best_correlations[xlsx] = deepcopy(df_neighbours_correlation)

# lastly, create a DF to store the coefficents of the regression that better 
# fit the whole data set      
d_final_coefficients = dict.fromkeys(xlsx_list)

for key in d_final_coefficients.keys():
    
    # list to save the mean R2 of each type of regression
    mean_R2_values = []
    
    for key2 in d_all_correlations[key]:
        
        # list to save all the R2 values of each regression (each pair use)
        R2_values = []

        # search for each value
        for index, row in d_all_correlations[key][key2].iterrows():
            
            R2_values.append(row["R_SQRT"])
        
        # compute the mean and add it
        mean_R2_values.append(mean(R2_values))
    
    # search for the best mean R2
    best_fit = mean_R2_values.index(max(mean_R2_values))
    
    # generate a DF based on the regression with the best results
    d_final_coefficients[key] = deepcopy(d_all_correlations[key][name(functions[best_fit])])
  
# directory to save the results
dir_coef = base + "\\coefficients"
try: 
    makedirs(dir_coef)
    chdir(dir_coef)
except:
    chdir(dir_coef)

for key in d_final_coefficients.keys():
    
    # delete from the name the ".xlsx" and save each DF with its name
    d_final_coefficients[key].to_excel(key[0:-5] + "_coefficients.xlsx", index = False)

'''Plot generation'''

chdir(wd)

# lists of the uses, colors to use on each one, and title to be displayed
ls_uses = ["commerce_utilities", "industrial", "single_family", "multi_family",
           "mixed", "vacant"]

ls_colors = ["#B53535", "#704489", "#6699CD", "#002673", "#F6C567", "gray"]

ls_titles = ["Commerce and utilities", "Industrial", "Single-family residential",
             "Multi-family residential", "Mixed (commercial & residential)", 
             "Non urban"]

ls_dist = [25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 500]

# Font, size and characteristics of the labels
font_legend = FontProperties(size = 12)
font_label = FontProperties(style = 'normal', size = 9)
font_tittle = FontProperties(weight = 'bold')

# iterate over the results
for xlsx in xlsx_list:
    
    # read the DF
    df = read_excel(xlsx)
    
    # format to display the data (pair of uses)
    nrow, ncol = 3, 2
    axe1, axe2 = 0, 0
    
    # log(vF) will be displayed as y axis
    if "vF" in xlsx:
        for dist in ls_dist:
            df[dist] = df[dist].apply(lambda x: log(x))
    
    # subset of plots
    figure, axes = plt.subplots(nrows = nrow, ncols = ncol)
    
    # iterate over te uses
    for use in ls_uses:
        
        df_uso = deepcopy(df[df["origin_use"] == use])
        df_uso = df_uso.drop("origin_use", axis = 1)
        df_uso = df_uso.set_index("evaluated_use")
        df_uso_t = df_uso.transpose()
        df_uso_t.index = df_uso_t.index.rename('distancia')
        
        for use2 in ls_uses:
                
            axes[axe1, axe2].plot(use2, data = df_uso_t,
                                  color = ls_colors[ls_uses.index(use2)],
                                  linewidth = 1.4)
            
        axes[axe1, axe2].set_title(ls_titles[ls_uses.index(use)],
                                   loc = "left", FontProperties = font_tittle)
        if axe1 < nrow - 1:
            axe1 += 1
        elif axe1 == nrow - 1: 
            axe1 = 0
            axe2 += 1

    # labels of the axis
    if xlsx[0:2] == "vF":
        y_label = "Log(" + xlsx[0:2] + ")"
    else:
        y_label = xlsx[0:3]
    for axe in axes.flat:
        axe.set_xlabel('Distance (m)', Fontproperties = font_label, loc = "right")
        axe.set_ylabel(y_label, color = "#D16103", loc = "top")

    # borders of the plots
    for axe in axes.flat:
        axe.spines['right'].set_visible(False)
        axe.spines['top'].set_visible(False)
        axe.spines['bottom'].set_visible(False)
        axe.spines['left'].set_visible(False)
    
    # plot grids
    for axe in axes.flat:
        axe.grid(color = 'gainsboro', linestyle = '-', linewidth = 0.75)
    
    # leyend
    figure.legend(('Commerce & utilities', 'Industrial','Single-family residential',
                  'Multi-family residential', 'Mixed', 'Non urban'), loc = 8,
                  ncol = 3, bbox_to_anchor=(0.27, -0.14, 0.5, 0.5), 
                  frameon = False, prop = font_legend)
    
    # size
    figure.set_size_inches(8, 6)
    
    # spacing
    figure.tight_layout(pad = 0, w_pad = 4, h_pad = 2)
    
    # export the plots
    figure.savefig("graf_" + xlsx[:-5] + ".jpg", quality = 100, dpi = 500,
                   bbox_inches = 'tight')
    
    figure.show()

        
                
