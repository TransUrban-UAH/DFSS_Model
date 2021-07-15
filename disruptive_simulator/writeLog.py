# -*- coding: utf-8 -*-
"""
@Author: UAH - TRANSURBAN - Nikolai Shurupov
@Date: Mon Feb 8 17:32:16 2021
@Version: 1.2
@Description: Disruptive vetorial cellullar automata simulation module.
              Encharged of the writing on a LOG file of every process of the
              simulation.
"""

###############################################################################

def writeLog(Msg, outFile):
    '''
    Write an input string on an input file. Has te objectivo fo tracing all the
    relevant processes that the model performs, storing it on a file as well as
    printing it to console.
    '''
    
    # print on console the string
    print (Msg)
    
    # wrinte the string on the output file after making a line break
    outFile.write("\n" + Msg)