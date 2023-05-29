'''
dependencies
!pip install pandas
!pip install pyarrow
'''

import pandas as pd
import numpy as np
import pyarrow
import os
import json

class ProcessGameState:
    '''
    
    '''


    def __init__(self):
        pass

    def setBoundary(self, edges):
        '''
        Sets the bondary for areas of interest

        Parameters
        __________
        edges : {array-like} of shape (n_edges, 2)
        '''
        pass


    def process(self, file, edges):
        '''
        Loads in parquet object from file path, and processes the game state

        Parameters
        ----------
        file : str, file path of parquet file 
        `data/game_state_frame_data.parquet`

        Returns
        -------
        self : object
            Processed Game State
        '''
        self.data = pd.read_parquet(file)
        edges = None
        return self

    def print(self):
        #print(self.data)
        print(self.data['inventory'][0][0]['weapon_class'])
        print('####')
        print(self.data['inventory'][0][1])

    def _getWeapons(self, data):
        '''
        
        '''
        weapon_classes = np.array()
        inventories = data['inventory']
        for inventory in inventories:
            weapons = np.array()
            for weapon in inventory:
                weapons = np.append(weapons, [weapon['weapon_class']])
            weapon_classes = np.append(weapon_classes, weapons)
        

    def _checkBoundaries(self, edges):
        '''
        
        '''
        points = self.data[['x','y']]
        edges
        pass


temp = ProcessGameState().process("data/game_state_frame_data.parquet", None)
print('gg')
temp.print()

