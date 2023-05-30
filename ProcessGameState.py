'''
dependencies
!pip install pandas
!pip install numpy
!pip install pyarrow
'''

import pandas as pd
import numpy as np
import pyarrow

class ProcessGameState:
    '''
    Handles file ingestion and ETL

    Processes game state by checking player positions and 
    extracts weapon class from players
    '''

    def __init__(self):
        self.n_rows = 0
        self.n_edges = 0
        self.Zbounds = None

    def loadFile(self, file):
        '''
        Loads in parquet object from file path

        Parameters
        ----------
        file : str, file path of parquet file 
        `data/game_state_frame_data.parquet`

        Returns
        -------
        self : object
            Object with loaded data file
        '''
        self.data = pd.read_parquet(file)
        self.n_rows = len(self.data)
        return self

    def setXYBounds(self, vertices):
        '''
        Sets the XY area of interest

        Converts array of n vertices into array of n+1 edges
            Assumes that each subsequent vertices are connected

        Parameters
        __________
        verticies : {array-like} of shape (n_vertices, 2)
            (x,y) coordinates of each vertex the bonudary 

        Returns
        -------
        self : object
            Object with added self.edges and self.n_edges
        '''
        index_1 = -1
        index_2 = 0
        edges = []
        while index_2 < len(vertices):
            vertex_1 = vertices[index_1]
            vertex_2 = vertices[index_2]
            edge = [vertex_1, vertex_2]
            edges.append(edge)
            index_1 += 1
            index_2 += 1
        self.edges = np.array(edges)
        self.n_edges = len(edges)
        return self
    
    def setZBounds(self, bounds):
        '''
        Sets the Z axis bounds z1 <= Z <= z2

        Parameters
        ----------
        bounds : {array-like} of shape (2, )
            Lower and upper z axis int ranges

        Returns
        -------
        self : object
            Z bounds added
        '''
        self.Zbounds = bounds
        return self


    def process(self, bounds=True, weapons=True):
        '''
        Processes the game state

        Parameters
        ----------
        bounds : bool, default = True
            If True, will check whether each row falls within the boundary
                and adds 'inBounds' to data frame

        weapons : bool, default = True
            If True, will extract the weapons classes from the inventory column
                amd adds weapon_classes to data frame

        Returns
        -------
        self : object
            Processed Game State
        '''
        self._validateData()
        
        if bounds:
            positions = self.data[['x','y','z']]
            inBounds = np.zeros(self.n_rows, dtype=bool)
            for index, position in positions.iterrows():
                inBounds[index] = self._inBounds(position)
            self.data['inBounds'] = inBounds

        if weapons:
            self.data['weapon_classes'] = self._getWeapons()
        
        return self

    def _getWeapons(self):
        '''
        Gets the weapon classes in a players inventory

        Returns
        -------
        weapon_classes : {array-like} of shape (n_rows,n_weapons)
            The weapon classes of a players inventory
        '''
        weapon_classes = []
        inventories = self.data['inventory']
        for inventory in inventories:
            try:
                inventory.any()
                weapons = []
                for weapon in inventory:
                    weapons.append(weapon['weapon_class'])
                weapon_classes.append(weapons) 
            except:
                weapon_classes.append([])

        return weapon_classes
        
    def hasRfSMG(self):
        '''
        Checks if the 'weapons_classes' column has a rifle or an smg 

        Requires _getWeapons() to be invoked first though the process() method
        
        Returns
        -------
        self : object
            Object with added column 'hasRfSMG' of type bool where True if
            inventory contains a 'Rifle' or 'SMG'
        '''
        data = self.data
        hasRfSMG = np.zeros(self.n_rows, dtype=bool)
        for index, weapons in data['weapon_classes'].items():
            if 'Rifle' in weapons or 'SMG' in weapons:
                hasRfSMG[index] = True
        self.data['hasRfSMG'] = hasRfSMG
        
        return self

    def _validateData(self):
        '''
        Check for sufficient numnber of edges and missing data

        Raises
        -------
        ValueError
            Raised when insufficient number of edges or missing data
        '''
        if self.n_edges < 3:
            raise ValueError(
                "Boundary has an insufficient number of edges "
                "please check that there are 3 or more edges in boundary."
            )
        try: 
            self.data.empty
        except:
            print("Data is missing. Import data before processing game state.")

    def _inBounds(self, position):
        '''
        Checks if position is inside boundary through 
            Ray Casting Algorithm O(n)

        Parameters
        ----------
        position :  {array-like} of shape (3, )
            (x, y, z) cordinates of player
        '''
        count = 0
        xp, yp, zp = position
        z1, z2 = self.Zbounds
        for edge in self.edges:
            (x1, y1), (x2, y2) = edge
            if (yp < y1) != (yp < y2) and \
                xp < x1 + ((yp-y1)/y2-y2)*(x2-x1) and \
                    (zp >= z1) and (zp <= z2):
                count += 1

        return count%2 == 1
    
    '''
    def print(self):
        #print(self.data)
        print(self.data['inventory'][0][0]['weapon_class'])
        print('####')
        print(self.data['inventory'][0][1])
    '''

        
'''
bounds = [
    [-1735, 250],
    [-2024, 398],
    [-2806, 742],
    [-2472, 1233],
    [-1565, 580]
]

temp = ProcessGameState()
temp = temp.setBounds(bounds)
temp = temp.loadFile("data/game_state_frame_data.parquet")
temp.process()

print('gg')
temp.print()
print(temp.data['area_name'].unique())
print(temp.data['inventory'][0])

'''