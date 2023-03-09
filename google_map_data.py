# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 14:14:24 2023

@author: jiay
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 14:55:13 2023

@author: jiay
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

data collections

Created on Tue Dec 31 17:23:58 2022

@author: jiay
"""

import os
import json
import concurrent.futures
import pandas as pd
import googlemaps as gm # pip install googlemaps
from itertools import product
# dir(map_client.places_nearby)

class GoogleMapData(object):
    """
    This class stores settings and common methods in collecting Google map data from different APIs.
    """
    def __init__(self, api_key):
        self.map_client = gm.Client(api_key) 
        
    def load_location_table(self, file_path, file_name):
        upath = os.path.join(file_path, file_name)
        loca_file = pd.read_excel(upath, engine='openpyxl', dtype=({'MEAN_X':str,'MEAN_Y':str}))
        loca_file.rename(columns = {'MEAN_X':'longitude','MEAN_Y':'latitude'}, inplace = True)
        loca_file['location']= loca_file['latitude']  + ',' + loca_file['longitude'] 
        loca = loca_file['location'].values.tolist()
        return loca
    
    def json_write(self, path, file_name, obj):
        with open(os.path.join(path, file_name), encoding='utf-8-sig', mode = "w") as write_file:
            json.dump(obj, write_file, ensure_ascii=False)

    
class PlacesNearby(GoogleMapData):

    def __init__(self, path_config_file, config_file_name, api_key, radius=300):
        '''
            config_file_name: a .txt file contains the business types defined at  https://developers.google.com/maps/documentation/places/web-service/supported_types
        '''
        super(PlacesNearby, self).__init__(api_key)
        self.type_list = self.load_type_list(os.path.join( path_config_file, config_file_name))
        self.radius = radius
        
    @staticmethod
    def load_type_list(upath):
        with open(upath) as f:
            type_list = f.read().splitlines()
            f.close()
        return type_list

    @staticmethod
    def tuple_to_dic(utuple):
        return {'loc': utuple[0], 'btype': utuple[1]}
    
    def get_batch_loc(self, loc_list, multithreads=False):
        p = list(product(loc_list, self.type_list))
        if multithreads:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                out = list(executor.map(self.tuple_to_dic, p))
        else:
            out = list(map(self.tuple_to_dic, p)) 
        return out
    
    def map_request(self, loc_type_dic):
        try:
            res = {'location': loc_type_dic['loc'], 'btype': loc_type_dic['btype'],
                   'map_info': self.map_client.places_nearby(location=loc_type_dic['loc'],radius = self.radius, 
                                                type = loc_type_dic['btype'])}
        except:
            res = {'location': loc_type_dic['loc'], 'btype': loc_type_dic['btype'], 'map_info':'error'}
        return res

    def request_by_list(self, loc_list, multithreads=False):
        inputs = self.get_batch_loc(loc_list, multithreads=multithreads)
        if multithreads:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                out = list(executor.map(self.map_request, inputs))
        else:
            out = list(map(self.map_request, inputs))
        return out
    
    def request_by_file(self, upath, ufile, multithreads=False):
        loc_list = super(PlacesNearby, self).load_location_table(upath, ufile)
        return self.request_by_list(loc_list, multithreads=multithreads)

if __name__ == '__main__':
    import time
    #key = 'AIzaSyCQYhZMsEoz5cRPnEm0cXn9m-Nz8z53K2I'
    key = 'AIzaSyD3SXn4fzSgygjLFhnrtxyqLAAG-Dy7V_w'
    filepath = r'/mnt/c/Users/jiay/UrbanModel/'
    #filepath = r'c:\\users\\jiay\\UrbanModel'
    config_file_name = 'typelist.txt'
    location_table = 'mean_coor.xlsx'
    a = PlacesNearby(filepath, config_file_name, key)
    loclist = a.load_location_table(filepath, location_table)[0:50]
    #res = a.get_batch_loc(loclist, multicore = True)
    start = time.time()
    res = a.request_by_list(loclist, multithreads=True) 
    print(time.time() - start)
    a.json_write(filepath, "test.json", {"res": res})
    
    
    















