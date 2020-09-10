#!/usr/bin/python
#-*- coding: utf-8 -*-
#Abstract class Algo
'''
@attr: data : type Data (DataSimple, DataSpark, ...)
@method: run : execute Algo and get result in data
@method: save : save result in intermidate bdd MongoDb
'''
from Abs import Data
import pandas as pd
from abc import ABC, abstractmethod

class Algo:
    dataIn: Data
    id: int
    name: str
    data_out = []

    def set_dataout(self,data_out):
        self.data_out = data_out

    @abstractmethod
    def run(self):
        '''
        run algo using data .
        return Data object 
        '''
        pass

    @abstractmethod
    def save(self):
        pass



