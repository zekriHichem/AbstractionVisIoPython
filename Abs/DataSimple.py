#!/usr/bin/python
#-*- coding: utf-8 -*-
import pandas as pd
import pyodbc
from Abs.TypeData import TYPEDATA
from Abs.Data import Data
from Abs.Connection import Connection

class DataSimple(Data):
   
    def __init__(self, type_data, name_input, mongo, datetime):
        '''
              constractor :
              :param typeData: type of data :  FromIntialToSpark , FromSparkTssToSpark , FromMongoToSpark
              :param nameInput: le nom de la table initial (ex: database.tablename) or mongo collection (umap14) or tss vavriable that exist in memory of spark
              :param mongo: url mongo sheard bdd (mongo://localhost:27017/)
        '''
        self.type_data = type_data
        self.name_input = name_input
        self.mongo = mongo
        self.datetime = datetime

    def upload_data(self, ):
        if self.type_data == TYPEDATA.FromIntialToSimple.value:
            return self.from_initial_to_simple()
        elif self.type_data == TYPEDATA.FromMongoToSimple.value:
            return self.from_mongo_to_simple()
        elif self.type_data == TYPEDATA.FromSparkToSimple.value:
            return self.from_mongo_to_simple()
        elif self.type_data == TYPEDATA.FromTssToSimple.value:
            return self.from_mongo_to_simple()
        elif self.type_data == TYPEDATA.FromSimpleToSimple.value:
            return self.from_mongo_to_simple()
        return None

    def from_initial_to_simple(self):

        with pyodbc.connect(self.get_connection(self.name_input).get_connection_simple(), autocommit=True) as conn:
            df = pd.read_sql("select * from " + self.name_input.split("/")[5],  conn)
        return df

    def get_connection(self, input_name):
        host = self.name_input.split("/")[0]
        port = self.name_input.split("/")[1]
        user = self.name_input.split("/")[2]
        password = self.name_input.split("/")[3]
        db = self.name_input.split("/")[4]

        return  Connection(host=host,port=port,database=db,user=user,password=password)

    def from_mongo_to_simple(self):
        cur = self.mongo.dblp[self.name_input].find()
        df = pd.DataFrame([])
        for i in cur :
            df[i['cols']] = i['series']
        return df


