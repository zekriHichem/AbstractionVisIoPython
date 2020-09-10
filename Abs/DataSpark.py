#!/usr/bin/python
#-*- coding: utf-8 -*-

from Abs.Data import Data
from Abs.TypeData import TYPEDATA
from livy.client import LivyClient
from livy.models import SessionState, StatementState

class DataSpark(Data):
    client: LivyClient
    session: int
    def __init__(self, type_data, name_input, session_id, LivyUrl, mongo, connection):
        '''
        constractor :
        :param typeData: type of data :  FromIntialToSpark , FromSparkTssToSpark , FromMongoToSpark
        :param nameInput: le nom de la table initial (ex: database.tablename) or mongo collection (umap14) or tss vavriable that exist in memory of spark
        :param sessionId: livy session id (idle session)
        :param LivyUrl: livy URL (ex : http://localhost:8998/)
        :param mongoUrl: url mongo sheard bdd (mongo://localhost:27017/)
        '''
        self.client = LivyClient(LivyUrl)
        self.type_data = type_data
        self.name_input = name_input
        self.mongo = mongo
        self.session = session_id
        self.connection = connection
        self.data = self.upload_data()


    def upload_data(self, ):
        '''
        get data to interactive spark session
        :return: return code to execute it with livy statement
        '''
        if self.type_data == TYPEDATA.FromIntialToSpark :
            return self.from_intial_to_spark()
        elif self.type_data == TYPEDATA.FromMongoToSpark :
            return self.from_mongo_to_spark()
        elif self.type_data == TYPEDATA.FromSparkToSpark:
            return self.from_spark_tss_to_spark()
        return None

    def from_intial_to_spark(self):
        '''

        :return: code if type is  fromIntialToSpark
        '''
        s = self.connection.get_connection_spark(self.name_input)
        code = '''
       import org.apache.spark.sql.SQLContext
       import com.mongodb.spark.config._
       import com.mongodb.spark._
       import com.github.unsupervise.spark.tss.core._
       val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
       ''' + s + '''
       val inputTss = TSS.build(dfInput , time = \" time \" )
        '''
        return code

    def from_mongo_to_spark(self):
        '''
             :return: code if type is  fromMongoToSpark
        '''
        collection_read = ' val readConfig = ReadConfig(Map(\"collection" -> \" '+self.name_input+'\", \"readPreference.name\" -> \"secondaryPreferred\"), Some(ReadConfig(sc)))'

        code = '''
       import org.apache.spark.sql.hive.HiveContext
       import com.mongodb.spark.config._
       import com.mongodb.spark._
       import com.github.unsupervise.spark.tss.core._
        ''' + collection_read + '''
        val dfInput = MongoSpark.load(sc, readConfig)
        val inputTss = TSS(dfInput)
        '''
        return  code

    def from_spark_tss_to_spark(self):
        '''
                :return: code if type is  fromSparkTssToSpark
        '''
        code = 'val inputTss = '+ self.name_input
        return code

#Run code on spark session
    def run(self, code):
        '''
        Run code on spark session and get state
        3 check :
        1/- server is running
        2/- session is idel
        3/- statement is AVAILABLE
        :param code: upload data + algo code
        :return: True or Flase : True if algo run and seccuss and flase and state if something is error
        '''
        code = self.upload_data() + code
        try:
            state = self.client.get_session(self.session).state
            if state == SessionState.IDLE:
                statment = self.client.create_statement(self.session,code)
                statment_id = statment.statement_id
                # wait until algo is AVAILABLE or ERROR
                while statment.state == StatementState.RUNNING:
                    statment = self.client.get_statement(self.session,statment_id)
                if statment.state == StatementState.AVAILABLE:
                    return True, 'God job'
                else:
                    return False, statment.state.value
            else:
                return False, state.value
        except: 
            return False, 'Spark or Livy server problem '

        



