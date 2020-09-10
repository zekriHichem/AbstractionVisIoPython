from Abs.Algo import Algo
from pymongo import MongoClient
from abc import ABC, abstractmethod

class AlgoSimple(Algo):
    is_MTS: bool
    client : MongoClient()
    columns = []
    save_with_col: bool


    def set_columns(self,colimns):
        self.columns = colimns
    def save(self):
        lisz = self.data_out
        print(type(lisz))
        i = 0
        print(self.name + self.id.__str__())
        if (self.save_with_col):
            for l in lisz:
                if self.is_MTS == "1":
                    self.client.dblp[self.name + self.id.__str__()].insert({'cols':  i.__str__(), 'series': l})
                else:
                    self.client.dblp[self.name + self.id.__str__()].insert({'cols': self.columns[i], 'series': l})
                i = i + 1
        else:
            for l in lisz:
                self.client.dblp[self.name + self.id.__str__()].insert({'cols': self.columns[i], 'series': l})
                i=i+1




