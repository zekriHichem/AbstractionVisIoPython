#!/usr/bin/python
#-*- coding: utf-8 -*-
from Abs.TypeData import TYPEDATA
from abc import ABC, abstractmethod

class Data:
    type_data: TYPEDATA
    name_input: str
    mongo: str
    datetime: str

    def upload_data(self, ):
        pass




