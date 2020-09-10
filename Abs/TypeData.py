from enum import Enum
class TYPEDATA(Enum):
    FromIntialToSpark  = 'FromIntialToSpark'
    FromIntialToSimple = 'FromIntialToSimple'
    FromSparkToSpark = 'FromSparkToSpark'
    FromMongoToSpark = 'FromMongoToSpark'
    FromMongoToSimple = 'FromMongoToSimple'
    FromSparkToSimple = 'FromSparkToSimple'
    FromTssToSimple = 'FromTssToSimple'
    FromSimpleToSimple = "FromSimpleToSimple"


    def get(self, typestr):
        if typestr == self.FromMongoToSimple.value:
            return self.FromMongoToSimple
        elif typestr == self.FromIntialToSimple.value:
            return self.FromIntialToSimple
        elif typestr == self.FromMongoToSpark.value:
            return self.FromMongoToSpark
        elif typestr == self.FromIntialToSpark.value:
            return self.FromIntialToSpark
        elif typestr == self.FromSparkToSpark.value:
            return self.FromSparkToSpark

