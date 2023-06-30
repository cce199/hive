import time
from TCLIService.ttypes import *
from pyspark.sql import SparkSession
import pyspark

class dataProcessHandler:
    def __init__(self):
        pass

class dataProcessSparkHandler():
    def __init__(self, cores="4", memory="20Gi", mount_path="/root"):
        from sparkdriver import K8sSparkDriver
        self.driver = K8sSparkDriver(cpu=cores, memory=memory, mount_path=mount_path, remote=True)
        self.spark = None
        self.df = None
        self.sparkContext = None
        self.schema = None
        self.resultRows = None
        self.fqueryCnt = 0

    def hasSparkContext(self):
        if self.sparkContext:
            return True
        return False
    # def getDriver(self):
    #     return self.driver
    
    def createExecutor(self, instances="2", memory="15g", cores="5"):
        config = {
            "spark.executor.instances": instances,
            "spark.executor.memory": memory,
            "spark.executor.cores": cores,
            # "spark.driver.memory": "10g",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3.impl": "com.amazon.ws.emr.hadoop.fs.EmrFileSystem",
            "spark.hadoop.fs.s3n.impl": "com.amazon.ws.emr.hadoop.fs.EmrFileSystem",
            "spark.hadoop.fs.s3bfs.impl": "org.apache.hadoop.fs.s3.S3FileSystem",
            "spark.hadoop.fs.s3.buffer.dir": "/opt/mnt/s3",
            "spark.executorEnv.SPARK_USER": "root",
            'spark.kubernetes.namespace': "spark-operator",
            "spark.kubernetes.node.selector.alpha.eksctl.io/nodegroup-name": "ng-memory-5-spark",
        }
        self.sparkContext = self.driver.getSparkContext(config)
        self.spark = SparkSession(self.sparkContext)
    
    def executQuery(self, query):
        self.df = self.spark.sql(query)
        # print(result)
        return True
    
    def getResultSchema(self):
        # "BOOLEAN_TYPE": 0,
        # "TINYINT_TYPE": 1,
        # "SMALLINT_TYPE": 2,
        # "INT_TYPE": 3,
        # "BIGINT_TYPE": 4,
        # "FLOAT_TYPE": 5,
        # "DOUBLE_TYPE": 6,
        # "STRING_TYPE": 7,
        # "TIMESTAMP_TYPE": 8,
        # "BINARY_TYPE": 9,
        # "ARRAY_TYPE": 10,
        # "MAP_TYPE": 11,
        # "STRUCT_TYPE": 12,
        # "UNION_TYPE": 13,
        # "USER_DEFINED_TYPE": 14,
        # "DECIMAL_TYPE": 15,
        # "NULL_TYPE": 16,
        # "DATE_TYPE": 17,
        # "VARCHAR_TYPE": 18,
        # "CHAR_TYPE": 19,
        # "INTERVAL_YEAR_MONTH_TYPE": 20,
        # "INTERVAL_DAY_TIME_TYPE": 21,
        # "TIMESTAMPLOCALTZ_TYPE": 22,
        rtnTColumnDescList = []
        for col, colPos in zip(self.df.schema, range(len(self.df.schema))):
            currTtype = TTypeId.NULL_TYPE
            colType = type(col.dataType)
            if  colType == pyspark.sql.types.DateType:
                currTtype = TTypeId.DATE_TYPE
            elif colType in [ pyspark.sql.types.IntegerType, pyspark.sql.types.ShortType ]:
                currTtype = TTypeId.INT_TYPE
            elif colType in [ pyspark.sql.types.LongType ]:
                currTtype = TTypeId.BIGINT_TYPE
            elif colType in [ pyspark.sql.types.DoubleType ]:
                currTtype = TTypeId.DOUBLE_TYPE
            elif colType in [ pyspark.sql.types.FloatType, pyspark.sql.types.DecimalType ]:
                currTtype = TTypeId.FLOAT_TYPE
            elif colType in [ pyspark.sql.types.TimestampType ]:
                currTtype = TTypeId.TIMESTAMP_TYPE
            elif colType in [ pyspark.sql.types.StringType, pyspark.sql.types.CharType, pyspark.sql.types.VarcharType ]:
                currTtype = TTypeId.STRING_TYPE
            elif colType in [ pyspark.sql.types.MapType ]:
                currTtype = TTypeId.MAP_TYPE
            currTypeEntry = TTypeEntry(primitiveEntry=
            TPrimitiveTypeEntry(
                type=currTtype,))
                # typeQualifiers=
                #     TTypeQualifiers(
                #         qualifiers={"string":TTypeQualifierValue(i32Value=10)})
                
            rtnTColumnDescList.append(TColumnDesc(columnName=col.name,typeDesc=TTypeDesc(types=[currTypeEntry]), position=colPos+1))
        self.schema = TTableSchema(columns=rtnTColumnDescList)
        return self.schema

    def getNextResultRow(self):
        if not self.resultRows:
            self.resultRows = self.df.collect()
        returnRow = []
        try:
            returnRow = self.resultRows[self.fqueryCnt]
            self.fqueryCnt += 1
        except IndexError as e:
             self.fqueryCnt = 0
        return returnRow
        