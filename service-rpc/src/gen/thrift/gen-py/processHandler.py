import time
from TCLIService.ttypes import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pyspark
# from pyspark_gateway import PysparkGateway

class dataProcessHandler:
    def __init__(self):
        pass

class dataProcessSparkHandler():
    def __init__(self, guid, test=False, cores="4", memory="20Gi", mount_path="/root"):
        self.test = test
        if not self.test:
            from sparkdriver import K8sSparkDriver
            self.driver = K8sSparkDriver(guid, cpu=cores, memory=memory, mount_path=mount_path, remote=True)
        self.spark = None
        self.df = None
        self.sparkContext = None
        self.schema = None
        self.resultRows = None
        self.colTColumnType = []
        self.currentRowOrd = 0
        self.testCnt = 0

    def hasSparkContext(self):
        if self.sparkContext:
            return True
        return False
    # def getDriver(self):
    #     return self.driver
    
    def createExecutor(self, instances="2", memory="15g", cores="5"):
        self.sparkContext = None
        if self.test:
            conf = SparkConf()
            conf.setMaster("local").setAppName("ThriftSparkTest") #.set("spark.driver.allowMultipleContexts", "true")
            self.sparkContext  = SparkContext(conf=conf)
            # self.spark = SparkSession.builder\
            #     .master("local[1]")\
            #     .appName("sample() and sampleBy() PySpark")\
            #     .getOrCreate()
        else:
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
                "spark.kubernetes.node.selector.alpha.eksctl.io/nodegroup-name": "ng-memory-5g-spark",
                "spark.kubernetes.executor.podTemplateFile":"s3a://zigbang-data/conf/executor.yaml",
            }
            self.sparkContext = self.driver.getSparkContext(config)
        self.spark = SparkSession(self.sparkContext)
        # print(PysparkGateway.host)
    
    def executQuery(self, query):
        if self.test:
            self.df = self.spark.createDataFrame([["Alex", self.testCnt + 1],\
                            ["Bob", self.testCnt + 2],\
                            ["Cathy", self.testCnt + 3],\
                            ["Doge", self.testCnt + 4]],\
                            ["name", "age"])
            self.testCnt += 5
        else:
            self.df = self.spark.sql(query)
        # self.resultRows = None
        # print(result)
        self.currentRowOrd = 0
        self.colTColumnType = []
        self.resultRows = self.df.collect()
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
            if colType == pyspark.sql.types.BooleanType:
                currTtype = TTypeId.BOOLEAN_TYPE
                self.colTColumnType.append("bool")
            elif colType == pyspark.sql.types.BinaryType:
                currTtype = TTypeId.BINARY_TYPE
                self.colTColumnType.append("binary")
            elif colType == pyspark.sql.types.ByteType:
                currTtype = TTypeId.BINARY_TYPE
                self.colTColumnType.append("byte")
            elif colType == pyspark.sql.types.DateType:
                currTtype = TTypeId.STRING_TYPE
                self.colTColumnType.append("string")
            elif colType in [ pyspark.sql.types.ShortType ]:
                currTtype = TTypeId.SMALLINT_TYPE
                self.colTColumnType.append("i16")
            elif colType in [ pyspark.sql.types.IntegerType ]:
                currTtype = TTypeId.INT_TYPE
                self.colTColumnType.append("i32")
            elif colType in [ pyspark.sql.types.LongType ]:
                currTtype = TTypeId.BIGINT_TYPE
                self.colTColumnType.append("i64")
            elif colType in [ pyspark.sql.types.DoubleType ]:
                currTtype = TTypeId.DOUBLE_TYPE
                self.colTColumnType.append("double")
            elif colType in [ pyspark.sql.types.FloatType, pyspark.sql.types.DecimalType ]:
                currTtype = TTypeId.FLOAT_TYPE
                self.colTColumnType.append("double")
            elif colType in [ pyspark.sql.types.TimestampType ]:
                currTtype = TTypeId.TIMESTAMP_TYPE
                self.colTColumnType.append("string")
            elif colType in [ pyspark.sql.types.StringType]: #, pyspark.sql.types.CharType, pyspark.sql.types.VarcharType ]:
                currTtype = TTypeId.STRING_TYPE
                self.colTColumnType.append("string")
            elif colType in [ pyspark.sql.types.MapType ]:
                currTtype = TTypeId.MAP_TYPE
                self.colTColumnType.append("string")
            else:
                currTtype = TTypeId.STRING_TYPE
                self.colTColumnType.append("string")
            currTypeEntry = TTypeEntry(primitiveEntry=
            TPrimitiveTypeEntry(
                type=currTtype,))
                # typeQualifiers=
                #     TTypeQualifiers(
                #         qualifiers={"string":TTypeQualifierValue(i32Value=10)})
                
            rtnTColumnDescList.append(TColumnDesc(columnName=col.name,typeDesc=TTypeDesc(types=[currTypeEntry]), position=colPos+1))
        self.schema = TTableSchema(columns=rtnTColumnDescList)
        # print(self.colTColumnType)
        return self.schema

    def closeConnection(self):
        print("stop spark/driver")
        self.spark.stop()
        self.driver.stop()        

    def getNextResultRow(self):            
        returnRow = []
        try:
            nextRow = self.resultRows[self.currentRowOrd]
        except IndexError as e:
            return returnRow
        self.currentRowOrd += 1
        # self.colTColumnType
        # rtnCols = []
        # for colType in self.colTColumnType:
        #     rtnCols.append([])
        # for row in self.resultRows:
            # for (colType, ord) in zip(self.colTColumnType, range(len(self.colTColumnType))):
        # for (colType, ord, colVal) in zip(self.colTColumnType, range(len(self.colTColumnType)), nextRow ):
        #     if colVal == None:
        #         pass # values is []
        #     elif colType == 'string':
        #         rtnCols.append( bytes(str(colVal), 'utf-8'))
        #     else:
        #         rtnCols.append(colVal)
        for (colType, colVal) in zip(self.colTColumnType, nextRow):
            nullVal = b'[NULL]' if colVal == None else b''
            if colVal == None:
                colVal = [] # values is []
            elif colType == 'string':
                colVal = [bytes(str(colVal), 'utf-8')]
            else:
                colVal = [colVal]

            currCol = TColumn(
                boolVal = TBoolColumn(values=colVal, nulls=nullVal) if colType == 'bool' else None,
                byteVal = TByteColumn(values=colVal, nulls=nullVal) if colType == 'binary' else None,
                i16Val = TI16Column(values=colVal, nulls=nullVal) if colType == 'i16' else None,
                i32Val = TI32Column(values=colVal, nulls=nullVal) if colType == 'i32' else None,
                i64Val = TI64Column(values=colVal, nulls=nullVal) if colType == 'i64' else None,
                doubleVal = TDoubleColumn(values=colVal, nulls=nullVal) if colType == 'double' else None,
                stringVal = TStringColumn(values=colVal, nulls=nullVal) if colType == 'string' else None,
                binaryVal = TBinaryColumn(values=colVal, nulls=nullVal) if colType == 'byte' else None,
            )
            returnRow.append(currCol)
            # [TColumn(stringVal=TStringColumn(values=[b"ido"],nulls=b""))
            #             ,TColumn(stringVal=TStringColumn(values=[b"namesx"],nulls=b""))]
            # except IndexError as e:
        
        # print("============= returnRow ===============")
        # print(returnRow)
        return returnRow
