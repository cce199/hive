#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import glob
import sys, os, re, json
# ThriftHive python
# sys.path.append('gen-py')

# Thrift library
# sys.path.insert(0, glob.glob('../../lib/py/build/lib*')[0])
# sys.path.insert(0, glob.glob('../lib/py/build/lib*')[0])

# from tutorial import Calculator
# from thrift.server import THttpServer #ThriftHiveMetastore
from TCLIService import TCLIService, ttypes
from TCLIService.ttypes import *

# from shared.ttypes import SharedStruct

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from processHandler import dataProcessSparkHandler
import time
from struct import pack
# from sparkdriver import K8sSparkDriver
# from pyspark.sql import SparkSession
from thrift import Thrift
# from sparkUtils import getParseSparkConf
import argparse

def commentParsing(statement):
    """_summary_

    Args:
        statement (_type_): _description_
        /* ... */
        spark_driver_cores
        spark_driver_memory
        memoryOverheadFactor
        spark_executor_cores
        spark_executor_instances
        spark_executor_memory
        spark_dynamicAllocation_enabled
        is_executor_pvc
        executor_local_pvc_size
    """
    try:
        m = re.search(r'( \n)*/\*[\w \'\=\"\n:{}_.,]+\*/',statement)
        sparkConfJson = json.loads(m.group(0)[2:][:-2])
        return sparkConfJson
    except Exception as e:
        return {}

class ThriftProcessHandler:

    def __init__(self):
        self.log = {}
        self.queryCnt = 0
        print("Initialized")
        self.sparkHndler = {}
        self.testCnt = 0
        self.sparkConfJson = None
        # self.sessions = []
        # self.sessionOrd = 0
        self.connDB = False

    def OpenSession(self, req):
        print('------------------------------------------')
        print('SparkThriftHandler-OpenSession')
        # print("SparkThriftHandler-OpenSession - sessionOrd : " + str(self.sessionOrd))
        # print(req) # TOpenSessionReq(client_protocol=7, username=None, password=None, configuration={'use:database': 'default'})
        # return TCLIService.OpenSession_result(status=True)
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS)
        serverProtocolVersion = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6
        guid = str(hex(os.getpid()) ).encode('utf-8')
        # self.sessions.append(guid)
        # self.sessionOrd += 1
        sessionHandle = TSessionHandle(sessionId=THandleIdentifier(guid=guid, secret=b"secret"))
        result = TOpenSessionResp(status=status, serverProtocolVersion=serverProtocolVersion, sessionHandle=sessionHandle)
        # result = TCLIService.OpenSession_result()
        # result.success = TCLIService.OpenSession_result()
        # result.success.success = True
        # print(result)
        return result
    
    def CloseSession(self, req):
        print('------------------------------------------')
        print('SparkThriftHandler-CloseSession')
        print(req.sessionHandle)
        guid = req.sessionHandle.sessionId.guid
        
        if guid in self.sparkHndler.keys():
            print('SparkThriftHandler-CloseSession-call spark close')
            self.sparkHndler[guid].closeConnection()
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS)
        result = TCloseSessionResp(status=status)
        return result

    def GetInfo(self, req):
        print('------------------------------------------')
        print('SparkThriftHandler-GetInfo')
        
    def ExecuteStatement(self, req):
        print("ExecuteStatement")
        # print(req)
        print(req.statement)
        # USE `default`
        chkUseDb = re.match(r"USE \`\w+\`",req.statement)
        chkCurrDb = re.match(r"SELECT current_database()",req.statement)
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                    infoMessages="infoMessages1111",
                    sqlState="RUNNING")
        if not self.sparkConfJson and ( chkUseDb or chkCurrDb ): # and not self.connDB: # 처음conn때 use default
            self.connDB = True
            return TExecuteStatementResp(status=status)
        elif not self.sparkConfJson: # conf 없을때
            self.sparkConfJson = commentParsing(req.statement)
            print(self.sparkConfJson)
        
        # sparkConf = getParseSparkConf(req.statement)
        self.queryCnt = 0 # Test
        guid = req.sessionHandle.sessionId.guid
        print(guid)
        if True:
            if guid not in self.sparkHndler.keys(): # or 추후에 query에 driver option을 바꾸는 명령/hint가 들어오면
                self.sparkHndler[guid] = dataProcessSparkHandler(guid.decode(), test=True, sparkConf = self.sparkConfJson)
            # sparkHndler.getSpark(query="select count(*) from common.dw_eventlogall where base_date = date '2023-03-01'")
            # time.sleep(20)
            if not self.sparkHndler[guid].hasSparkContext():
                self.sparkHndler[guid].createExecutor()
                # time.sleep(20)

            self.sparkHndler[guid].executQuery(query=req.statement)
            # "select base_date, count(*) cnt from common.dw_eventlogall where base_date >= date '2023-06-20' group by 1 order by 1")
        # req -> TExecuteStatementReq(sessionHandle=TSessionHandle(sessionId=THandleIdentifier(guid=b'guid', secret=b'secret')), statement='select 1', confOverlay={}, runAsync=True, queryTimeout=0)
        # req.sessionHandle -> TSessionHandle(sessionId=THandleIdentifier(guid=b'guid', secret=b'secret'))
        # print(req.sessionHandle)
        
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                    infoMessages=self.sparkHndler[guid].errMsg,
                    sqlState="RUNNING")

        # operationId = THandleIdentifier
        self.operationId  = req.sessionHandle.sessionId
        operationHandle = TOperationHandle(
            operationId=self.operationId,
            operationType=TOperationType.EXECUTE_STATEMENT,
            hasResultSet=True,
            modifiedRowCount=2 )
        executeStatementResult = TExecuteStatementResp(status=status,
            operationHandle=operationHandle)
        return executeStatementResult

    def GetTypeInfo(self, req):
        print("GetTypeInfo")

    def GetCatalogs(self, req):
        print('------------------------------------------')
        print("GetCatalogs")

    def GetSchemas(self, req):
        print('------------------------------------------')
        print("GetSchemas")

    def GetTables(self, req):
        print("GetTables")

    def GetTableTypes(self, req):
        print("GetTableTypes")

    def GetColumns(self, req):
        print("GetColumns")

    def GetFunctions(self, req):
        print("GetFunctions")

    def GetPrimaryKeys(self, req):
        print("GetPrimaryKeys")

    def GetCrossReference(self, req):
        print("GetCrossReference")

    def GetOperationStatus(self, req):
        print("GetOperationStatus")
        # req - TGetOperationStatusReq(operationHandle=TOperationHandle(operationId=THandleIdentifier(guid=b'guid', secret=b'secret'), operationType=0, hasResultSet=False, modifiedRowCount=5.0), getProgressUpdate=None)
        # TGetOperationStatusResp
            # - status
            # - operationState
            # - sqlState
            # - errorCode
            # - errorMessage
            # - taskStatus
            # - operationStarted
            # - operationCompleted
            # - hasResultSet
            # - progressUpdateResponse
            # - numModifiedRows
        # TProgressUpdateResp
            # - headerNames
            # - rows
            # - progressedPercentage
            # - status
            # - footerSummary
            # - startTime
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                         infoMessages="infoMessages1111",
                         sqlState="RUNNING")
        operationState = TOperationState.FINISHED_STATE # RUNNING_STATE
        headerNames = [b"idop", b"nameop"]
        rows = [[b'1', b"ace"], [b'12', b"Very"]]
        progressUpdateResponse = TProgressUpdateResp(
            headerNames = headerNames,
            # rows = rows,
            progressedPercentage = 1,
            status = TStatusCode.SUCCESS_STATUS
        )
        result = TGetOperationStatusResp(status=status,
                    operationState = operationState,
                    hasResultSet = True,
                    # progressUpdateResponse = progressUpdateResponse,
                    numModifiedRows = 0)
        # print(result)
        return result

    def CancelOperation(self, req):
        print("CancelOperation")

    def CloseOperation(self, req):
        print("CloseOperation")
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                         infoMessages="infoMessages0000",
                         sqlState="ENDRUNNING")
        result = TCloseOperationResp(status=status)
        return result 

    def GetResultSetMetadata(self, req):
        print("GetResultSetMetadata")
        # print(req)
        guid = req.operationHandle.operationId.guid
        # operationId  = self.operationId # req.sessionHandle.sessionId
        # operationHandle = TOperationHandle(
        #     operationId=self.operationId,
        #     operationType=TOperationType.EXECUTE_STATEMENT,
        #     hasResultSet=True,
        #     modifiedRowCount=2 )
        # result = TGetResultSetMetadataReq(operationHandle=operationHandle)
        # TGetResultSetMetadataResp
        #     - status
        #     - schema
        if guid not in self.sparkHndler.keys(): # or 추후에 query에 driver option을 바꾸는 명령/hint가 들어오면
            # spark not initialized
            # status Error
            status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                            infoMessages="infoMessages33",
                            sqlState="ENDRUNNING")
            typeEntry1 = TTypeEntry(primitiveEntry=
            TPrimitiveTypeEntry(
                type=TTypeId.STRING_TYPE,
                # typeQualifiers=
                #     TTypeQualifiers(
                #         qualifiers={"string":TTypeQualifierValue(i32Value=10)})
            ))
                # arrayEntry=,
                # mapEntry=,
                # structEntry=,
                # unionEntry=,
                # userDefinedTypeEntry=)
            # typeEntry2 = TTypeEntry(primitiveEntry=TPrimitiveTypeEntry(type=TTypeId.BIGINT_TYPE))
            typeEntry2 = TTypeEntry(primitiveEntry=TPrimitiveTypeEntry(type=TTypeId.BIGINT_TYPE))
            typeEntryNull = TTypeEntry(primitiveEntry=TPrimitiveTypeEntry(type=TTypeId.NULL_TYPE))
            col1 = TColumnDesc(columnName="idmeta",typeDesc=TTypeDesc(types=[typeEntry1]), position=1)
            col2 = TColumnDesc(columnName="namesm",typeDesc=TTypeDesc(types=[typeEntry1]), position=2)
            # col1 = TColumnDesc(columnName="idmeta", position=1)
            # col2 = TColumnDesc(columnName="namesm", position=2)
            schema = TTableSchema(columns=[col1,col2])
            result = TGetResultSetMetadataResp(status=status, schema=schema)
            return result

        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                            infoMessages="infoMessages33",
                            sqlState="ENDRUNNING")
        resultSchema = self.sparkHndler[guid].getResultSchema()
        result = TGetResultSetMetadataResp(status=status, schema=resultSchema)
        # print("================ GetResultSetMetadata_result ===============")
        # print(result)
        # GetResultSetMetadata_result
        return result

    def FetchResults(self, req):
        # print("FetchResults" + str(self.queryCnt))
        # print(req)
        # TFetchResultsResp
        # - status
        # - hasMoreRows
        # - results
        # rows = [TRow([])]
        rows = []
        guid = req.operationHandle.operationId.guid
        if self.sparkHndler[guid].colTColumnType == []:
            self.GetResultSetMetadata(req)

        if guid not in self.sparkHndler.keys(): # or 추후에 query에 driver option을 바꾸는 명령/hint가 들어오면
            status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                         infoMessages="infoMessages44",
                         sqlState="ENDRUNNING")
            # rows = [TRow([TColumnValue(stringVal=TStringValue(value="1")),TColumnValue(stringVal=TStringValue(value="ace"))])
            #             ,TRow([TColumnValue(stringVal=TStringValue(value="12")),TColumnValue(stringVal=TStringValue(value="Vert"))])
            #             ,TRow([TColumnValue(stringVal=TStringValue(value="12")),TColumnValue(stringVal=TStringValue(value="Vert"))])
            #             ,TRow([TColumnValue(stringVal=TStringValue(value="12")),TColumnValue(stringVal=TStringValue(value="Vert"))])
            #             ]
            if self.queryCnt > 2:
                columns = []
            else:
                columns = [TColumn(stringVal=TStringColumn(values=[b"2023-03-01"],nulls=b""))
                        # ,TColumn(i32Val=TI32Column(values=[16121901, 16121], nulls=b'[NULL]') ,
                        ,TColumn(i32Val=TI32Column(values=[self.testCnt], nulls=b'') ,
                                #  binaryVal=TBinaryColumn(values=[b'', b''], nulls=b'')
                                 ) # b'\x00'
                        ]
                self.testCnt += 1
                # columns = [TColumn(stringVal=TStringColumn(values=[b"2023-03-01"],nulls=b""))
                #         ,TColumn(stringVal=TStringColumn(values=[b"asdfa"],nulls=b""))]
            results = TRowSet(
                startRowOffset=0,
                rows=rows,
                # columns=columns,
                columnCount=1
            )
            # if columns == []:
            #     result = TFetchResultsResp(status=status,hasMoreRows=False, results=results)
            # else:
            result = TFetchResultsResp(status=status,hasMoreRows=False, results=results)
            # print(result)
            self.queryCnt += 1
            return result

        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                         infoMessages="infoMessages44",
                         sqlState="ENDRUNNING")
        columns = self.sparkHndler[guid].getNextResultRow()
        
        results = TRowSet(
            startRowOffset=0,
            rows=rows,
            columns=columns,
            columnCount=1
        )
        result = TFetchResultsResp(status=status,hasMoreRows=False, results=results)
        # print(result)
        return result
        
    def GetDelegationToken(self, req):
        print("GetDelegationToken")

    def CancelDelegationToken(self, req):
        print("CancelDelegationToken")

    def RenewDelegationToken(self, req):
        print("RenewDelegationToken")

    def GetQueryId(self, req):
        print("GetQueryId")

    def SetClientInfo(self, req):
        print("SetClientInfo")

    def UploadData(self, req):
        print("UploadData")

    def DownloadData(self, req):
        print("DownloadData")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='PythonHiveServer.py')
    parser.add_argument('--session-timeout', default=1800000, #action='store_const',
        help='session timeout(ms)')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    # parse_args = parser.parse_args(["--session-timeout"])
    parse_args = parser.parse_args()
    print(parse_args)
    session_timeout = int(parse_args.session_timeout)
    # if parser.parse_args(["--help"]):
    #     print(parser.print_help())
    #     exit()

    handler = ThriftProcessHandler()
    processor = TCLIService.Processor(handler)
    transport = TSocket.TServerSocket(host='0.0.0.0', port=9091, session_timeout=session_timeout)
    # tfactory = TTransport.TBufferedTransportFactory()
    tfactory = TTransport.TSaslClientTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    # server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server = TServer.TForkingServer(processor, transport, tfactory, pfactory)

    # TForkingServer
    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
