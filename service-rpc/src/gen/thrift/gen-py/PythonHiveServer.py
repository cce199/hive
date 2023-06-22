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
import sys
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


class SparkThriftHandler:

    def __init__(self):
        self.log = {}
        self.queryCnt = 0
        print("Initialized")

    def OpenSession(self, req):
        print('------------------------------------------')
        print('SparkThriftHandler-OpenSession')
        # print(req) TOpenSessionReq(client_protocol=7, username=None, password=None, configuration={'use:database': 'default'})
        # return TCLIService.OpenSession_result(status=True)
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS)
        serverProtocolVersion = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
        sessionHandle = TSessionHandle(sessionId=THandleIdentifier(guid=b"guid", secret=b"secret"))
        result = TOpenSessionResp(status=status, serverProtocolVersion=serverProtocolVersion, sessionHandle=sessionHandle)
        # result = TCLIService.OpenSession_result()
        # result.success = TCLIService.OpenSession_result()
        # result.success.success = True
        return result
    
    def CloseSession(self, req):
        print('------------------------------------------')
        print('SparkThriftHandler-CloseSession')
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS)
        result = TCloseSessionResp(status=status)
        return result
    
    def GetInfo(self, req):
        print('------------------------------------------')
        print('SparkThriftHandler-GetInfo')

    def ExecuteStatement(self, req):
        print("ExecuteStatement")
        print(req.statement)
        # req -> TExecuteStatementReq(sessionHandle=TSessionHandle(sessionId=THandleIdentifier(guid=b'guid', secret=b'secret')), statement='select 1', confOverlay={}, runAsync=True, queryTimeout=0)
        # req.sessionHandle -> TSessionHandle(sessionId=THandleIdentifier(guid=b'guid', secret=b'secret'))
        # print(req.sessionHandle)
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                         infoMessages="infoMessages1111",
                         sqlState="RUNNING")
        # operationId = THandleIdentifier
        self.operationId  = req.sessionHandle.sessionId
        operationHandle = TOperationHandle(
            operationId=self.operationId,
            operationType=TOperationType.EXECUTE_STATEMENT,
            hasResultSet=True,
            modifiedRowCount=2 )
        result = TExecuteStatementResp(status=status,
            operationHandle=operationHandle)
        return result

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
        status = TStatus(statusCode=TStatusCode.SUCCESS_WITH_INFO_STATUS,
                         infoMessages="infoMessages1111",
                         sqlState="RUNNING")
        operationState = TOperationState.FINISHED_STATE # RUNNING_STATE
        headerNames = [b"id", b"names"]
        rows = [[b'1', b"ace"], [b'12', b"Very"]]
        progressUpdateResponse = TProgressUpdateResp(
            headerNames = headerNames,
            # rows = rows,
            progressedPercentage = 1,
            status = TStatusCode.SUCCESS_STATUS
        )
        result = TGetOperationStatusResp(status=status,
                    operationState = operationState,
                    hasResultSet = False,
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
        print(req)
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
        typeEntry2 = TTypeEntry(primitiveEntry=TPrimitiveTypeEntry(type=TTypeId.STRING_TYPE))
        col1 = TColumnDesc(columnName="idmeta",typeDesc=TTypeDesc(types=[typeEntry1]), position=1)
        col2 = TColumnDesc(columnName="namesm",typeDesc=TTypeDesc(types=[typeEntry2]), position=2)
        # col1 = TColumnDesc(columnName="idmeta", position=1)
        # col2 = TColumnDesc(columnName="namesm", position=2)
        schema = TTableSchema(columns=[col1,col2])
        result = TGetResultSetMetadataResp(status=status, schema=schema)
        # GetResultSetMetadata_result
        return result

    def FetchResults(self, req):
        print("FetchResults" + str(self.queryCnt))
        # TFetchResultsResp
        # - status
        # - hasMoreRows
        # - results
        status = TStatus(statusCode=TStatusCode.SUCCESS_STATUS,
                         infoMessages="infoMessages44",
                         sqlState="ENDRUNNING")
        rows = [TRow([TColumnValue(stringVal=TStringValue(value="1")),TColumnValue(stringVal=TStringValue(value="ace"))])
                 ,TRow([TColumnValue(stringVal=TStringValue(value="12")),TColumnValue(stringVal=TStringValue(value="Vert"))])
                 ,TRow([TColumnValue(stringVal=TStringValue(value="12")),TColumnValue(stringVal=TStringValue(value="Vert"))])
                 ,TRow([TColumnValue(stringVal=TStringValue(value="12")),TColumnValue(stringVal=TStringValue(value="Vert"))])
                 ]
        columns = [TColumn(stringVal=TStringColumn(values=[b"ido"],nulls=b""))
                    ,TColumn(stringVal=TStringColumn(values=[b"namesx"],nulls=b""))]
        results = TRowSet(
            startRowOffset=0,
            rows=rows,
            # [[b'1', b"ace"], [b'12', b"Very"]],
            columns=columns,
            # [b"id", b"names"],
            # binaryColumns=b'',
            columnCount=10
        )
        if self.queryCnt > 2:
            results = TRowSet(
                startRowOffset=0,
                rows=rows,
                # [[b'1', b"ace"], [b'12', b"Very"]],
                columns=[],
                # [b"id", b"names"],
                # binaryColumns=b'',
                columnCount=2
                )
            result = TFetchResultsResp(status=status,hasMoreRows=False, results=results)
        else:
            result = TFetchResultsResp(status=status,hasMoreRows=False, results=results)
        self.queryCnt += 1
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
    handler = SparkThriftHandler()
    processor = TCLIService.Processor(handler)
    transport = TSocket.TServerSocket(host='127.0.0.1', port=9091)
    # tfactory = TTransport.TBufferedTransportFactory()
    tfactory = TTransport.TSaslClientTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    # server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
