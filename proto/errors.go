// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pkg

import (
	"errors"

	"github.com/vearch/vearch/proto/vearchpb"
)

// http response error code and error message definitions
const ERRCODE_SUCCESS = 200

const (
	ERRCODE_INTERNAL_ERROR = 550 + iota
	ERRCODE_NAME_OR_PASSWORD
	ERRCODE_SYSBUSY
	ERRCODE_PARAM_ERROR
	ERRCODE_INVALID_CFG
	ERRCODE_TIMEOUT
	ERRCODE_SERVICE_UNAVAILABLE
	ERRCODE_ZONE_NOT_EXISTS
	ERRCODE_LOCAL_ZONE_OPS_FAILED
	ERRCODE_DUP_ZONE
	ERRCODE_DUP_DB
	ERRCODE_INVALID_ENGINE
	ERRCODE_DB_NOTEXISTS
	ERRCODE_DB_Not_Empty
	ERRCODE_DUP_SPACE
	ERRCODE_SPACE_NOTEXISTS
	ERRCODE_PARTITION_HAS_TASK_NOW
	ERRCODE_REPLICA_NOT_EXISTS
	ERRCODE_DUP_REPLICA
	ERRCODE_PARTITION_REPLICA_LEADER_NOT_DELETE
	ERRCODE_PS_NOTEXISTS
	ERRCODE_PS_Already_Exists
	ERRCODE_LOCAL_SPACE_OPS_FAILED
	ERRCODE_Local_PS_Ops_Failed
	ERRCODE_GENID_FAILED
	ERRCODE_LOCALDB_OPTFAILED
	ERRCODE_SPACE_SCHEMA_INVALID
	ERRCODE_RPC_GET_CLIENT_FAILED
	ERRCODE_RPC_INVALID_RESP
	ERRCODE_RPC_INVOKE_FAILED
	ERRCODE_RPC_PARAM_ERROR
	ERRCODE_METHOD_NOT_IMPLEMENT
	ERRCODE_USER_NOT_EXISTS
	ERRCODE_DUP_USER
	ERRCODE_USER_OPS_FAILED
	ERRCODE_AUTHENTICATION_FAILED
	ERRCODE_REGION_NOT_EXISTS
	ERRCODE_MASTER_PS_CAN_NOT_SELECT
	ERRCODE_MASTER_PS_NOT_ENOUGH_SELECT
	ERRCODE_PARTITION_DUPLICATE
	ERRCODE_PARTITION_NOT_EXIST
	ERRCODE_PARTITION_NOT_LEADER
	ERRCODE_PARTITION_NO_LEADER
	ERRCODE_PARTITION_REQ_PARAM
	ERRCODE_PARTITON_ENGINENAME_INVALID
	ERRCODE_UNKNOWN_PARTITION_RAFT_CMD_TYPE
	ERRCODE_MASTER_SERVER_IS_NOT_RUNNING
	ERRCODE_PARTITION_IS_INVALID
	ERRCODE_PARTITION_IS_CLOSED
	ERRCODE_DOCUMENT_NOT_EXIST
	ERRCODE_DOCUMENT_EXIST
	ERRCODE_PULL_OUT_VERSION_NOT_MATCH
	ERRCODE_FUNC_CAN_NOT_INVOKE_IN_FROZEN_ENGINE
)

const SUCCESS = "success"

// General server err
var (
	errGeneralSuccess            = errors.New(SUCCESS)
	errGeneralMethodNotImplement = errors.New("method not implement")
	errGeneralServiceUnavailable = errors.New("Service Unavailable")
	errGeneralInternalError      = errors.New("internal error")
	errGeneralTimeoutError       = errors.New("timeout error")
	errGeneralSysBusy            = errors.New("system busy")
	errGeneralParamError         = errors.New("param error")
	errGeneralInvalidCfg         = errors.New("config error")
	errGeneralNameOrPassword     = errors.New("username or password err")
)

// Partition err
var (
	errPartitionDuplicate             = errors.New("Partition Already Exists")
	errPartitionNotExist              = errors.New("Partition Not Exists")
	errPartitionClosed                = errors.New("Partition is Closed")
	errPartitionInvalid               = errors.New("Partition is Invalid")
	errPartitionNotLeader             = errors.New("Partition Not Leader")
	errPartitionReqParam              = errors.New("Request Param Error")
	errUnknownPartitionRaftCmdType    = errors.New("Unknown partition raft cmd type")
	errPartitionEngineNameInvalid     = errors.New("Registration engine name is invalid")
	errPartitionFieldNotFound         = errors.New("field not found")
	errRecordJournal                  = errors.New("Record journal failed")
	errFuncCanNotInvokeInFrozenEngine = errors.New("Func can not invoke in frozen engine")
)

var (
	errDocumentNotExist                 = errors.New("Document Not Exists")
	errDocDelVersionNotSpecified        = errors.New("document delete version not specified")
	errDocReplaceVersionNotSpecified    = errors.New("document replace version not specified")
	errDocumentMergeVersionNotSpecified = errors.New("document merge version not specified")
	errDocPulloutVersionNotMatch        = errors.New("document pullout version not match")
	errDocumentExist                    = errors.New("Document Exists")
)
var (
	errMasterAuthenticationFailed            = errors.New("master authentication failed")
	errMasterRegionNotExists                 = errors.New("region not exists")
	errMasterZoneNotExists                   = errors.New("zone not exists")
	errMasterUserNotExists                   = errors.New("user not exists")
	errMasterLocalZoneOpsFailed              = errors.New("local storage zone operation error")
	errMasterLocalUserOpsFailed              = errors.New("local storage user operation error")
	errMasterDupZone                         = errors.New("duplicated zone")
	errMasterDupUser                         = errors.New("duplicated user")
	errMasterDupDb                           = errors.New("duplicated database")
	errMasterInvalidEngine                   = errors.New("invalid engine")
	errMasterDbNotExists                     = errors.New("db not exists")
	errMasterDbNotEmpty                      = errors.New("db not empty")
	errMasterDupSpace                        = errors.New("duplicated space")
	errMasterSpaceNotExists                  = errors.New("space not exists")
	errMasterUseerNotExists                  = errors.New("user not exists")
	errMasterPartitionHasTaskNow             = errors.New("partition has task now")
	errMasterReplicaNotExists                = errors.New("replica not exists")
	errMasterDupReplica                      = errors.New("duplicated Replica")
	errMasterPartitionReplicaLeaderNotDelete = errors.New("partition replica leader can not delete")

	errMasterPSNotExists         = errors.New("partition server is not exists")
	errMasterPSCanNotSelect      = errors.New("can not select PS")
	errMasterPSNotEnoughSelect   = errors.New("not enough PS")
	errMasterPSAlreadyExists     = errors.New("partition server is already exists")
	errMasterLocalSpaceOpsFailed = errors.New("local storage space operation error")
	errMasterLocalPSOpsFailed    = errors.New("local storage ps operation error")
	errMasterGenIdFailed         = errors.New("generate id is failed")
	errMasterLocalDbOpsFailed    = errors.New("local storage db operation error")
	errMasterSpaceSchemaInvalid  = errors.New("space schema invalid")

	errMasterRpcGetClientFailed = errors.New("get rpc client handle is failed")
	errMasterRpcInvalidResp     = errors.New("invalid rpc response")
	errMasterRpcInvokeFailed    = errors.New("invoke rpc is failed")
	errMasterRpcParamErr        = errors.New("rpc param error")
	errMasterServerIsNotRunning = errors.New("master server is not running")
)

// get err code by error if error is nil , return ERRCODE_SUCCESS
func ErrCode(err error) int64 {
	if err == nil {
		return int64(vearchpb.ErrorEnum_SUCCESS)
	}

	if ve, ok := err.(*vearchpb.VearchErr); ok {
		return int64(ve.GetError().Code)
	}

	return int64(vearchpb.ErrorEnum_INTERNAL_ERROR)
}

// it will return format err, is not registe it will return general err string
func FormatErr(err error) string {
	if err == nil {
		return ""
	}

	if code := ErrCode(err); code == ERRCODE_INTERNAL_ERROR {
		return errGeneralInternalError.Error()
	}

	return err.Error()
}

func CodeErr(code int64) *vearchpb.VearchErr {
	return vearchpb.NewError(vearchpb.ErrorEnum(code), nil)
}

func NewCodeErr(code int64, msg string) *vearchpb.VearchErr {
	return vearchpb.NewErrorInfo(vearchpb.ErrorEnum(code), msg)
}

var code2ErrMap = map[int64]error{
	ERRCODE_SUCCESS:                              errGeneralSuccess,
	ERRCODE_INTERNAL_ERROR:                       errGeneralInternalError,
	ERRCODE_SERVICE_UNAVAILABLE:                  errGeneralServiceUnavailable,
	ERRCODE_TIMEOUT:                              errGeneralTimeoutError,
	ERRCODE_SYSBUSY:                              errGeneralSysBusy,
	ERRCODE_PARAM_ERROR:                          errGeneralParamError,
	ERRCODE_INVALID_CFG:                          errGeneralInvalidCfg,
	ERRCODE_ZONE_NOT_EXISTS:                      errMasterZoneNotExists,
	ERRCODE_LOCAL_ZONE_OPS_FAILED:                errMasterLocalZoneOpsFailed,
	ERRCODE_DUP_ZONE:                             errMasterDupZone,
	ERRCODE_DUP_DB:                               errMasterDupDb,
	ERRCODE_INVALID_ENGINE:                       errMasterInvalidEngine,
	ERRCODE_DB_NOTEXISTS:                         errMasterDbNotExists,
	ERRCODE_DB_Not_Empty:                         errMasterDbNotEmpty,
	ERRCODE_DUP_SPACE:                            errMasterDupSpace,
	ERRCODE_SPACE_NOTEXISTS:                      errMasterSpaceNotExists,
	ERRCODE_PARTITION_HAS_TASK_NOW:               errMasterPartitionHasTaskNow,
	ERRCODE_REPLICA_NOT_EXISTS:                   errMasterReplicaNotExists,
	ERRCODE_DUP_REPLICA:                          errMasterDupReplica,
	ERRCODE_PARTITION_REPLICA_LEADER_NOT_DELETE:  errMasterPartitionReplicaLeaderNotDelete,
	ERRCODE_PS_NOTEXISTS:                         errMasterPSNotExists,
	ERRCODE_PS_Already_Exists:                    errMasterPSAlreadyExists,
	ERRCODE_LOCAL_SPACE_OPS_FAILED:               errMasterLocalSpaceOpsFailed,
	ERRCODE_Local_PS_Ops_Failed:                  errMasterLocalPSOpsFailed,
	ERRCODE_GENID_FAILED:                         errMasterGenIdFailed,
	ERRCODE_LOCALDB_OPTFAILED:                    errMasterLocalDbOpsFailed,
	ERRCODE_SPACE_SCHEMA_INVALID:                 errMasterSpaceSchemaInvalid,
	ERRCODE_RPC_GET_CLIENT_FAILED:                errMasterRpcGetClientFailed,
	ERRCODE_RPC_INVALID_RESP:                     errMasterRpcInvalidResp,
	ERRCODE_RPC_INVOKE_FAILED:                    errMasterRpcInvokeFailed,
	ERRCODE_RPC_PARAM_ERROR:                      errMasterRpcParamErr,
	ERRCODE_METHOD_NOT_IMPLEMENT:                 errGeneralMethodNotImplement,
	ERRCODE_USER_NOT_EXISTS:                      errMasterUserNotExists,
	ERRCODE_DUP_USER:                             errMasterDupUser,
	ERRCODE_USER_OPS_FAILED:                      errMasterLocalUserOpsFailed,
	ERRCODE_AUTHENTICATION_FAILED:                errMasterAuthenticationFailed,
	ERRCODE_REGION_NOT_EXISTS:                    errMasterRegionNotExists,
	ERRCODE_MASTER_PS_CAN_NOT_SELECT:             errMasterPSCanNotSelect,
	ERRCODE_MASTER_PS_NOT_ENOUGH_SELECT:          errMasterPSNotEnoughSelect,
	ERRCODE_PARTITION_DUPLICATE:                  errPartitionDuplicate,
	ERRCODE_PARTITION_NOT_EXIST:                  errPartitionNotExist,
	ERRCODE_PARTITION_NOT_LEADER:                 errPartitionNotLeader,
	ERRCODE_PARTITION_REQ_PARAM:                  errPartitionReqParam,
	ERRCODE_PARTITON_ENGINENAME_INVALID:          errPartitionEngineNameInvalid,
	ERRCODE_UNKNOWN_PARTITION_RAFT_CMD_TYPE:      errUnknownPartitionRaftCmdType,
	ERRCODE_MASTER_SERVER_IS_NOT_RUNNING:         errMasterServerIsNotRunning,
	ERRCODE_PARTITION_IS_CLOSED:                  errPartitionClosed,
	ERRCODE_PARTITION_IS_INVALID:                 errPartitionInvalid,
	ERRCODE_DOCUMENT_NOT_EXIST:                   errDocumentNotExist,
	ERRCODE_DOCUMENT_EXIST:                       errDocumentExist,
	ERRCODE_PULL_OUT_VERSION_NOT_MATCH:           errDocPulloutVersionNotMatch,
	ERRCODE_FUNC_CAN_NOT_INVOKE_IN_FROZEN_ENGINE: errFuncCanNotInvokeInFrozenEngine,
	ERRCODE_NAME_OR_PASSWORD:                     errGeneralNameOrPassword,
}

var err2CodeMap map[string]int64

func init() {
	err2CodeMap = make(map[string]int64)
	for code, err := range code2ErrMap {
		err2CodeMap[err.Error()] = code
	}
}
