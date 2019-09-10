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
	"fmt"
)

// http response error code and error message definitions
const ERRCODE_SUCCESS = 200

const (
	ERRCODE_INTERNAL_ERROR = 550 + iota
	ERRCODE_SYSBUSY
	ERRCODE_PARAM_ERROR
	ERRCODE_INVALID_CFG
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
	ERRCODE_PARTITION_FROZEN
	ERRCODE_UNKNOWN_PARTITION_RAFT_CMD_TYPE
	ERRCODE_MASTER_SERVER_IS_NOT_RUNNING
	ERRCODE_PARTITION_IS_INVALID
	ERRCODE_PARTITION_IS_CLOSED
	ERRCODE_DOCUMENT_NOT_EXIST
	ERRCODE_DOCUMENT_EXIST
	ERRCODE_DOCUMENT_MUST_HAS_SOURCE
	ERRCODE_PULL_OUT_VERSION_NOT_MATCH
	ERRCODE_FUNC_CAN_NOT_INVOKE_IN_FROZEN_ENGINE
)

//General server err
var (
	ErrGeneralSuccess            = errors.New("success")
	ErrGeneralMethodNotImplement = errors.New("method not implement")
	ErrGeneralServiceUnavailable = errors.New("Service Unavailable")
	ErrGeneralInternalError      = errors.New("internal error")
	ErrGeneralTimeoutError       = errors.New("timeout error")
	ErrGeneralSysBusy            = errors.New("system busy")
	ErrGeneralParamError         = errors.New("param error")
	ErrGeneralInvalidCfg         = errors.New("config error")
)

//Partition err
var (
	ErrPartitionDuplicate             = errors.New("Partition Already Exists")
	ErrPartitionNotExist              = errors.New("Partition Not Exists")
	ErrPartitionClosed                = errors.New("Partition is Closed")
	ErrPartitionInvalid               = errors.New("Partition is Invalid")
	ErrPartitionNotLeader             = errors.New("Partition Not Leader")
	ErrPartitionReqParam              = errors.New("Request Param Error")
	ErrPartitionFrozen                = errors.New("Partition Frozen Error")
	ErrUnknownPartitionRaftCmdType    = errors.New("Unknown partition raft cmd type")
	ErrPartitionEngineNameInvalid     = errors.New("Registration engine name is invalid")
	ErrPartitionFieldNotFound         = errors.New("field not found")
	ErrRecordJournal                  = errors.New("Record journal failed")
	ErrFuncCanNotInvokeInFrozenEngine = errors.New("Func can not invoke in frozen engine")
)

var (
	ErrDocumentNotExist                 = errors.New("Document Not Exists")
	ErrDocDelVersionNotSpecified        = errors.New("document delete version not specified")
	ErrDocReplaceVersionNotSpecified    = errors.New("document replace version not specified")
	ErrDocumentMergeVersionNotSpecified = errors.New("document merge version not specified")
	ErrDocPulloutVersionNotMatch        = errors.New("document pullout version not match")
	ErrDocumentExist                    = errors.New("Document Exists")
	ErrDocumentMustHasSource            = errors.New("Document Must has source")
)
var (
	ErrMasterAuthenticationFailed            = errors.New("master authentication failed")
	ErrMasterRegionNotExists                 = errors.New("region not exists")
	ErrMasterZoneNotExists                   = errors.New("zone not exists")
	ErrMasterUserNotExists                   = errors.New("user not exists")
	ErrMasterLocalZoneOpsFailed              = errors.New("local storage zone operation error")
	ErrMasterLocalUserOpsFailed              = errors.New("local storage user operation error")
	ErrMasterDupZone                         = errors.New("duplicated zone")
	ErrMasterDupUser                         = errors.New("duplicated user")
	ErrMasterDupDb                           = errors.New("duplicated database")
	ErrMasterInvalidEngine                   = errors.New("invalid engine")
	ErrMasterDbNotExists                     = errors.New("db not exists")
	ErrMasterDbNotEmpty                      = errors.New("db not empty")
	ErrMasterDupSpace                        = errors.New("duplicated space")
	ErrMasterSpaceNotExists                  = errors.New("space not exists")
	ErrMasterUseerNotExists                  = errors.New("user not exists")
	ErrMasterPartitionHasTaskNow             = errors.New("partition has task now")
	ErrMasterReplicaNotExists                = errors.New("replica not exists")
	ErrMasterDupReplica                      = errors.New("duplicated Replica")
	ErrMasterPartitionReplicaLeaderNotDelete = errors.New("partition replica leader can not delete")

	ErrMasterPSNotExists         = errors.New("partition server is not exists")
	ErrMasterPSCanNotSelect      = errors.New("can not select PS")
	ErrMasterPSNotEnoughSelect   = errors.New("not enough PS")
	ErrMasterPSAlreadyExists     = errors.New("partition server is already exists")
	ErrMasterLocalSpaceOpsFailed = errors.New("local storage space operation error")
	ErrMasterLocalPSOpsFailed    = errors.New("local storage ps operation error")
	ErrMasterGenIdFailed         = errors.New("generate id is failed")
	ErrMasterLocalDbOpsFailed    = errors.New("local storage db operation error")
	ErrMasterSpaceSchemaInvalid  = errors.New("space schema invalid")

	ErrMasterRpcGetClientFailed = errors.New("get rpc client handle is failed")
	ErrMasterRpcInvalidResp     = errors.New("invalid rpc response")
	ErrMasterRpcInvokeFailed    = errors.New("invoke rpc is failed")
	ErrMasterRpcParamErr        = errors.New("rpc param error")
	ErrMasterServerIsNotRunning = errors.New("master server is not running")
)

//get err code by error if error is nil , return ERRCODE_SUCCESS
func ErrCode(err error) int {
	if err == nil {
		return ERRCODE_SUCCESS
	}

	if code, ok := err2CodeMap[err.Error()]; ok {
		return code
	}
	return ERRCODE_INTERNAL_ERROR
}

func CodeErr(code int) error {
	for e, c := range err2CodeMap {
		if c == code {
			return fmt.Errorf(e)
		}
	}
	return ErrGeneralInternalError
}

func ErrError(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

var err2CodeMap = map[string]int{
	ErrGeneralSuccess.Error():                        ERRCODE_SUCCESS,
	ErrGeneralInternalError.Error():                  ERRCODE_INTERNAL_ERROR,
	ErrGeneralSysBusy.Error():                        ERRCODE_SYSBUSY,
	ErrGeneralParamError.Error():                     ERRCODE_PARAM_ERROR,
	ErrGeneralInvalidCfg.Error():                     ERRCODE_INVALID_CFG,
	ErrMasterZoneNotExists.Error():                   ERRCODE_ZONE_NOT_EXISTS,
	ErrMasterLocalZoneOpsFailed.Error():              ERRCODE_LOCAL_ZONE_OPS_FAILED,
	ErrMasterDupZone.Error():                         ERRCODE_DUP_ZONE,
	ErrMasterDupDb.Error():                           ERRCODE_DUP_DB,
	ErrMasterInvalidEngine.Error():                   ERRCODE_INVALID_ENGINE,
	ErrMasterDbNotExists.Error():                     ERRCODE_DB_NOTEXISTS,
	ErrMasterDbNotEmpty.Error():                      ERRCODE_DB_Not_Empty,
	ErrMasterDupSpace.Error():                        ERRCODE_DUP_SPACE,
	ErrMasterSpaceNotExists.Error():                  ERRCODE_SPACE_NOTEXISTS,
	ErrMasterPartitionHasTaskNow.Error():             ERRCODE_PARTITION_HAS_TASK_NOW,
	ErrMasterReplicaNotExists.Error():                ERRCODE_REPLICA_NOT_EXISTS,
	ErrMasterDupReplica.Error():                      ERRCODE_DUP_REPLICA,
	ErrMasterPartitionReplicaLeaderNotDelete.Error(): ERRCODE_PARTITION_REPLICA_LEADER_NOT_DELETE,
	ErrMasterPSNotExists.Error():                     ERRCODE_PS_NOTEXISTS,
	ErrMasterPSAlreadyExists.Error():                 ERRCODE_PS_Already_Exists,
	ErrMasterLocalSpaceOpsFailed.Error():             ERRCODE_LOCAL_SPACE_OPS_FAILED,
	ErrMasterLocalPSOpsFailed.Error():                ERRCODE_Local_PS_Ops_Failed,
	ErrMasterGenIdFailed.Error():                     ERRCODE_GENID_FAILED,
	ErrMasterLocalDbOpsFailed.Error():                ERRCODE_LOCALDB_OPTFAILED,
	ErrMasterSpaceSchemaInvalid.Error():              ERRCODE_SPACE_SCHEMA_INVALID,
	ErrMasterRpcGetClientFailed.Error():              ERRCODE_RPC_GET_CLIENT_FAILED,
	ErrMasterRpcInvalidResp.Error():                  ERRCODE_RPC_INVALID_RESP,
	ErrMasterRpcInvokeFailed.Error():                 ERRCODE_RPC_INVOKE_FAILED,
	ErrMasterRpcParamErr.Error():                     ERRCODE_RPC_PARAM_ERROR,
	ErrGeneralMethodNotImplement.Error():             ERRCODE_METHOD_NOT_IMPLEMENT,
	ErrMasterUserNotExists.Error():                   ERRCODE_USER_NOT_EXISTS,
	ErrMasterDupUser.Error():                         ERRCODE_DUP_USER,
	ErrMasterLocalUserOpsFailed.Error():              ERRCODE_USER_OPS_FAILED,
	ErrMasterAuthenticationFailed.Error():            ERRCODE_AUTHENTICATION_FAILED,
	ErrMasterRegionNotExists.Error():                 ERRCODE_REGION_NOT_EXISTS,
	ErrMasterPSCanNotSelect.Error():                  ERRCODE_MASTER_PS_CAN_NOT_SELECT,
	ErrMasterPSNotEnoughSelect.Error():               ERRCODE_MASTER_PS_NOT_ENOUGH_SELECT,
	ErrPartitionDuplicate.Error():                    ERRCODE_PARTITION_DUPLICATE,
	ErrPartitionNotExist.Error():                     ERRCODE_PARTITION_NOT_EXIST,
	ErrPartitionNotLeader.Error():                    ERRCODE_PARTITION_NOT_LEADER,
	ErrPartitionReqParam.Error():                     ERRCODE_PARTITION_REQ_PARAM,
	ErrPartitionFrozen.Error():                       ERRCODE_PARTITION_FROZEN,
	ErrUnknownPartitionRaftCmdType.Error():           ERRCODE_UNKNOWN_PARTITION_RAFT_CMD_TYPE,
	ErrMasterServerIsNotRunning.Error():              ERRCODE_MASTER_SERVER_IS_NOT_RUNNING,
	ErrPartitionClosed.Error():                       ERRCODE_PARTITION_IS_CLOSED,
	ErrPartitionInvalid.Error():                      ERRCODE_PARTITION_IS_INVALID,
	ErrDocumentNotExist.Error():                      ERRCODE_DOCUMENT_NOT_EXIST,
	ErrDocumentExist.Error():                         ERRCODE_DOCUMENT_EXIST,
	ErrDocumentMustHasSource.Error():                 ERRCODE_DOCUMENT_MUST_HAS_SOURCE,
	ErrDocPulloutVersionNotMatch.Error():             ERRCODE_PULL_OUT_VERSION_NOT_MATCH,
	ErrFuncCanNotInvokeInFrozenEngine.Error():        ERRCODE_FUNC_CAN_NOT_INVOKE_IN_FROZEN_ENGINE,
}

func NewErrMsg(msg string) *Err {
	return &Err{
		msg: msg,
	}
}

func NewErrMsgError(err error) *Err {
	return &Err{
		msg: err.Error(),
	}
}

func NewErrCode(code int) *Err {
	return &Err{
		code: code,
	}
}

func NewErr(code int, msg string) *Err {
	return &Err{
		code: code,
		msg:  msg,
	}
}

type Err struct {
	code int
	msg  string
}

func (e *Err) SetCode(code int) *Err {
	e.code = code
	return e
}

func (e *Err) SetMsg(msg string) *Err {
	e.msg = msg
	return e
}

func (e *Err) GetCode() int {
	return e.code
}

func (e *Err) GetMsg() string {
	return e.msg
}

func (e *Err) GetError() error {
	return fmt.Errorf(e.msg)
}
