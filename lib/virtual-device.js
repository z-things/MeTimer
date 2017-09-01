/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var tv4 = require('tv4');
var _ = require('lodash');
var util = require('util');
var nodeUuid = require("node-uuid");
var EventEmitter = require("events").EventEmitter;

var logger = require('./mlogger/mlogger');
var MESSAGE_SCHEMAS = {
    "RPC_MESSAGE": {
        "type": "object",
        "properties": {
            "devices": {
                "type": [
                    "string",
                    "array"
                ]
            },
            "topic": {
                "type": "string",
                "enum": ["RPC_CALL", "RPC_BACK"]
            },
            "fromUuid": {
                "type": "string"
            },
            "callbackId": {
                "type": "string"
            },
            "payload": {
                "type": "object"
            }
        },
        "required": ["devices", "topic", "fromUuid", "payload"]
    },
    "RPC_CALL": {
        "type": "object",
        "properties": {
            "cmdName": {"type": "string"},
            "cmdCode": {"type": "string"},
            "parameters": {
                "type": [
                    "object",
                    "array",
                    "number",
                    "boolean",
                    "string",
                    "null"
                ]
            }
        },
        "required": ["cmdName", "cmdCode", "parameters"]
    },
    "RPC_BACK": {
        "type": "object",
        "properties": {
            "retCode": {"type": "number"},
            "description": {"type": "string"},
            "data": {
                "type": [
                    "object",
                    "array",
                    "number",
                    "boolean",
                    "string",
                    "null"
                ]
            }
        },
        "required": ["retCode", "description", "data"]
    }
};

function VirtualDevice(conx, uuid, token, configurator) {
    this.conx = conx;
    this.configurator = configurator;
    this.deviceUuid = uuid;
    this.deviceToken = token;
    this.messageValidate = function (message, schema, callback) {
        var valid = tv4.validate(message, schema);
        if (!valid) {
            logger.debug(message);
            var error = {
                retCode: 200001,
                description: {
                    message: tv4.error.message,
                    dataPath: tv4.error.dataPath,
                    schemaPath: tv4.error.schemaPath
                },
                data: {}
            };
            callback(error);
        }
        else {
            callback(null);
        }
    }
}
util.inherits(VirtualDevice, EventEmitter);

VirtualDevice.prototype.message = function (message, callback) {
    var self = this;
    var handled = false;
    message.topic = "RPC_CALL";
    message.fromUuid = self.deviceUuid;
    if (callback && _.isFunction(callback)) {  //如果存在回调，那么监听消息反馈事件
        message.callbackId = nodeUuid.v4();//生成消息ID，作为事件反馈标识
        var listener = function (result) {
            handled = true;
            callback(result);
        };
        self.once(message.callbackId, listener);
        setTimeout(function () {  //超时控制
            if (!handled) { //移除事件监听器
                handled = true;
                self.removeListener(message.callbackId, listener);
                logger.error(200003, message);
                callback({
                    retCode: 200003,
                    description: message.callbackId + ",message listening time out:" + self.configurator.getConf("meshblu_server.message_timeout") + "s",
                    data: {}
                });
            }
        }, self.configurator.getConf("meshblu_server.message_timeout") * 1000);
    }
    self.conx.message(message); //发送消息
    logger.trace(message);
};


/**
 *虚拟设备消息入口
 * @param {object[]} message:消息体
 * */
VirtualDevice.prototype.onMessage = function (message) {
    var self = this;
    //request message
    var retMsg = {
        devices: [message.fromUuid],
        topic: "RPC_BACK",
        fromUuid: self.deviceUuid,
        callbackId: message.callbackId,
        payload: {
            retCode: 200,
            description: "Success.",
            data: {}
        }
    };
    self.messageValidate(message, MESSAGE_SCHEMAS.RPC_MESSAGE, function (error) {
        if (!util.isNullOrUndefined(error)) {
            retMsg.payload = error;
            logger.error(error.retCode, error.description);
            if (message.topic === "RPC_CALL") {
                self.conx.message(retMsg);
            }
        }
        else if (message.topic === "RPC_BACK") {
            self.messageValidate(message.payload, MESSAGE_SCHEMAS.RPC_BACK, function (error) {
                if (util.isNullOrUndefined(error)) {
                    self.emit(message.callbackId, message.payload);
                }
                else {
                    logger.error(error.retCode, error.description);
                }
            })
        }
        else {
            self.messageValidate(message.payload, MESSAGE_SCHEMAS.RPC_CALL, function (error) {
                if (!util.isNullOrUndefined(error)) {
                    logger.error(error.retCode, error.description);
                    retMsg.payload = error;
                    self.conx.message(retMsg);
                }
                else {
                    var func = self[message.payload.cmdName];
                    if (util.isNullOrUndefined(func) || !_.isFunction(func)) {
                        var logError = {
                            errorId: 200004,
                            errorMsg: "method name=" + message.payload.cmdName
                        };
                        logger.error(200004, " method name=" + message.payload.cmdName);
                        retMsg.payload.retCode = logError.errorId;
                        retMsg.payload.description = logError.errorMsg;
                        self.conx.message(retMsg);
                    }
                    else {
                        var logInfo = "method call:" + message.payload.cmdName + ", "
                            + "method params:" + JSON.stringify(message.payload.parameters);
                        logger.debug(logInfo);
                        func.call(self, message.payload.parameters, function (result) {
                            if (!util.isNullOrUndefined(retMsg.callbackId)) {
                                retMsg.payload.retCode = result.retCode;
                                retMsg.payload.description = JSON.stringify(result.description);
                                retMsg.payload.data = result.data;
                                //发送反馈消息
                                self.conx.message(retMsg);
                            }
                        });
                    }
                }
            })
        }
    });
};

module.exports = {
    VirtualDevice: VirtualDevice,
    MessageSchemas: MESSAGE_SCHEMAS
};
