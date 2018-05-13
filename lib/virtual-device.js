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
var WATCH_INTERVAL = 60 * 1000;
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
  },
  "STATUS": {
    "type": "array",
    "items": {
      "type": "string",
      "enum:": [
        "total_msg_in",
        "total_msg_out",
        "total_msg_timeout",
        "avg_msg_in_1min",
        "avg_msg_out_1min",
        "avg_msg_in_5min",
        "avg_msg_out_5min",
        "avg_msg_in_30min",
        "avg_msg_out_30min"
      ]
    }
  }
};

function VirtualDevice(conx, uuid, token, configurator) {
  this.conx = conx;
  this.configurator = configurator;
  this.deviceUuid = uuid;
  this.deviceToken = token;
  this.workStatus = {
    "total_msg_in": 0,            //接收处理消息总和
    "total_msg_out": 0,           //外部请求消息总和
    "total_msg_timeout": 0,       //外部超时消息总和
    "total_msg_in_1min": 0,       //最近1分钟接收处理消息总和
    "total_msg_out_1min": 0,      //最近1分钟外部请求消息总和
    "avg_msg_in_1min": 0,         //最近1分钟平均消息处理时间
    "avg_msg_out_1min": 0         //最近1分钟平均外部请求时间
  };
  this.tempData = {
    "total_msg_in": 0,        //接收处理消息总和
    "total_msg_out": 0,       //外部请求消息总和
    "total_msg_in_time": 0,   //接受消息处理时间总和
    "total_msg_out_time": 0   //外部消息处理时间总和
  };
  this.clearTempData = function (self) {
    self.tempData.total_msg_in = 0;
    self.tempData.total_msg_out = 0;
    self.tempData.total_msg_in_time = 0;
    self.tempData.total_msg_out_time = 0;
  };
  this.messageTimeout = configurator.getConf("meshblu_server.message_timeout") * 1000 | 30000;
  this.messageValidate = function (message, schema, callback) {
    var valid = tv4.validate(message, schema);
    if (!valid) {
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
  };

  setInterval(function (self) {
    self.workStatus.avg_msg_in_1min =
      self.tempData.total_msg_in > 0 ? _.ceil(self.tempData.total_msg_in_time / self.tempData.total_msg_in, 2) : 0;
    self.workStatus.avg_msg_out_1min =
      self.tempData.total_msg_out > 0 ? _.ceil(self.tempData.total_msg_out_time / self.tempData.total_msg_out, 2) : 0;
    self.workStatus.total_msg_in_1min = self.tempData.total_msg_in;
    self.workStatus.total_msg_out_1min = self.tempData.total_msg_out;
    self.clearTempData(self);
    //logger.info(self.workStatus);
  }, WATCH_INTERVAL, this);
}

util.inherits(VirtualDevice, EventEmitter);

VirtualDevice.prototype.message = function (message, callback) {
  var self = this;
  var handled = false;
  self.workStatus.total_msg_out++;
  message.topic = "RPC_CALL";
  message.fromUuid = self.deviceUuid;
  if (callback && _.isFunction(callback)) {  //如果存在回调，那么监听消息反馈事件
    message.callbackId = nodeUuid.v4();//生成消息ID，作为事件反馈标识
    var requestTime = Date.now();
    var listener = function (result) {
      handled = true;
      self.tempData.total_msg_out++;
      self.tempData.total_msg_out_time += Date.now() - requestTime;
      callback(result);
    };
    self.once(message.callbackId, listener);
    setTimeout(function () {  //超时控制
      if (!handled) { //移除事件监听器
        self.workStatus.total_msg_timeout++;
        handled = true;
        self.removeListener(message.callbackId, listener);
        logger.error(200003, message);
        callback({
          retCode: 200003,
          description: message.callbackId + ",message listening time out:" + self.messageTimeout + "s",
          data: {}
        });
      }
    }, self.messageTimeout);
  }
  self.conx.message(message); //发送消息
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
          if (util.isNullOrUndefined(func) || !util.isFunction(func)) {
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
            self.workStatus.total_msg_in++;
            self.tempData.total_msg_in++;
            //排除状态查询消息日志打印
            if("status" !== message.payload.cmdName || "0009" !== message.payload.cmdCode){
              logger.debug(message);
            }
            var requestTime = Date.now();
            func.call(self, message.payload.parameters, function (result) {
              self.tempData.total_msg_in_time += Date.now() - requestTime;
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
/**
 *虚拟设备工作状态
 * @param {object[]} message:消息体
 * @param {method} peerCallback:远程RPC回调
 * */
VirtualDevice.prototype.status = function (message, peerCallback) {
  var self = this;
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  if (util.isNullOrUndefined(message)) {
    responseMessage.data = self.workStatus;
    peerCallback(responseMessage);
    return;
  }
  self.messageValidate(message, MESSAGE_SCHEMAS.STATUS, function (error) {
    if (error) {
      peerCallback(error);
      return;
    }
    _.forEach(message, function (item) {
      responseMessage.data[item] = self.workStatus[item];
    });
    logger.debug(responseMessage);
    peerCallback(responseMessage);
  });
};

module.exports = {
  VirtualDevice: VirtualDevice,
  MessageSchemas: MESSAGE_SCHEMAS
};
