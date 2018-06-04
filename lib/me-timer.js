/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var _ = require('lodash');
var util = require('util');
var uuid = require('node-uuid');
var async = require('async');
var schedule = require('node-schedule');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var logger = require('./mlogger/mlogger');
const HL_TYPE_ID = '050608070001';
const ONE_MIN_MS = 60 * 1000;
const ONE_HOUR_MS = 60 * ONE_MIN_MS;
const ONE_DAY_MS = 24 * ONE_HOUR_MS;
const RESPONSE_MESSAGE = {retCode: 200, description: "Success.", data: {}};
const SUPPORTED_DEVICE = [
  "05060B050000",
  "05060B052000",
  "050608070001",
  "040B08040004",
  "040B09050101",
  "040B09050102",
  "040B09050103",
  "040B09050111",
  "040B09050112",
  "040B09050113",
  "040B09050201",
  "040B09050202",
  "040B09050203"
];
const OPERATION_SCHEMAS = {
  get: {
    "type": "object",
    "properties": {
      "deviceId": {"type": "string"},
      "timerId": {"type": "string"}
    },
    "required": ["deviceId", "timerId"]
  },
  update: {
    "type": "object",
    "properties": {
      "deviceId": {
        "type": "string"
      },
      "timerId": {
        "type": "string"
      },
      "timer": {
        "type": "object",
        "properties": {
          "timerId": {
            "type": "string"
          },
          "name": {
            "type": "string"
          },
          "mode": {
            "type": "string",
            "enum": ["PARALLEL", "SERIES", "WATERFALL"]
          },
          "interval": {
            "type": "integer",
            "minimum": 0,
            "maximum": 59
          },
          "between": {
            "type": "array",
            "items": {
              "type": "string",
              "pattern": "([01]\\d|2[0-3]):([0-5]\\d)"
            }
          },
          "weekday": {
            "type": "array",
            "items": {
              "type": "integer",
              "minimum": 0,
              "maximum": 6
            }
          },
          "timeZoneOffset": {
            "type": "integer"
          },
          "commands": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "uuid": {
                  "type": ["string", "array"]
                },
                "deviceType": {
                  "type": "string"
                },
                "cmd": {
                  "type": "object",
                  "properties": {
                    "cmdName": {
                      "type": "string"
                    },
                    "cmdCode": {
                      "type": "string"
                    },
                    "parameters": {
                      "type": ["object", "string", "array"]
                    }
                  },
                  "required": ["cmdName", "cmdCode", "parameters"]
                },
                "enable": {"type": "boolean"}
              },
              "required": ["uuid", "deviceType"]
            }
          },
          "enable": {"type": "boolean"}
        },
        "required": ["timerId", "name", "mode", "interval", "between", "commands"]
      }
    },
    "required": ["deviceId", "timerId", "timer"]
  },
  delete: {
    "type": "object",
    "properties": {
      "deviceId": {"type": "string"},
      "timerId": {"type": ["string", "array"]}
    },
    "required": ["deviceId", "timerId"]
  },
  add: {
    "type": "object",
    "properties": {
      "deviceId": {
        "type": "string"
      },
      "timer": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          },
          "enable": {
            "type": "boolean"
          },
          "mode": {
            "type": "string",
            "enum": ["PARALLEL", "SERIES", "WATERFALL"]
          },
          "interval": {
            "type": "integer",
            "minimum": 0,
            "maximum": 59
          },
          "between": {
            "type": "array",
            "items": {
              "type": "string",
              "pattern": "([01]\\d|2[0-3]):([0-5]\\d)"
            }
          },
          "weekday": {
            "type": "array",
            "items": {
              "type": "integer",
              "minimum": 0,
              "maximum": 6
            }
          },
          "timeZoneOffset": {
            "type": "integer"
          },
          "commands": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "uuid": {
                  "type": ["string", "array"]
                },
                "deviceType": {
                  "type": "string"
                },
                "enable": {
                  "type": "boolean"
                },
                "cmd": {
                  "type": "object",
                  "properties": {
                    "cmdName": {
                      "type": "string"
                    },
                    "cmdCode": {
                      "type": "string"
                    },
                    "parameters": {
                      "type": ["object", "string", "array"]
                    }
                  },
                  "required": ["cmdName", "cmdCode", "parameters"]
                }
              },
              "required": ["uuid", "deviceType", "cmd"]
            }
          }
        },
        "required": ["name", "mode", "interval", "between", "commands"]
      }
    },
    "required": ["deviceId", "timer"]
  },
  active: {
    "type": "object",
    "properties": {
      "deviceId": {"type": "string"},
      "timerId": {"type": "string"},
      "automatic": {
        "enable": {
          "type": "boolean"
        },
        "commands": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "index": {"type": "integer"},
              "cmdCode": {"type": "string"},
              "enable": {
                "type": "boolean"
              }
            },
            "required": ["cmdCode", "enable"]
          }
        }
      }
    },
    "required": ["deviceId", "timerId", "automatic"]
  }
};

function conflictDetection(timers, newTimer, callback) {
  if (!timers || timers.length <= 0) {
    callback(null);
    return;
  }
  if (newTimer.between.length >= 2 && newTimer.commands.length >= 2) {
    if (newTimer.between[0] === newTimer.between[1]) {
      var error = "Conflict with ["
        + newTimer.between[0] + " "
        + newTimer.commands[0].name + ","
        + newTimer.between[1] + " "
        + newTimer.commands[1].name + "]";
      logger.error(205015, error);
      callback({
        errorId: 205015,
        errorMsg: error
      });
      return;
    }
  }
  for (var i = 0; i < timers.length; i++) {
    var policy = timers[i].policy;
    if (timers[i].description === "control flow"
      && policy.commands.length === newTimer.commands.length) {
      var weekConflict = false;
      //先检测周期是否有重叠，如果周期都没有重叠那么就肯定不会产生冲突
      for (var l = 0; l < policy.weekday.length && weekConflict === false; l++) {
        for (var m = 0; m < newTimer.weekday.length; m++) {
          if (policy.weekday[l] === newTimer.weekday[m]) {
            weekConflict = true;
            break;
          }
        }
      }
      if (weekConflict) {
        if (policy.commands.length === 1) {
          //如果是单操作那么只要时间点重叠就意味发生冲突
          if (policy.between[0] === newTimer.between[0]) {
            //205015
            var opName = policy.commands[0].name.toUpperCase();
            var opId = policy.commands[0].id;
            var option = policy.commands[0].options[0];
            if (opName === "setMode" && opId === "3102" && option) {
              opName = option.value;
            }
            if (opName !== "OFF") {
              opName = "OPEN";
            }
            else {
              opName = "CLOSE";
            }
            var errorDetail = "Conflict with ["
              + policy.between[0] + " "
              + opName + "]";
            logger.error(205015, errorDetail);
            callback({
              errorId: 205015,
              errorMsg: errorDetail
            });
            return;
          }
        }
        else {//如果是成对操作，那么只要起始时间段发生重叠那么就意味着发生冲突
          try {
            var beginArray1 = policy.between[0].split(':');
            var endArray1 = policy.between[1].split(':');
            var beginTime1 = parseInt(beginArray1[0], 10) * 3600 + parseInt(beginArray1[1], 10) * 60;
            var endTime1 = parseInt(endArray1[0], 10) * 3600 + parseInt(endArray1[1], 10) * 60;
            if (beginTime1 > endTime1) {
              var temp1 = beginTime1;
              beginTime1 = endTime1;
              endTime1 = temp1;
            }
            var beginArray2 = newTimer.between[0].split(':');
            var endArray2 = newTimer.between[1].split(':');
            var beginTime2 = parseInt(beginArray2[0], 10) * 3600 + parseInt(beginArray2[1], 10) * 60;
            var endTime2 = parseInt(endArray2[0], 10) * 3600 + parseInt(endArray2[1], 10) * 60;
            if (beginTime2 > endTime2) {
              var temp2 = beginTime2;
              beginTime2 = endTime2;
              endTime2 = temp2;
            }
            if ((beginTime2 >= beginTime1 && beginTime2 <= endTime1)
              || (beginTime1 >= beginTime2 && beginTime2 <= endTime2)) {
              var opName0 = policy.commands[0].name.toUpperCase();
              var opId0 = policy.commands[0].id;
              var option0 = policy.commands[0].options[0];
              var opName1 = policy.commands[1].name.toUpperCase();
              var opId1 = policy.commands[1].id;
              var option1 = policy.commands[1].options[0];
              if (opName0 === "setMode" && opId0 === "3102" && option0) {
                opName0 = option0.value;
              }
              if (opName1 === "setMode" && opId1 === "3102" && option1) {
                opName1 = option1.value;
              }
              if (opName0 !== "OFF") {
                opName0 = "OPEN";
              }
              else {
                opName1 = "CLOSE";
              }
              if (opName1 !== "OFF") {
                opName1 = "OPEN";
              }
              else {
                opName1 = "CLOSE";
              }
              var errorDetail1 = "Conflict with ["
                + policy.between[0] + " "
                + opName0 + ","
                + policy.between[1] + " "
                + opName1 + "]";
              //logger.error(205015, errorDetail1);
              callback({
                errorId: 205015,
                errorMsg: errorDetail1
              });
              return;
            }
          }
          catch (exception) {
            logger.error(200000, exception.message);
          }
        }
      }
    }
  }
  callback(null);
}

function removeTimer(timers, timerId) {
  var index = _.findIndex(timers, {"timerId": timerId});
  if (-1 !== index) {
    _.pullAt(timers, index);
  }
  return index;
}

function Timer(conx, uuid, token, configurator) {
  this.jobSchedule = {};
  VirtualDevice.call(this, conx, uuid, token, configurator);

  /*
   * 并行执行定时器，对于每一个执行对象并行执行定时命令，
   * 如果指定了命令索引（index）则，只执行执行索引的命令，
   * 否则对于每个执行对象并行执行所有定时命令。
   * @param {object} self :自引用
   * @param {object} timer :定时器
   * @param {integer} index : 命令索引
   * */
  this.executeParallel = function (self, timer, index) {
    if (util.isNullOrUndefined(self)) {
      self = this;
    }
    _.forEach(timer.commands, function (command, i) {
      if (util.isNullOrUndefined(index) || i === index) {
        if (util.isNullOrUndefined(command.enable) || command.enable === true) {
          if (!_.isArray(command.uuid)) {
            command.uuid = [command.uuid];
          }
          _.forEach(command.uuid, function (uuid) {
            self.message({
              devices: self.configurator.getConfRandom("services.executor"),
              payload: {
                cmdName: "execute",
                cmdCode: "0001",
                parameters: {
                  deviceUuid: uuid,
                  cmd: command.cmd
                }
              }
            });
          });
        }
      }
    });
  };

  /*
   * 串行执行定时器，对于每一个执行对象串行执行定时命令，
   * 如果指定了命令索引（index）则，只执行执行索引的命令，
   * 否则对于每个执行对象串行执行所有定时命令。
   * @param {object} self :自引用
   * @param {object} timer :定时器
   * @param {integer} index : 命令索引
   * */
  this.executeSeries = function (self, timer, index) {
    _.forEach(timer.commands, function (command, i) {
      if (!util.isNullOrUndefined(index) && i !== index) {
        return;
      }
      if (!util.isNullOrUndefined(command.enable) && command.enable !== true) {
        return;
      }
      if (!_.isArray(command.uuid)) {
        command.uuid = [command.uuid];
      }
      async.mapSeries(command.uuid,
        function (uuid, innerCallback) {
          self.message({
            devices: self.configurator.getConfRandom("services.executor"),
            payload: {
              cmdName: "execute",
              cmdCode: "0001",
              parameters: {
                deviceUuid: uuid,
                cmd: command.cmd
              }
            }
          }, function (response) {
            if (response.retCode === 200) {
              innerCallback(null, {
                cmdName: command.cmd.cmdName,
                cmdCode: command.cmd.cmdCode,
                result: "SUCCESS"
              });
            }
            else {
              innerCallback(null, {
                cmdName: command.cmd.cmdName,
                cmdCode: command.cmd.cmdCode,
                result: "FAILED"
              });
            }
          });
        },
        function (error, results) {
          logger.debug(results);
        });
    });
  };

  this.executeTimer = function (self, deviceId, timerId, index) {
    logger.debug("deviceId=" + deviceId + ",timerId=" + timerId + ",index=" + index);
    if (util.isNullOrUndefined(self)) {
      self = this;
    }
    self.get({deviceId: deviceId, timerId: timerId}, function (response) {
      if (200 !== response.retCode) {
        logger.error(response.retCode, response.description);
        return;
      }
      var timer = response.data;
      if (true !== timer.enable) {
        return;
      }
      if (timer.mode === "PARALLEL") {
        self.executeParallel(self, timer, index);
      }
      else if (timer.mode === "SERIES") {
        self.executeSeries(self, timer, index);
      }
      else {
        //todo
      }
      //如果是定点单次执行的定时器，执行后自动禁用，避免定时器重启后再次被解析执行
      if (util.isNullOrUndefined(timer.weekday) || 0 === timer.weekday.length) {
        if (util.isNullOrUndefined(index)) {
          //无指定命令则禁用整个定时器
          self.active({
            deviceId: deviceId,
            timerId: timerId,
            automatic: {
              enable: false
            }
          }, function (response) {
            if (response.retCode !== 200) {
              logger.error({errorId: response.retCode, errorMsg: response.description});
            }
          });
        }
        else {
          //有指定命令，禁用指定的命令
          self.active({
            deviceId: deviceId,
            timerId: timerId,
            automatic: {
              commands: [
                {
                  index: index,
                  cmdCode: timer.commands[index].cmd.cmdCode,
                  enable: false
                }
              ]
            }
          }, function (response) {
            if (response.retCode !== 200) {
              logger.error({errorId: response.retCode, errorMsg: response.description});
            }
          });
        }
      }
    });
  };

  this.parseTimer = function (deviceId, timer, callback) {
    var self = this;
    var scheduleJobs = [];
    try {
      /*if (!util.isNullOrUndefined(timer.enable) && timer.enable === false) {
       callback(null, scheduleJobs);
       return;
       }*/
      if (timer.interval > 0) {  //在某个时间段内间隔一定时间周期执行
        var crontab = "*/" + timer.interval;    //"crontab": "*/5 0-13 * * 1,4,5,0",
        if (!util.isNullOrUndefined(timer.between) || timer.between.length === 2) {
          var beginTime = timer.between[0];
          var endTime = timer.between[1];
          var beginTimeArray = beginTime.split(':');
          var endTimeArray = endTime.split(':');
          var beginHour = parseInt(beginTimeArray[0], 10);
          var endHour = parseInt(endTimeArray[0], 10) - 1;
          if (beginHour === endHour) {
            crontab += " "
              + beginHour
              + " * * ";
          }
          else {
            crontab += " "
              + beginHour
              + "-"
              + endHour
              + " * * ";
          }

        }
        else {
          crontab += " * * * ";
        }
        if (util.isNullOrUndefined(timer.weekday) || timer.weekday.length >= 7 || timer.weekday.length === 0) {
          crontab += "*";
        }
        else {
          for (var i = 0, len1 = timer.weekday.length; i < len1; i++) {
            if (i === 0) {
              crontab += timer.weekday[i];
            }
            else {
              crontab += "," + timer.weekday[i];
            }
          }
        }
        logger.debug("crontab:[" + crontab + "]");
        scheduleJobs = [schedule.scheduleJob(crontab, self.executeTimer.bind(null, self, deviceId, timer.timerId))];
      }
      else {
        if (util.isNullOrUndefined(timer.weekday) || 0 === timer.weekday.length) {  //单次执行
          timer.between.forEach(function (time, index) {
            if (!_.isEmpty(time)) {
              logger.debug(time);
              var dateNow = new Date();
              var timestampNow = dateNow.getTime();
              var timeArray = time.split(":");
              var hour = parseInt(timeArray[0], 10);
              var minute = parseInt(timeArray[1], 10);
              var msOfDay = hour * ONE_HOUR_MS + minute * ONE_MIN_MS;
              var msOfDayNow = dateNow.getHours() * ONE_HOUR_MS + dateNow.getMinutes() * ONE_MIN_MS;
              if (msOfDayNow > msOfDay) {
                timestampNow += ONE_DAY_MS - (msOfDayNow - msOfDay);
              }
              else {
                timestampNow += msOfDay - msOfDayNow;
              }
              logger.debug(new Date(timestampNow).toISOString());
              var job = schedule.scheduleJob(new Date(timestampNow),
                self.executeTimer.bind(null, self, deviceId, timer.timerId, index));
              scheduleJobs.push(job);
            }
          });
        }
        else {//在指定时间点周期执行（重复执行，自定义执行）
          timer.between.forEach(function (time, index) {
            if (!_.isEmpty(time)) {
              var rule = new schedule.RecurrenceRule();
              rule.dayOfWeek = timer.weekday;
              var timeArray = time.split(":");
              rule.hour = parseInt(timeArray[0], 10);
              rule.minute = parseInt(timeArray[1], 10);
              logger.debug({
                dayOfWeek: rule.dayOfWeek,
                hour: rule.hour,
                minute: rule.minute
              });
              var job = schedule.scheduleJob(rule,
                self.executeTimer.bind(null, self, deviceId, timer.timerId, index));
              scheduleJobs.push(job);
            }
          });
        }
      }
      callback(null, scheduleJobs);
    }
    catch (e) {
      callback({errorId: 209002, errorMsg: timer})
    }
  };

  this.deleteJobScheduleOfTimer = function (timerId) {
    var self = this;
    if (!util.isNullOrUndefined(self.jobSchedule[timerId])
      && util.isArray(self.jobSchedule[timerId])) {
      _.forEach(self.jobSchedule[timerId], function (job) {
        !util.isNullOrUndefined(job) && job.cancel();
      });
      delete self.jobSchedule[timerId];
    }
  }
}

util.inherits(Timer, VirtualDevice);

/**
 * 远程RPC回调函数
 * @callback onMessage~init
 * @param {object} response:
 * {
 *      "retCode":{number},
 *      "description":{string},
 *      "data":{object}
 * }
 */
/**
 * 初始化定时器
 * */
Timer.prototype.init = function () {
  var self = this;
  async.mapSeries(SUPPORTED_DEVICE, function (deviceType, innerCallback) {
    var msg = {
      devices: self.configurator.getConfRandom("services.device_manager"),
      payload: {
        cmdName: "getDevice",
        cmdCode: "0003",
        parameters: {
          "type.id": deviceType
        }
      }
    };
    self.message(msg, function (response) {
      if (response.retCode === 200) {
        innerCallback(null, response.data);
      } else {
        innerCallback(null);
      }
    });
  }, function (error, allDevices) {
    if (!util.isNullOrUndefined(allDevices)) {
      allDevices.forEach(function (deviceGroup) {
        if (!util.isNullOrUndefined(deviceGroup) && util.isArray(deviceGroup)) {
          deviceGroup.forEach(function (device) {
            var timers = device.extra.timers;
            if (!util.isNullOrUndefined(timers) && util.isArray(timers)) {
              timers.forEach(function (timer) {
                self.parseTimer(device.uuid, timer, function (error, jobs) {
                  if (error) {
                    logger.error(error.errorId, error.errorMsg);
                  }
                  else {
                    self.jobSchedule[timer.timerId] = jobs;
                  }
                });
              })
            }
          });
        }
      });
    }
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~get
 * @param {object} response:
 * {
 *      "retCode":{number},
 *      "description":{string},
 *      "data":{object}
 * }
 */
/**
 * 查询定时器
 * @param {object} message :消息体
 * @param {onMessage~get} peerCallback: 远程RPC回调函数
 * */
Timer.prototype.get = function (message, peerCallback) {
  var self = this;
  var responseMessage = _.clone(RESPONSE_MESSAGE);
  self.messageValidate(message, OPERATION_SCHEMAS.get, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
      return;
    }
    async.waterfall([
      /*get device info*/
      function (innerCallback) {
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "getDevice",
            cmdCode: "0003",
            parameters: {
              uuid: message.deviceId
            }
          }
        };
        if (!util.isNullOrUndefined(message.userId)) {
          msg.payload.parameters.userId = message.userId;
        }
        self.message(msg, function (response) {
          if (response.retCode === 200) {
            var deviceInfo = util.isArray(response.data) ? _.first(response.data) : response.data;
            innerCallback(null, deviceInfo);
          } else {
            innerCallback({errorId: response.retCode, errorMsg: response.description});
          }
        });
      },
      function (deviceInfo, innerCallback) {
        if (message.timerId === "*") {
          responseMessage.data = deviceInfo.extra.timers;
          if (util.isNullOrUndefined(responseMessage.data)) {
            responseMessage.data = []
          }
          innerCallback(null);
        }
        else {
          var timer = null;
          if (!util.isNullOrUndefined(deviceInfo.extra) && !util.isNullOrUndefined(deviceInfo.extra.timers)) {
            timer = _.find(deviceInfo.extra.timers, {timerId: message.timerId});
          }
          if (timer) {
            responseMessage.data = timer;
            innerCallback(null);
          }
          else {
            innerCallback({
              errorId: 209001,
              errorMsg: "no timer found by given uuid:[" + message.timerId + "]"
            });
          }
        }
      }
    ], function (error) {
      if (error) {
        responseMessage.retCode = error.errorId;
        responseMessage.description = error.errorMsg;
      }
      peerCallback(responseMessage);
    });
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~update
 * @param {object} response:
 * {
 *      "retCode":{number},
 *      "description":{string},
 *      "data":{object}
 * }
 */
/**
 * 更新定时器
 * @param {object} message :消息体
 * @param {onMessage~update} peerCallback: 远程RPC回调函数
 * */
Timer.prototype.update = function (message, peerCallback) {
  var self = this;
  var responseMessage = _.clone(RESPONSE_MESSAGE);
  self.messageValidate(message, OPERATION_SCHEMAS.update, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
      return;
    }
    async.waterfall([
      /*get device info*/
      function (innerCallback) {
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "getDevice",
            cmdCode: "0003",
            parameters: {
              uuid: message.deviceId
            }
          }
        };
        if (!util.isNullOrUndefined(message.userId)) {
          msg.payload.parameters.userId = message.userId;
        }
        self.message(msg, function (response) {
          if (response.retCode === 200) {
            var deviceInfo = util.isArray(response.data) ? _.first(response.data) : response.data;
            innerCallback(null, deviceInfo);
          } else {
            innerCallback({errorId: response.retCode, errorMsg: response.description});
          }
        });
      },
      function (deviceInfo, innerCallback) {
        /*if (deviceInfo.type.id === HL_TYPE_ID)*/
        {
          self.parseTimer(deviceInfo.uuid, message.timer, function (error, jobs) {
            if (error) {
              logger.error(error.errorId, error.errorMsg);
              innerCallback(error)
            }
            else {
              self.deleteJobScheduleOfTimer(message.timer.timerId);
              self.jobSchedule[message.timer.timerId] = jobs;
              innerCallback(null, deviceInfo);
            }
          });
        }
        /*else {
         var msg = {
         devices: deviceInfo.owner,
         payload: {
         cmdName: "update_timer",
         cmdCode: "0003",
         parameters: message.timer
         }
         };
         /!*if (deviceInfo.type.id === HL_TYPE_ID) {
         msg = {
         devices: self.configurator.getConfRandom("services.executor"),
         payload: {
         cmdName: "execute",
         cmdCode: "0001",
         parameters: {
         userUuid: deviceInfo.userId,
         deviceUuid: deviceInfo.uuid,
         cmd: {
         cmdName: "update_timer",
         cmdCode: "0005",
         parameters: {
         timer: [{
         week: message.timer.weekday[0],
         sub_timer: [{
         index: message.timer.index,
         time: message.timer.between[0],
         temp_heat: message.timer.commands[0].cmd.parameters.temp_heat
         }]
         }]
         }
         }
         }
         }
         }
         }*!/
         self.message(msg, function (response) {
         if (response.retCode !== 200) {
         innerCallback({errorId: response.retCode, errorMsg: response.description});
         } else {
         innerCallback(null, deviceInfo);
         }
         });
         }*/
      },
      function (deviceInfo, innerCallback) {
        var foundFlag = false;
        if (!util.isNullOrUndefined(deviceInfo.extra) && !util.isNullOrUndefined(deviceInfo.extra.timers)) {
          var index = _.findIndex(deviceInfo.extra.timers, {timerId: message.timerId});
          if (-1 !== index) {
            deviceInfo.extra.timers[index] = message.timer;
            foundFlag = true;
          }
        }
        if (!foundFlag) {
          innerCallback({
            errorId: 209001,
            errorMsg: "no timer found by given uuid:[" + message.timerId + "]"
          });
        }
        else {
          var msg = {
            devices: self.configurator.getConfRandom("services.device_manager"),
            payload: {
              cmdName: "deviceUpdate",
              cmdCode: "0004",
              parameters: {
                "uuid": deviceInfo.uuid,
                "extra.timers": deviceInfo.extra.timers
              }
            }
          };
          self.message(msg, function (response) {
            if (response.retCode !== 200) {
              innerCallback({errorId: response.retCode, errorMsg: response.description});
            } else {
              innerCallback(null);
            }
          });
        }
      }
    ], function (error) {
      if (error) {
        responseMessage.retCode = error.errorId;
        responseMessage.description = error.errorMsg;
      }
      peerCallback(responseMessage);
    });
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~delete
 * @param {object} response:
 * {
 *      "retCode":{number},
 *      "description":{string},
 *      "data":{object}
 * }
 */
/**
 * 删除定时器
 * @param {object} message :消息体
 * @param {onMessage~delete} peerCallback: 远程RPC回调函数
 * */
Timer.prototype.delete = function (message, peerCallback) {
  var self = this;
  var responseMessage = _.clone(RESPONSE_MESSAGE);
  self.messageValidate(message, OPERATION_SCHEMAS.delete, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
      return;
    }
    async.waterfall([
      /*get device info*/
      function (innerCallback) {
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "getDevice",
            cmdCode: "0003",
            parameters: {
              uuid: message.deviceId
            }
          }
        };
        if (!util.isNullOrUndefined(message.userId)) {
          msg.payload.parameters.userId = message.userId;
        }
        self.message(msg, function (response) {
          if (response.retCode === 200) {
            var deviceInfo = util.isArray(response.data) ? _.first(response.data) : response.data;
            innerCallback(null, deviceInfo);
          } else {
            innerCallback({errorId: response.retCode, errorMsg: response.description});
          }
        });
      },
      function (deviceInfo, innerCallback) {
        /*if (deviceInfo.type.id === HL_TYPE_ID)*/
        if (util.isArray(message.timerId)) {
          _.forEach(message.timerId, function (timerId) {
            self.deleteJobScheduleOfTimer(timerId);
          });
        }
        else {
          self.deleteJobScheduleOfTimer(message.timerId);
        }
        innerCallback(null, deviceInfo);
        /*else {
         var msg = {
         devices: deviceInfo.owner,
         payload: {
         cmdName: "delete_timer",
         cmdCode: "0004",
         parameters: {
         timerId: message.timerId
         }
         }
         };
         //innerCallback(null, deviceInfo);
         self.message(msg, function (response) {
         if (response.retCode !== 200) {
         innerCallback({errorId: response.retCode, errorMsg: response.description});
         } else {
         innerCallback(null, deviceInfo);
         }
         });
         }*/
      },
      function (deviceInfo, innerCallback) {
        var foundFlag = false;
        if (!util.isNullOrUndefined(deviceInfo.extra) && !util.isNullOrUndefined(deviceInfo.extra.timers)) {
          if (util.isArray(message.timerId)) {
            _.forEach(message.timerId, function (timerId) {
              var index = removeTimer(deviceInfo.extra.timers, timerId);
              if (-1 !== index) {
                foundFlag = true;//如果是批量删除定时器，只要有一个正确删除，表示成功
              }
            });
          }
          else {
            var index = removeTimer(deviceInfo.extra.timers, message.timerId);
            if (-1 !== index) {
              foundFlag = true;
            }
          }
        }
        if (!foundFlag) {
          innerCallback({
            errorId: 209001,
            errorMsg: "no timer found by given uuid:[" + message.timerId + "]"
          });
        }
        else {
          var msg = {
            devices: self.configurator.getConfRandom("services.device_manager"),
            payload: {
              cmdName: "deviceUpdate",
              cmdCode: "0004",
              parameters: {
                "uuid": deviceInfo.uuid,
                "extra.timers": deviceInfo.extra.timers
              }
            }
          };
          self.message(msg, function (response) {
            if (response.retCode !== 200) {
              innerCallback({errorId: response.retCode, errorMsg: response.description});
            } else {
              innerCallback(null);
            }
          });
        }
      }
    ], function (error) {
      if (error) {
        responseMessage.retCode = error.errorId;
        responseMessage.description = error.errorMsg;
      }
      peerCallback(responseMessage);
    });
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~add
 * @param {object} response:
 * {
 *      "retCode":{number},
 *      "description":{string},
 *      "data":{object}
 * }
 */
/**
 * 添加定时器
 * @param {object} message :消息体
 * @param {onMessage~add} peerCallback: 远程RPC回调函数
 * */
Timer.prototype.add = function (message, peerCallback) {
  var self = this;
  var responseMessage = _.clone(RESPONSE_MESSAGE);
  self.messageValidate(message, OPERATION_SCHEMAS.add, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
      return;
    }
    async.waterfall([
      /*get device info*/
      function (innerCallback) {
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "getDevice",
            cmdCode: "0003",
            parameters: {
              uuid: message.deviceId
            }
          }
        };
        if (!util.isNullOrUndefined(message.userId)) {
          msg.payload.parameters.userId = message.userId;
        }
        self.message(msg, function (response) {
          if (response.retCode === 200) {
            var deviceInfo = util.isArray(response.data) ? _.first(response.data) : response.data;
            innerCallback(null, deviceInfo);
          } else {
            innerCallback({errorId: response.retCode, errorMsg: response.description});
          }
        });
      },
      function (deviceInfo, innerCallback) {
        /*if (deviceInfo.type.id === HL_TYPE_ID)*/
        {
          var timerId = uuid.v4();
          message.timer.timerId = timerId;
          self.parseTimer(deviceInfo.uuid, message.timer, function (error, jobs) {
            if (error) {
              logger.error(error.errorId, error.errorMsg);
              innerCallback(error)
            }
            else {
              self.jobSchedule[timerId] = jobs;
              innerCallback(null, deviceInfo, timerId);
            }
          });
        }
        /*else {
         var msg = {
         devices: deviceInfo.owner,
         payload: {
         cmdName: "set_timer",
         cmdCode: "0002",
         parameters: message.timer
         }
         };
         //innerCallback(null, deviceInfo, uuid.v4());
         self.message(msg, function (response) {
         if (response.retCode !== 200) {
         innerCallback({errorId: response.retCode, errorMsg: response.description});
         } else {
         innerCallback(null, deviceInfo, response.data);
         }
         });
         }*/
      },
      function (deviceInfo, timerId, innerCallback) {
        message.timer.timerId = timerId;
        if (util.isNullOrUndefined(deviceInfo.extra.timers)) {
          deviceInfo.extra.timers = [];
        }
        deviceInfo.extra.timers.push(message.timer);
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "deviceUpdate",
            cmdCode: "0004",
            parameters: {
              "uuid": deviceInfo.uuid,
              "extra.timers": deviceInfo.extra.timers
            }
          }
        };
        self.message(msg, function (response) {
          if (response.retCode !== 200) {
            innerCallback({errorId: response.retCode, errorMsg: response.description});
          } else {
            innerCallback(null, timerId);
          }
        });
      }
    ], function (error, timerId) {
      if (error) {
        responseMessage.retCode = error.errorId;
        responseMessage.description = error.errorMsg;
      }
      else {
        responseMessage.data = timerId;
      }
      peerCallback(responseMessage);
    });
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~active
 * @param {object} response:
 * {
 *      "retCode":{number},
 *      "description":{string},
 *      "data":{object}
 * }
 */
/**
 * 添加定时器
 * @param {object} message :消息体
 * @param {onMessage~active} peerCallback: 远程RPC回调函数
 * */
Timer.prototype.active = function (message, peerCallback) {
  logger.debug(JSON.stringify(message));
  var self = this;
  var responseMessage = _.clone(RESPONSE_MESSAGE);
  self.messageValidate(message, OPERATION_SCHEMAS.active, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
      return;
    }
    async.waterfall([
      /*get device info*/
      function (innerCallback) {
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "getDevice",
            cmdCode: "0003",
            parameters: {
              uuid: message.deviceId
            }
          }
        };
        if (!util.isNullOrUndefined(message.userId)) {
          msg.payload.parameters.userId = message.userId;
        }
        self.message(msg, function (response) {
          if (response.retCode === 200) {
            var deviceInfo = util.isArray(response.data) ? _.first(response.data) : response.data;
            innerCallback(null, deviceInfo);
          } else {
            innerCallback({errorId: response.retCode, errorMsg: response.description});
          }
        });
      },
      function (deviceInfo, innerCallback) {
        /*if (deviceInfo.type.id === HL_TYPE_ID) */
        {
          innerCallback(null, deviceInfo);
        }
        /*else {
         var msg = {
         devices: deviceInfo.owner,
         payload: {
         cmdName: "active_timer",
         cmdCode: "0006",
         parameters: {
         timerId: message.timerId,
         automatic: message.automatic
         }
         }
         };
         //innerCallback(null, deviceInfo);
         self.message(msg, function (response) {
         if (response.retCode !== 200) {
         innerCallback({errorId: response.retCode, errorMsg: response.description});
         } else {
         innerCallback(null, deviceInfo);
         }
         });
         }*/
      },
      function (deviceInfo, innerCallback) {
        if (util.isNullOrUndefined(deviceInfo.extra.timers)) {
          deviceInfo.extra.timers = [];
        }
        var index = _.findIndex(deviceInfo.extra.timers, function (timer) {
          return timer.timerId === message.timerId;
        });
        if (0 > index) {
          innerCallback({
            errorId: 209001,
            errorMsg: "no timer found by given uuid:[" + message.timerId + "]"
          });
          return;
        }
        var timer = deviceInfo.extra.timers[index];
        var automatic = message.automatic;
        if (!util.isNullOrUndefined(automatic.enable)) {
          //总开关，将同步到每个子命令状态
          timer.enable = automatic.enable;
          _.forEach(timer.commands, function (cmd) {
            cmd.enable = automatic.enable;
          })
        }
        else {
          _.forEach(automatic.commands, function (cmd) {
            _.findIndex(timer.commands, function (_cmd, _index) {
              if (_cmd.cmd.cmdCode === cmd.cmdCode) {
                if (util.isNullOrUndefined(cmd.index)) {
                  _cmd.enable = cmd.enable;
                  return true;
                }
                else if (_index === cmd.index) {
                  _cmd.enable = cmd.enable;
                  return true;
                }
              }
              return false;
            });
          });
        }
        //如果命令被全部禁用，那么整个定时器状态被禁用
        var foundIndex = _.findIndex(timer.commands, function (cmd) {
          return true === cmd.enable;
        });
        timer.enable = 0 <= foundIndex;
        //如果是单次执行的定时器
        if (util.isNullOrUndefined(timer.weekday) || _.isEmpty(timer.weekday)) {
          //并且是定时器整体激活
          if (!util.isNullOrUndefined(automatic.enable) && true === automatic.enable) {
            //重构schedule job
            self.parseTimer(deviceInfo.uuid, timer, function (error, jobs) {
              if (error) {
                logger.error(error.errorId, error.errorMsg);
                innerCallback(error)
              }
              else {
                self.deleteJobScheduleOfTimer(timer.timerId);
                self.jobSchedule[timer.timerId] = jobs;
                logger.debug()
              }
            });
          }
        }
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "deviceUpdate",
            cmdCode: "0004",
            parameters: {
              "uuid": deviceInfo.uuid,
              "extra.timers": deviceInfo.extra.timers
            }
          }
        };
        self.message(msg, function (response) {
          if (response.retCode !== 200) {
            innerCallback({errorId: response.retCode, errorMsg: response.description});
          } else {
            innerCallback(null);
          }
        });
      }
    ], function (error) {
      if (error) {
        responseMessage.retCode = error.errorId;
        responseMessage.description = error.errorMsg;
      }
      peerCallback(responseMessage);
    });
  });
};

module.exports = {
  Service: Timer,
  OperationSchemas: OPERATION_SCHEMAS
};