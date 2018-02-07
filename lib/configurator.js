/**
 * Created by jacky on 2017/2/24.
 */
'use strict';
var logger = require('./mlogger/mlogger');
var _ = require('lodash');
var util = require('util');
var zookeeper = require('node-zookeeper-client');
var config = require('../config.json');
var path = require('path');
var fs = require('fs');
var async = require('async');

function Configurator(host, port) {
  this.host = host;
  this.port = port;
  this.zkClient = null;
  this.serviceMap = [];
  this.getZkNodeChildren = function (zkPath, watch, callback) {
    var self = this;
    if (util.isNullOrUndefined(callback)) {
      callback = watch;
      watch = null;
    }
    async.series([
      function (innerCallback) {
        var clientState = self.zkClient.getState();
        if (clientState.name !== "SYNC_CONNECTED") {
          self.zookeeperConnect(function (error) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null);
          });
        }
        else {
          innerCallback(null);
        }
      }
    ], function (error) {
      if (error) {
        callback(error);
      }
      else {
        self.zkClient.getChildren(zkPath,
          watch,
          function (error, children, stat) {
            error ? callback({
              errorId: 202002,
              errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
            }) : callback(null, zkPath, children);
          });
      }
    });
  };
  this.getZkNodeData = function (zkPath, watch, callback) {
    var self = this;
    if (util.isNullOrUndefined(callback)) {
      callback = watch;
      watch = null;
    }
    async.series([
      function (innerCallback) {
        var clientState = self.zkClient.getState();
        if (clientState.name !== "SYNC_CONNECTED") {
          self.zookeeperConnect(function (error) {
            if (error) {
              logger.error(error.errorId, error.errorMsg);
              innerCallback(error);
            }
            else {
              innerCallback(null);
            }
          });
        }
        else {
          innerCallback(null);
        }
      }
    ], function (error) {
      if (error) {
        callback(error);
      }
      else {
        self.zkClient.getData(zkPath,
          watch,
          function (error, data, stat) {
            if (error) {
              callback({
                errorId: 202002,
                errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
              });
            }
            else {
              var dataStr = data.toString('utf8');
              callback(null, zkPath, dataStr);
            }
          });
      }
    });
  };
  this.setZkNodeData = function (zkPath, data, callback) {
    var self = this;
    async.series([
      function (innerCallback) {
        var clientState = self.zkClient.getState();
        if (clientState.name !== "SYNC_CONNECTED") {
          self.zookeeperConnect(function (error) {
            if (error) {
              logger.error(error.errorId, error.errorMsg);
              innerCallback(error);
            }
            else {
              innerCallback(null);
            }
          });
        }
        else {
          innerCallback(null);
        }
      }
    ], function (error) {
      if (error) {
        callback(error);
      }
      else {
        self.zkClient.setData(zkPath, new Buffer(data), function (error, stat) {
          if (error) {
            callback({
              errorId: 202003,
              errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
            });
          }
          else {
            callback(null);
          }
        });
      }
    });

  };
  this.addZkNodeData = function (zkPath, data, callback) {
    var self = this;
    var dataBuffer = undefined;
    if (!util.isNullOrUndefined(data)) {
      dataBuffer = new Buffer(data);
    }
    async.series([
      function (innerCallback) {
        var clientState = self.zkClient.getState();
        if (clientState.name !== "SYNC_CONNECTED") {
          self.zookeeperConnect(function (error) {
            if (error) {
              logger.error(error.errorId, error.errorMsg);
              innerCallback(error);
            }
            else {
              innerCallback(null);
            }
          });
        }
        else {
          innerCallback(null);
        }
      }
    ], function (error) {
      if (error) {
        callback(error);
      }
      else {
        self.zkClient.create(zkPath, dataBuffer, zookeeper.CreateMode.PERSISTENT, function (error, path) {
          if (error) {
            callback({
              errorId: 202004,
              errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
            });
          }
          else {
            callback(null, path);
          }
        });
      }
    });
  };
  this.zookeeperConnect = function (callback) {
    var self = this;
    self.zkClient.on('connected', function () {
      logger.info("connect to zookeeper");
      callback(null);
    });
    self.zkClient.on('connectedReadOnly', function () {
      logger.info("connect to zookeeper(read only)");
      callback(null);
    });

    self.zkClient.on('authenticationFailed', function () {
      var logMsg = {
        errorId: 202001,
        errorMsg: "connect zookeeper"
      };
      logger.error(202001, "connect zookeeper failed.");
      callback(logMsg);
    });
    self.zkClient.connect();
  };
  this.syncMeshbluServer = function (callback) {
    var self = this;
    //sync meshblu_server
    async.series({
        db_url: function (innerCallback) {
          self.getZkNodeData("/system/meshblu_server/db_url", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, data);
          });
        },
        host: function (innerCallback) {
          self.getZkNodeData("/system/meshblu_server/host", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, data);
          });
        },
        port: function (innerCallback) {
          self.getZkNodeData("/system/meshblu_server/port", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, parseInt(data));
          });
        },
        message_timeout: function (innerCallback) {
          self.getZkNodeData("/system/meshblu_server/message_timeout", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, parseInt(data));
          });
        }
      },
      function (error, result) {
        if (error) {
          callback(error);
        }
        else {
          config.meshblu_server = result;
          var configPath = path.join(__dirname, '../config.json');
          fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
          callback(null, result);
        }
      }
    );
  };
  this.syncNoderedServer = function (callback) {
    var self = this;
    //sync nodered_server
    async.series({
        host: function (innerCallback) {
          self.getZkNodeData("/system/nodered_server/host", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, data);
          });
        },
        port: function (innerCallback) {
          self.getZkNodeData("/system/nodered_server/port", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, parseInt(data));
          });
        },
        user: function (innerCallback) {
          self.getZkNodeData("/system/nodered_server/user", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, data);
          });
        },
        password: function (innerCallback) {
          self.getZkNodeData("/system/nodered_server/password", function (error, path, data) {
            error && logger.error(error.errorId, error.errorMsg);
            error ? innerCallback(error) : innerCallback(null, data);
          });
        }
      },
      function (error, result) {
        if (error) {
          callback(error);
        }
        else {
          config.nodered_server = result;
          var configPath = path.join(__dirname, '../config.json');
          fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
          callback(null, result);
        }
      });
  };
  this.updateServices = function (zkPath, serviceName, children) {
    var self = this;
    var updater = function (error, zkPath, data) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
      else if (zkPath && data) {
        logger.debug(zkPath + "/" + data);
        var pathNodes = zkPath.split("/");
        if (pathNodes.length > 0) {
          var uuid = pathNodes[pathNodes.length - 2];
          if (util.isNullOrUndefined(config.services[serviceName])) {
            config.services[serviceName] = [];
          }
          var services = config.services[serviceName];
          var found = false;
          for (var i = 0, len = services.length; i < len; ++i) {
            if (services[i].uuid === uuid) {
              services[i].online = data;
              found = true;
            }
          }
          !found && services.push({uuid: uuid, online: data});
          fs.writeFileSync(path.join(__dirname, '../config.json'), JSON.stringify(config, null, 2));
        }
      }
    };
    var watcher = function (event) {
      console.log(event);
      self.getZkNodeData(event.path, watcher, updater);
    };

    for (var i = 0, lenChildren = children.length; i < lenChildren; ++i) {
      self.getZkNodeData(zkPath + "/" + children[i] + "/online", watcher, updater);
    }
  };
  this.syncServices = function (callback) {
    var self = this;
    var serviceNumber = 0;
    //sync services
    var updater = function (error, zkPath, data) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
      else if (zkPath && data) {
        var IDs = data;
        if (util.isNullOrUndefined(config.services)) {
          config.services = {};
        }
        if (util.isArray(IDs)) {
          var mapItem = _.find(self.serviceMap, function (mapItem) {
            return zkPath === mapItem.zkPath;
          });
          var services = config.services[mapItem.name];
          if (!util.isNullOrUndefined(services)) {
            var tempServices = [];
            _.forEach(IDs, function (ID) {
              var service = _.find(services, function (service) {
                return service.uuid === ID;
              });
              tempServices.push(service)
            });
            services = tempServices;
            var configPath = path.join(__dirname, '../config.json');
            fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
          }
          self.updateServices(zkPath, mapItem.name, IDs);
        }
      }
      if (--serviceNumber === 0) {
        callback(null, config.services);
      }
    };
    var watcher = function (event) {
      console.log(event);
      self.getZkNodeChildren(event.path, watcher, updater);
    };
    var servicesPath = "/system/services";
    self.getZkNodeChildren(servicesPath, function (error, path, data) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
      else if (util.isArray(data)) {
        serviceNumber = data.length;
        _.forEach(data, function (item) {
          var serviceClusterPath = servicesPath + "/" + item + "/cluster";
          self.serviceMap.push({name: item, zkPath: serviceClusterPath});
          self.getZkNodeChildren(serviceClusterPath, watcher, updater);
        });
      }
    });
  };
  this.init = function (callback) {
    var self = this;
    self.zkClient = zookeeper.createClient(self.host + ':' + self.port);
    self.zookeeperConnect(function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
        callback(error);
      }
      else {
        async.series([
          function (innerCallback) {
            self.syncMeshbluServer(function (error, result) {
              error ? innerCallback(error) : innerCallback(null, result);
            })
          },
          function (innerCallback) {
            self.syncNoderedServer(function (error, result) {
              error ? innerCallback(error) : innerCallback(null, result);
            })
          },
          function (innerCallback) {
            self.syncServices(function (error, result) {
              error ? innerCallback(error) : innerCallback(null, result);
            })
          }
        ], function (error, result) {
          error ? callback(error) : callback(null, result);
        });
      }
    });
  };
}
Configurator.prototype.setConf = function (path, data, callback) {
  var self = this;
  if (util.isNullOrUndefined(callback) && util.isFunction(data)) {
    callback = data;
    data = null;
  }
  self.setZkNodeData(path, data, callback);
};
Configurator.prototype.addConfNode = function (path, data, callback) {
  var self = this;
  if (util.isNullOrUndefined(callback) && util.isFunction(data)) {
    callback = data;
    data = null;
  }
  self.addZkNodeData(path, data, callback);
};
Configurator.prototype.getConf = function (path) {
  var result = require('../config.json');
  var arrayNode = path.split(".");
  for (var i = 0, len = arrayNode.length; i < len; i++) {
    if (!util.isNullOrUndefined(result[arrayNode[i]])) {
      result = result[arrayNode[i]];
    }
    else {
      result = null;
      break;
    }
  }
  return result;
};
Configurator.prototype.getConfRandom = function (path) {
  var self = this;
  var result = self.getConf(path);
  if (util.isArray(result) && result.length > 0) {
    var availableServices = [];
    for (var i = 0, len = result.length; i < len; ++i) {
      if (result[i].online === "true") {
        availableServices.push(result[i]);
      }
    }
    if (availableServices.length <= 0) {
      result = null;
    }
    else {
      var index = Math.ceil(Math.random() * (availableServices.length)) - 1;
      result = availableServices[index].uuid;
    }
  }
  return result;
};
module.exports = {
  Configurator: Configurator
};