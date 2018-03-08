'use strict';

var syncRequest = require('sync-request');
var _ = require('lodash');
var fs = require('fs');
var tv4 = require('tv4');
var path = require('path');
var util = require('util');
var config = require('./config');
var configSchema = require('./configSchema');
var Service = require(config.self.module).Service;
var Configurator = require('./lib/configurator').Configurator;
var MeshbluSocketIO = require('meshblu');
var logger = require('./lib/mlogger/mlogger');
var serviceType = config.self.type;
var service = null;
var valid = tv4.validate(config, configSchema);
if (!valid) {
  logger.debug(config);
  logger.error(200001, {
    message: tv4.error.message,
    dataPath: tv4.error.dataPath,
    schemaPath: tv4.error.schemaPath
  });
  process.exit(1);
}
var configurator = new Configurator(config.zookeeper.host, config.zookeeper.port);
configurator.init(function (error) {
  if (error) {
    logger.error(error.errorId, error.errorMsg);
    process.exit(1);
    return;
  }

  if (util.isNullOrUndefined(config.self)
    || util.isNullOrUndefined(config.self.uuid)
    || util.isNullOrUndefined(config.self.token)) {
    var url = "http://" + config.meshblu_server.host + ":" + config.meshblu_server.port + "/devices";
    var opt = {
      body: JSON.stringify({
        name: serviceType.name,
        type: serviceType,
        description: ""
      }),
      headers: {
        "Content-Type": "application/json"
      }
    };
    try {
      var resp = syncRequest('POST', url, opt);
      var body = resp.getBody('UTF-8');
      var deviceInfo = JSON.parse(body);
      config.self.uuid = deviceInfo.uuid;
      config.self.token = deviceInfo.token;
      var configPath = path.join(__dirname, './config.json');
      fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
    }
    catch (e) {
      logger.error(201004, e);
      process.exit(1);
      return;
    }
  }
  var zkPath = "/system/services/" + serviceType.name + "/cluster/" + config.self.uuid;
  if (!util.isNullOrUndefined(config.self)
    && !util.isNullOrUndefined(config.self.uuid)
    && !util.isNullOrUndefined(config.self.token)
    && _.isEmpty(config.self.zkPath)) {
    logger.debug(zkPath);
    config.self.zkPath = zkPath;
    var transaction = configurator.zkClient.transaction();
    transaction.create(zkPath)
      .create(zkPath + "/uuid", new Buffer(config.self.uuid))
      .create(zkPath + "/token", new Buffer(config.self.token))
      .create(zkPath + "/online", new Buffer("false"));
    if (!util.isNullOrUndefined(config.self.extra)) {
      var extra = config.self.extra;
      for (var item in extra) {
        var itemObj = extra[item];
        transaction.create(zkPath + "/" + item, new Buffer(JSON.stringify(itemObj)));
      }
    }
    transaction.commit(function (error, results) {
      if (error) {
        logger.error(202004, results);
        return;
      }
      logger.debug('Transaction completed.');
    })
    /*configurator.addConfNode(path + "/uuid", config.self.uuid, function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
    });
    configurator.addConfNode(path + "/token", config.self.token, function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
    });
    configurator.addConfNode(path + "/online", "false", function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
    });
    if (!util.isNullOrUndefined(config.self.listen_port)) {
      configurator.addConfNode(path + "/listen_port", config.self.listen_port, function (error) {
        if (error) {
          logger.error(error.errorId, error.errorMsg);
        }
      });
    }
    if (!util.isNullOrUndefined(config.self.forward)) {
      configurator.addConfNode(path + "/forward", config.self.forward, function (error) {
        if (error) {
          logger.error(error.errorId, error.errorMsg);
        }
      });
    }*/
  }

  var conx = new MeshbluSocketIO({
    protocol: 'ws',
    hostname: config.meshblu_server.host || 'localhost',
    port: config.meshblu_server.port || 3000,
    uuid: config.self.uuid,
    token: config.self.token,
    resolveSrv: false,
    bufferRate: config.meshblu_server.buffer_rate || 10
  });

  conx.on('ready', function () {
    conx.update({uuid: config.self.uuid, myToken: config.self.token});
    logger.info(serviceType.name + "[" + config.self.uuid + "] connected to M2M.\n");
    configurator.setConf(zkPath + "/online", "true", function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
    });
    if (util.isNullOrUndefined(service)) {
      service = new Service(conx, config.self.uuid, config.self.token, configurator);
      if (!util.isNullOrUndefined(service.init) && util.isFunction(service.init)) {
        service.init();
      }
    }
  });

  conx.on('notReady', function (error) {
    configurator.setConf(zkPath + "/online", "false", function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
    });
    logger.error(201001, {
      message: serviceType.name + "[" + config.self.deviceUuid + "] can not connected to M2M.",
      detail: error
    });
    logger.info("Try reconnect after 3 seconds.");
    setTimeout(conx.connect, 3000);
  });

  conx.on('error', function (error) {
    configurator.setConf(zkPath + "/online", "false", function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
    });
    logger.error(201002, error);
    logger.info("Try reconnect after 3 seconds.");
    setTimeout(conx.connect, 3000);
  });

  conx.on('message', function (message) {
    if (service) {
      service.onMessage(message);
    }
  });
  // Event triggered when device loses connection to skynet
  conx.on('disconnect', function (data) {
    configurator.setConf(zkPath + "/online", "false", function (error) {
      if (error) {
        logger.error(error.errorId, error.errorMsg);
      }
    });
    logger.error(201003, {errorMsg: serviceType.name + "[" + config.self.uuid + "] disconnected from M2M.\n"});
    logger.info("Try reconnect after 3 seconds.");
    setTimeout(conx.connect, 3000);
  });
  conx.connect();
});

/*
 * System UncaughtException
 * */
process.on('uncaughtException', function (exception) {
  logger.exception(200000, exception.stack);
  setTimeout(function () {
    process.exit(1);
  }, 3000);
});
