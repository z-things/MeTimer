'use strict';
var _ = require('util');
var request = require('sync-request');
var fs = require('fs');
var path = require('path');
var config = require('./config.json');
var zookeeper = require('node-zookeeper-client');
var Service = require(config.self.module).Service;
var Configurator = require('./lib/configurator').Configurator;
var service = null;
var MeshbluSocketIO = require('meshblu');
var logger = require('./lib/mlogger/mlogger');
var serviceType = config.self.type;

var configurator = new Configurator(config.zookeeper.host, config.zookeeper.port);
configurator.init(function (error) {
    if (error) {
        logger.error(error.errorId, error.errorMsg);
        process.exit(1);
    }
    else {
        if (_.isNullOrUndefined(config.self)
            || _.isNullOrUndefined(config.self.uuid)
            || _.isNullOrUndefined(config.self.token)) {
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
                var resp = request('POST', url, opt);
                var body = resp.getBody('UTF-8');
                var deviceInfo = JSON.parse(body);
                config.self.uuid = deviceInfo.uuid;
                config.self.token = deviceInfo.token;
                var configPath = path.join(__dirname, './config.json');
                fs.writeFileSync(configPath, JSON.stringify(config));
            }
            catch (e) {
                logger.error(201004, e);
                process.exit(1);
            }
        }
        var zkPath = "/system/services/" + serviceType.name + "/cluster/" + config.self.uuid;
        if (!_.isNullOrUndefined(config.self)
            && !_.isNullOrUndefined(config.self.uuid)
            && !_.isNullOrUndefined(config.self.token)) {
            if (_.isNullOrUndefined(config.self.zkPath) || config.self.zkPath === "") {
                configurator.addConfNode(zkPath, function (error, path) {
                    if (error) {
                        logger.error(error.errorId, error.errorMsg);
                        process.exit(1);
                    }
                    else {
                        logger.debug(path);
                        config.self.zkPath = path;
                        configurator.addConfNode(path + "/uuid", config.self.uuid, function (error) {
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
                        if (!_.isNullOrUndefined(config.self.listen_port)) {
                            configurator.addConfNode(path + "/listen_port", config.self.listen_port, function (error) {
                                if (error) {
                                    logger.error(error.errorId, error.errorMsg);
                                }
                            });
                        }
                        if (!_.isNullOrUndefined(config.self.forward)) {
                            configurator.addConfNode(path + "/forward", config.self.forward, function (error) {
                                if (error) {
                                    logger.error(error.errorId, error.errorMsg);
                                }
                            });
                        }
                    }
                });
            }
        }

        var conx = new MeshbluSocketIO({
            protocol: 'ws',
            hostname: config.meshblu_server.host,
            port: config.meshblu_server.port,
            uuid: config.self.uuid,
            token: config.self.token,
            resolveSrv: false
        });

        conx.on('ready', function () {
            logger.info(serviceType.name + "[" + config.self.uuid + "] connected to M2M.\n");
            configurator.setConf(zkPath + "/online", "true", function (error) {
                if (error) {
                    logger.error(error.errorId, error.errorMsg);
                }
            });
            if (_.isNullOrUndefined(service)) {
                service = new Service(conx, config.self.uuid, config.self.token, configurator);
                if (!_.isNullOrUndefined(service.init) && _.isFunction(service.init)) {
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
    }
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
