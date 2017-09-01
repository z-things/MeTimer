/**
 * Created by jacky on 2015/9/17.
 */
var fs = require('fs');
var async = require('async');
var path = require('path');
//var mongoose = require('mongoose');
var errJSON = require('./error-code.json');

var errLangdefault = errJSON.getErrLang;
var dirPath = path.join(__dirname, '../../log');
var filePath = dirPath + "/M-Cloud.log";
var exceptionFilePath = dirPath + "/SystemUncaughtException.log";
if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath);
}
var logFileId = null;
var exceptionFileId = null;
var loggerSchema = {
    "timestamp": {"type": "date"},
    "message": {
        "type": "object",
        "properties": {
            "errId": {"type": "number"},
            "errMsg": {"type": "string"},
            "detail": {
                "type": "object",
                "properties": {
                    "info": {"type": "string"}
                }
            }
        }
    },
    "title": {"type": "string"},
    "level": {"type": "number"},
    "args": {
        "type": "object"
    },
    "file": {"type": "string"},
    "pos": {"type": "string"},
    "line": {"type": "string"},
    "path": {"type": "string"},
    "method": {"type": "string"},
    "stack": {"type": "string"}
};

var inited = false;
var allErrcode = [];
var showAll = function (errJSON) {
    inited = true;
    for (var e in errJSON) {
        for (var ex in errJSON[e]) {
            if (ex === 'errors') {
                allErrcode = allErrcode.concat(errJSON[e].errors);
            }
            if ('object' === typeof(errJSON[e][ex])) {
                showAll(errJSON[e][ex]);
            }
        }
    }
};
if (!inited) {
    showAll(errJSON);
}

slice = [].slice;
/*looger*/
var logger = require('tracer').colorConsole({
    level: 'debug',
    format: [
        "{{timestamp}} <{{title}}> (in {{file}}:{{line}}) ({{method}}) {{message}} ",
        {
            error: "{{timestamp}} <{{title}}> (in {{file}}:{{line}}) ({{method}}) {{message}} \nCall Stack:\n{{stack}}"
        }
    ],
    maxLength: 120,
    stackIndex: 1,
    dateformat: "yyyy-mm-dd HH:MM:ss",
    inspectOpt: {
        showHidden: false,
        depth: null
    },
    preprocess: function (data) {
        data.title = data.title.toUpperCase()
    },
    transport: function (data) {
        //if(data.level >= 4){
        //    var db = mongoose.createConnection(mongoUriString, mongoOptions);
        //    var Schema = mongoose.Schema(loggerSchema);
        //    var Model = db.model('MLogger', Schema);
        //    Model.create(data, function (err, result) {
        //        if (err) {
        //            console.log(err);
        //        }
        //        db.close();
        //    });
        //}
        var logInfo = data.output;
        console.log(logInfo);
        async.waterfall(
            [
                function (innerCallback) {
                    if (logFileId === null) {
                        console.log("fs.open:" + filePath);
                        fs.open(filePath, 'a', 0666, function (e, id) {
                            if (e) {
                                innerCallback(e);
                            }
                            else {
                                logFileId = id;
                                innerCallback(null);
                            }
                        });
                    }
                    else {
                        innerCallback(null);
                    }
                },
                function (innerCallback) {
                    try {
                        var logStat = fs.statSync(filePath);
                        if (logStat.size > 1024 * 1024) {
                            var curDate = new Date();
                            var dateString = curDate.toISOString();
                            dateString = dateString.replace(/-/g, "");
                            dateString = dateString.replace(/:/g, "");
                            dateString = dateString.replace(/ /g, "");
                            var newPath = filePath + "." + dateString;
                            if (logFileId) {
                                fs.close(logFileId, function () {
                                    logFileId = null;
                                    fs.rename(filePath, newPath, function(error){
                                        if(error){
                                            innerCallback(error);
                                        }
                                        else{
                                            fs.open(filePath, 'a', 0666, function (e, id) {
                                                if (e) {
                                                    innerCallback(e);
                                                }
                                                else {
                                                    logFileId = id;
                                                    innerCallback(null);
                                                }
                                            });
                                        }
                                    });
                               });
                            }
                            else {
                                fs.rename(filePath, newPath, function(error){
                                    if(error){
                                        innerCallback(error);
                                    }else{
                                        fs.open(filePath, 'a', 0666, function (e, id) {
                                            if (e) {
                                                innerCallback(e);
                                            }
                                            else {
                                                logFileId = id;
                                                innerCallback(null);
                                            }
                                        });
                                    }
                                });
                            }
                        }
                        else {
                            innerCallback(null);
                        }
                    } catch (e) {
                        innerCallback(e);
                    }
                }
            ],
            function (error) {
                if (error) {
                    console.log("-------------------------------------------------------");
                    console.error(error);
                    console.log("-------------------------------------------------------");
                    logFileId = null;
                }
                else {
                    try {
                        fs.write(logFileId, logInfo + "\n", null, 'utf8', function () {
                        });
                    } catch (error) {
                        console.log("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        console.error(error);
                        console.log("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        logFileId = null;
                    }
                }
            }
        );
    }
});

var sysLogger = require('tracer').colorConsole({
    level: 'debug',
    format: [
        "{{timestamp}} <{{title}}> (in {{file}}:{{line}}) ({{method}}) {{message}} ",
        {
            error: "{{timestamp}} <{{title}}> (in {{file}}:{{line}}) ({{method}}) {{message}} \nCall Stack:\n{{stack}}"
        }
    ],
    maxLength: 120,
    stackIndex: 1,
    dateformat: "yyyy-mm-dd HH:MM:ss",
    inspectOpt: {
        showHidden: false,
        depth: null
    },
    preprocess: function (data) {
        data.title = data.title.toUpperCase()
    },
    transport: function (data) {
        var logInfo = data.output;
        console.log(logInfo);
        async.waterfall(
            [
                function (innerCallback) {
                    if (exceptionFileId === null) {
                        console.log("fs.open:" + exceptionFilePath);
                        fs.open(exceptionFilePath, 'a', 0666, function (e, id) {
                            if (e) {
                                innerCallback(e);
                            }
                            else {
                                exceptionFileId = id;
                                innerCallback(null);
                            }
                        });
                    }
                    else {
                        innerCallback(null);
                    }
                },
                function (innerCallback) {
                    try {
                        var logStat = fs.statSync(exceptionFilePath);
                        if (logStat.size > 1024 * 1024) {
                            var curDate = new Date();
                            var dateString = curDate.toISOString();
                            dateString = dateString.replace(/-/g, "");
                            dateString = dateString.replace(/:/g, "");
                            dateString = dateString.replace(/ /g, "");
                            var newPath = exceptionFilePath + "." + dateString;
                            if (exceptionFileId) {
                                fs.close(exceptionFileId, function () {
                                    exceptionFileId = null;
                                    fs.rename(exceptionFilePath, newPath, function(error){
                                        if(error){
                                            innerCallback(error);
                                        }
                                        else{
                                            fs.open(exceptionFilePath, 'a', 0666, function (e, id) {
                                                if (e) {
                                                    innerCallback(e);
                                                }
                                                else {
                                                    exceptionFileId = id;
                                                    innerCallback(null);
                                                }
                                            });
                                        }
                                    });
                                });
                            }
                            else {
                                fs.rename(exceptionFilePath, newPath, function(error){
                                    if(error){
                                        innerCallback(error);
                                    }
                                    else{
                                        fs.open(exceptionFilePath, 'a', 0666, function (e, id) {
                                            if (e) {
                                                innerCallback(e);
                                            }
                                            else {
                                                exceptionFileId = id;
                                                innerCallback(null);
                                            }
                                        });
                                    }
                                });
                            }
                        }
                        else {
                            innerCallback(null);
                        }
                    } catch (e) {
                        innerCallback(e);
                    }
                }
            ],
            function (error) {
                if (error) {
                    console.log("-------------------------------------------------------");
                    console.error(error);
                    console.log("-------------------------------------------------------");
                    exceptionFileId = null;
                }
                else {
                    try {
                        fs.write(exceptionFileId, logInfo + "\n", null, 'utf8', function () {
                        });
                    } catch (error) {
                        console.log("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        console.error(error);
                        console.log("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        exceptionFileId = null;
                    }
                }
            }
        );
    }
});

/*helper*/
function getErrorInfo(code, lang) {
    lang = lang || errLangdefault;
    for (var i = 0; i < allErrcode.length; i++) {
        if (allErrcode[i].errorId === code) {
            return allErrcode[i].errorMsg[lang];
        }
    }
    return "unknown error.";
}

function handlerLog() {
    var logMsg;
    logMsg = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    logger.log(logMsg);
}

function handlerTraceLog() {
    var traceMsg;
    traceMsg = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    logger.trace(traceMsg);
}

function handlerDebugLog() {
    var debugMsg;
    debugMsg = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    logger.debug(debugMsg);
}
function handlerInfoLog() {
    var infoMsg;
    infoMsg = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    logger.info(infoMsg);
}

function handlerWarnLog() {
    var warnMsg;
    warnMsg = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    logger.warn(warnMsg);
}

function handlerErrorLog(errCode, detail) {
    var logError = {
        errId: errCode,
        errMsg: getErrorInfo(errCode),
        detail: detail
    };
    logger.error(logError);
}

function handlerExceptionLog(errCode, detail) {
    var logError = {
        errId: errCode,
        errMsg: getErrorInfo(errCode),
        detail: detail
    };
    //fs.open(exceptionFilePath, 'a', 0666, function (e, id) {
    //    if (e) {
    //        console.error(e);
    //    }
    //    else {
    //        fs.write(id, JSON.stringify(logError) + "\n", null, 'utf8', function () {
    //            fs.close(id);
    //        });
    //    }
    //});
    sysLogger.error(logError);
}

function emitErrorLog(errCode, errMsg) {
    var logError = {
        errorId: errCode,
        errorMsg: getErrorInfo(errCode) + "\n detail=" + errMsg
    };
    logger.error(logError);
    return logError;
}
function packError(errCode, errMsg) {
    var logError = {
        code: errCode,
        message: getErrorInfo(errCode) + "\n detail=" + errMsg
    };
    logger.error(logError);
    return logError;
}
function responseError(errCode, errMsg, peerCallback) {
    var responseMessage = {
        code: errCode,
        message: getErrorInfo(errCode) + "\n detail=" + errMsg
    };
    logger.error(responseMessage);
    if (peerCallback) {
        peerCallback({payload: responseMessage});
    }
}
/*exports*/
module.exports = {
    getErrorInfo: getErrorInfo,
    log: handlerLog,
    trace: handlerTraceLog,
    debug: handlerDebugLog,
    info: handlerInfoLog,
    warn: handlerWarnLog,
    error: handlerErrorLog,
    exception: handlerExceptionLog,
    emitError: emitErrorLog,
    respError: responseError,
    packError: packError
};
