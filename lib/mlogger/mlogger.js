var util = require('util');
var log4js = require('log4js');
var errJSON = require('./error-code.json');
var errLangDefault = errJSON.getErrLang;
var allErrorCode = {};
var path = require('path');
var dirPath = path.join(__dirname, '../../log/');
var inited = false;
var showAll = function (errJSON) {
    for (var e in errJSON) {
        for (var ex in errJSON[e]) {
            if (ex === 'errors') {
                var errors = errJSON[e].errors;
                if (util.isArray(errors)) {
                    errors.forEach(function (error) {
                        allErrorCode[error.errorId] = error.errorMsg;
                    })
                }
            }
            if ('object' === typeof(errJSON[e][ex])) {
                showAll(errJSON[e][ex]);
            }
        }
    }
};

if(!inited){
    showAll(errJSON);
    inited = true;
}

function getErrorInfo(code, lang) {
    lang = lang || errLangDefault;
    if (!util.isNullOrUndefined(allErrorCode[code])) {
        return allErrorCode[code][lang];
    }
    return "unknown error.";
}
var LOG_CONFIG = {
    "appenders": {
        "Access": {
            "type": "dateFile",
            "filename": dirPath + "access.log",
            "pattern": "-yyyy-MM-dd"
        },
        "Console": {
            "type": "console"
        },
        "MeCloud": {
            "type": "dateFile",
            "filename": dirPath + "MeCloud_",
            "encoding": "utf-8",
            "maxLogSize": 10000000,
            "numBackups": 3,
            "pattern": "yyyy-MM-dd.log",
            "alwaysIncludePattern": true
        }
    },
    "categories": {
        "default": {
            "appenders": [
                "Console",
                "MeCloud"
            ],
            "level": "debug"
        },
        "http": {
            "appenders": [
                "Access"
            ],
            "level": "info"
        }
    }
};
log4js.configure(LOG_CONFIG);
var mLogger = log4js.getLogger("MeCloud");
var handleLog = function (info) {
    mLogger.log(info);
};
var handleTrace = function (info) {
    mLogger.trace(info);
};
var handleDebug = function (info) {
    mLogger.debug(info);
};
var handleInfo = function (info) {
    mLogger.info(info);
};
var handleWarn = function (info) {
    mLogger.warn(info);
};
var handleError = function (code, info) {
    var error = {
        errId: code,
        errMsg: getErrorInfo(code),
        detail: info
    };
    mLogger.error(error);
};
var handleException = function (code, info) {
    var error = {
        errId: code,
        errMsg: getErrorInfo(code),
        detail: info
    };
    mLogger.error(error);
};
module.exports = {
    getErrorInfo: getErrorInfo,
    log: handleLog,
    trace: handleTrace,
    debug: handleDebug,
    info: handleInfo,
    warn: handleWarn,
    error: handleError,
    exception: handleException
};