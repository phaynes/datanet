"use strict";

var fs = null; // NOT used by BROWSER

var ZBH, ZH;
if (typeof(exports) !== 'undefined') {
  fs     = require('fs');
  ZBH    = require('./zbase_helper');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOGGING -------------------------------------------------------------------

var LogBufferSize                = 16384;
var LogStaleness                 = 2000;
var LogUninitializedAgentTimeout = 10000;
LogUninitializedAgentTimeout = 30000; //TODO FIXME HACK (geo-cluster startup)
var LogStartTime                 = ZBH.GetMsTime();

var LogFD                    = null;
var LogBuffer                = '';

var LogCloseNext             = null;
var LogCloseWaitOnFlushSleep = 1000;

function close_log_post_flush() {
  if (FlushLogFileInProgress) {
    console.error('close_log_post_flush: FlushLogFileInProgress');
    setTimeout(close_log_post_flush, LogCloseWaitOnFlushSleep);
  } else {
    console.error('close_log_post_flush: CALLING NEXT()');
    LogCloseNext(null, null);
  }
}

exports.CloseLogFile = function(next) {
  if (!LogFD) return;
  do_flush_log(LogBuffer, function(ferr, fres) {
    if (ferr) console.error('LOG FILE CLOSE ERROR: ' + ferr);
    if (FlushLogFileInProgress) {
      console.error(LogBuffer);
      LogCloseNext = next;
      setTimeout(close_log_post_flush, LogCloseWaitOnFlushSleep);
    } else {
      fs.close(LogFD, function() { 
        console.error('LOG FILE CLOSED');
        LogFD = null;
        next(null, null);
      });
    }
  });
}

function get_log_file_name() {
  if      (ZH.ZyncRole === 'CENTRAL') return ZH.Central.Logfile;
  else if (ZH.ZyncRole === 'AGENT')   return ZH.Agent.Logfile;
  else                                 return null;
}

function do_flush_log(lbuf, next) {
  fs.write(LogFD, lbuf, undefined, undefined, function(werr, written) { 
    FlushLogFileInProgress = false;
    if (werr) {
      var path = get_log_file_name();
      var perr = 'ERROR FLUSH LOG: ' + path;
      next(perr, null);
    } else {
      next(null, null);
    }
  });
}

var FlushLogFileInProgress = false;
function flush_log_file(next) {
  if (FlushLogFileInProgress) return next(null, null);
  if (LogBuffer.length === 0) return next(null, null);
  FlushLogFileInProgress = true;
  var lbuf  = ZBH.clone(LogBuffer);
  LogBuffer = '';
  if (!fs) { // BROWSER
    console.log(lbuf);
    FlushLogFileInProgress = false;
    next(null, null);
  } else {
    if (LogFD) {
      do_flush_log(lbuf, next);
    } else {
      var path = get_log_file_name();
      if (!path) { // NOT INITED (e.g. Agent has not been assigned DEVICE-UUID)
        var now = ZBH.GetMsTime();
        if ((now - LogStartTime) < LogUninitializedAgentTimeout) { // NO FLUSH
          LogBuffer = lbuf;
        } else {                                                   // TO STDERR
          console.error("FLUSHING LOG TO STDERR");
          console.error(lbuf);
        }
        FlushLogFileInProgress = false;
        next(null, null);
      } else {
        console.error(ZH.MyUUID + ': OPEN: ' + path);
        fs.open(path, 'a', function(oerr, fd) {
          if (oerr) {
            FlushLogFileInProgress = false;
            var perr = 'Error opening Logfile: ' + path + ' ERROR: ' + oerr;
            next(perr, null);
          } else { 
            LogFD = fd;
            do_flush_log(lbuf, next);
          }
        });
      }
    }
  }
}

var AutoFlushLogSleep = 1000;
var AutoFlushLogTimer = null;
var LastLogTimeStamp  = null;

function auto_flush_log_file() {
  flush_log_file(function(ferr, fres) {
    AutoFlushLogTimer = null;
  });
  
}
function append_to_log_file(text) {
  if (AutoFlushLogTimer) {
    clearTimeout(AutoFlushLogTimer);
    AutoFlushLogTimer = null;
  }
  var next   = ZH.OnErrThrow;
  LogBuffer += text;
  var now    = Date.now();
  if (LogBuffer.length > LogBufferSize) flush_log_file(next);
  else if (LastLogTimeStamp) {
    if ((now - LastLogTimeStamp) >= LogStaleness) flush_log_file(next);
    else {
      setTimeout(auto_flush_log_file, AutoFlushLogSleep);
    }
  }
  LastLogTimeStamp = now;
}

function log_obj(json) {
  if (ZH.Debug) {
    var text = JSON.stringify(json, null, "    ") + "\n";
    if (ZH.LogToConsole) {
      console.error(text);
      return;
    } else {
      append_to_log_file(text);
    }
  }
}

function log_txt(text) {
  if (ZH.Debug) {
    if (ZH.LogToConsole) {
      console.error(text);
      return;
    } else {
      text += "\n";
      append_to_log_file(text);
    }
  }
}

function log_err(x) {
  var output;
  if (typeof(x) === "string") {
    output = ZH.MyUUID + ": " + x;
  } else {
    var text = JSON.stringify(x, null, "    ") + "\n";
    output   = ZH.MyUUID + ": " + text;
  }
  if (!ZH.LogToConsole) log_txt(output);
  console.error(output);
}

// LOG_LEVEL: 0:TRACE, 1:NOTICE, 2:ERROR
exports.LogLevel = 0;

exports.t = function(x) {
  if (exports.LogLevel == 0) {
    exports.l(x);
  }
}

exports.p = function(json) {
  if (exports.LogLevel != 2) {
    log_obj(json);
  }
}

exports.l = function(x) {
  if (exports.LogLevel != 2) {
    if (typeof(x) === "string") log_txt(x);
    else                        log_obj(x);
  }
}

exports.e = function(x) {
  log_err(x);
}

exports.SetLogLevel = function(level) {
  if      (level === "TRACE")  exports.LogLevel = 0;
  else if (level === "NOTICE") exports.LogLevel = 1;
  else if (level === "ERROR")  exports.LogLevel = 2;
  else {
    throw(new Error("SUPPORTED LOG-LEVELS: [TRACE, NOTICE, ERROR]"));
  }
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZLog']={} : exports);

