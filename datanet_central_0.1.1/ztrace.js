
var fs = require('fs');
var ZH = require('./zhelper');

var TraceEnabled = false;

// NOTE: zcli_DC1-1 throws error: "TypeError: Cannot redefine property: __stack"
if (TraceEnabled) {
  Object.defineProperty(global, '__stack', {
    get: function() {
      var orig = Error.prepareStackTrace;
      Error.prepareStackTrace = function(_, stack) {
          return stack;
      };
      var err   = new Error;
      Error.captureStackTrace(err, arguments.callee);
      var stack = err.stack;
      Error.prepareStackTrace = orig;
      return stack;
    }
  });
}

function get_trace_name(filename, funcname) {
  return filename + '|' + funcname;
}

var TraceTimes = {};
var TraceStartTimes = {};

function add_to_trace_times(tname, hrend) {
  if (!TraceTimes[tname]) TraceTimes[tname] = {num: 0, cnt: 0};
  var t  = TraceTimes[tname];
  t.num += 1;
  t.cnt += (hrend[0] * 1000000 + hrend[1]/1000); // microseconds
  //console.error('TRACE: ' + tname + ' avg: ' + (t.cnt / t.num));
}

exports.G = function() {
  if (TraceEnabled) {
    var tname = get_trace_name(__stack[1].getFileName(),
                               __stack[1].getFunctionName());
    if (TraceStartTimes[tname]) {
      TraceStartTimes[tname] = null; // NOTE: faster than delete()
      return null;
    }
    TraceStartTimes[tname] = process.hrtime();
    return tname;
  }
}

exports.X = function(tn) {
  if (TraceEnabled) {
    var tname = tn ? tn : get_trace_name(__stack[1].getFileName(),
                                         __stack[1].getFunctionName());
    if (!TraceStartTimes[tname]) return;
    var hrstart = TraceStartTimes[tname];
    TraceStartTimes[tname] = null; // NOTE: faster than delete()
    var hrend   = process.hrtime(hrstart);
    add_to_trace_times(tname, hrend);
  }
}

exports.GetTraceTimes = function() {
  if (TraceEnabled) return ZH.clone(TraceTimes);
  else              return null;
}

function dump_trace_times() {
  if (!ZH.AmCentral) return;
  var tt  = exports.GetTraceTimes();
  var tta = [];
  for (var tn in tt) {
    var t   = tt[tn];
    var avg = (t.cnt / t.num);
    tta.push({average : avg, name : tn, count : t.cnt, number : t.num});
  }
  tta.sort(ZH.CmpTraceAverage);
  var dname = '/tmp/ZYNC_DUMP_' + ZH.Central.role + '_' + ZH.MyUUID;
  ZH.e('DUMP: TRACE TIMES: ' + dname);
  var data  = JSON.stringify(tta);
  fs.writeFileSync(dname, data, 'utf8');
}

function cron_dump_trace_times(ts) {
  setTimeout(function() {
    dump_trace_times();
    cron_dump_trace_times(ts);
  }, ts);
}

var CronDumpSleep = 10000;
if (TraceEnabled) {
  cron_dump_trace_times(CronDumpSleep);
}


