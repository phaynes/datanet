"use strict"

var fs   = require('fs');

var ZH   = require('./zhelper');
ZH.AmClient = true; // MUST PRECEDE require(zcli_core)
var ZCC  = require('./zcli_core');

ZH.NetworkDebug = false;
ZH.Debug        = false;
ZH.NetworkDebug = true; ZH.Debug = true; ZH.LogToConsole = true;

ZCC.Initialize();

var AgentIP    = process.argv[2];
var AgentPort  = process.argv[3];
var Namespace  = process.argv[4];
var Username   = process.argv[5];
var Password   = process.argv[6];
var CmdFile    = process.argv[7]; // [OPTIONAL]
var Options    = process.argv[8]; // [OPTIONAL] ... TODO extend (1+ options)


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGV[] --------------------------------------------------------------------

if (typeof(AgentIP) === 'undefined') {
  Usage('Agent-IP argument missing');
}
if (typeof(AgentPort) === 'undefined') {
  Usage('Agent-Port argument missing');
}
ZCC.SetAgentAddress(AgentIP, AgentPort);

if (typeof(Namespace) === 'undefined') {
  Usage('Namespace argument missing');
}

if (typeof(Username) === 'undefined') {
  Usage('Username argument missing');
}
if (typeof(Password) === 'undefined') {
  Usage('Password argument missing');
}

ZCC.FromFile = (typeof(CmdFile) !== 'undefined');

ZCC.Quiet    = false;
if (Options === '-q') ZCC.Quiet = true;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SIGNAL HANDLERS -----------------------------------------------------------

process.on('SIGALRM', function() {
  ZH.l('CENTRAL: Caught SIGALRM');
  if (ZH.Debug) ZH.Debug = false;
  else          ZH.Debug = true;
  console.warn('ZH.Debug: ' + ZH.Debug);
});


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// KEYPRESS ------------------------------------------------------------------

var keypress = require('keypress'), tty = require('tty');
keypress(process.stdin);

if (typeof process.stdin.setRawMode !== 'function') {
  Usage('Piped input not allowed');
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function Usage(err) {
  if (typeof(err) !== 'undefined') ZCC.WriteStderr('Error: ' + err);
  ZCC.WriteStderr('Usage: ' + process.argv[0] + ' ' + process.argv[1] +
               ' agent-ip agent-port namespace username password' + 
               ' [file] [options]');
  ZCC.Exit(-1);
}

function write_stdout_nl(txt) {
  if (exports.Quiet) return;
  process.stdout.write(txt + "\n");
}
ZCC.WriteResponse = write_stdout_nl;

function write_stdout(txt) {
  if (exports.Quiet) return;
  process.stdout.write(txt);
}
ZCC.WriteStdout   = write_stdout;
ZCC.WritePrompt   = write_stdout;

function exit(num) {
  process.exit(num);
}
ZCC.Exit = exit;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CURSOR HELPERS ------------------------------------------------------------

function reset_cursor(pos) {
  if (ZCC.Quiet) return;
  process.stdout.cursorTo(pos);
}
ZCC.ResetCursor = reset_cursor;

function rewrite_current_line() {
  if (ZCC.Quiet) return;
  var line = ZCC.GetCurrentDisplayHistory();
  rewrite_line(line);
}

function clear_cursor_line() {
  if (ZCC.Quiet) return;
  process.stdout.clearLine();          // clear current text
  process.stdout.cursorTo(0);          // move cursor to beginning of line
}

function rewrite_line(post_text) {
  if (ZCC.Quiet) return;
  clear_cursor_line();
  ZCC.PrintPrompt();                   // print prompt
  ZCC.WriteStdout(post_text);          // print post_text
}
ZCC.RewriteLine = rewrite_line;

function rewrite_newline(post_text) {
  if (ZCC.Quiet) return;
  rewrite_line(post_text);
  process.stdout.cursorTo(post_text.length + ZCC.Prompt.length);// cursor @ EOL
  ZCC.CursorPos = -1;                                           // normal input
}
ZCC.RewriteNewline = rewrite_newline;

function rewrite_mid_line(post_text) {
  if (ZCC.Quiet) return;
  rewrite_line(post_text);
  if (ZCC.CursorPos !== -1) process.stdout.cursorTo(ZCC.CursorPos);
}
ZCC.RewriteMidLine = rewrite_mid_line;

function reset_line() {
  if (ZCC.Quiet) return;
  clear_cursor_line();
  ZCC.PrintPrompt();
}
ZCC.ResetLine = reset_line;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// KEYSTROKE ACTIONS ---------------------------------------------------------

function parse_key_stroke(stroke) {
  var key = {is_up : false, is_down : false, is_left : false, is_right : false};
  if (stroke.key) {
    key.cstr = stroke.key.sequence;
    if      (stroke.key.name === 'up')    key.is_up    = true;
    else if (stroke.key.name === 'down')  key.is_down  = true;
    else if (stroke.key.name === 'left')  key.is_left  = true;
    else if (stroke.key.name === 'right') key.is_right = true;
  } else {
    key.cstr = stroke.ch;
  }
  key.ccode = key.cstr.charCodeAt(0);
  ZCC.ProcessKeyStroke(key);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STDIN EVENT LOOP ----------------------------------------------------------

ZCC.AlwaysPrintLine = true;
ZCC.PrintPrompt();
var line = 'NAMESPACE ' + Namespace;
ZCC.ProcessLine(line);

line = 'USER ' + Username + ' ' + Password;
ZCC.ProcessLine(line);
ZCC.AlwaysPrintLine = false;

if (ZCC.FromFile) {
  var flines = fs.readFileSync(CmdFile).toString().split("\n");
  for (var i = 0; i < flines.length; i++) {
    var line = flines[i];
    ZCC.ProcessLine(line);
    ZCC.ResetHistoryAndCursor(line);
  }
}

process.stdin.on('keypress', function (ch, key) {
  if (key && ((key.ctrl && key.name == 'c') || 
              (key.ctrl && key.name == 'd'))) {
    ZCC.WriteStdout("+BYE\n");
    ZCC.Exit(0);
  }
  var stroke = {key : key, ch : ch}
  parse_key_stroke(stroke);
});

process.stdin.setRawMode(true);
process.stdin.resume();

