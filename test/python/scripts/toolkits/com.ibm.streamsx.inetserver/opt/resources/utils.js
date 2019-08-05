localTimeString = function(seconds) { return new Date(seconds*1000).toLocaleTimeString(); };
localTimestampString = function(seconds) { return new Date(seconds*1000).toLocaleString(); };
localTimeMillisString = function(ms) { return new Date(ms).toLocaleString(); };

fmtLink = function(path) {
  return '<a href="' + path + '">' + path + '</a>';
}
