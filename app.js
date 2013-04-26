var Mongo = require('./lib/mongo.js'),
    MySQL = require('./lib/mysql.js'),
    Tailer = require('./lib/tailer.js'),
    fs = require('fs'),
    config = JSON.parse(fs.readFileSync(process.cwd() + '/config.json'));

//var heapdump = require('heapdump');
var app = {};
app.last_timestamp = 0;
app.refresh = (process.argv[2] && process.argv[2] === 'import'); // import all data
app.config = config;
app.mongo = new Mongo(app);
app.mysql = new MySQL(app, function () {
  app.tailer = new Tailer(app);
  if (app.refresh === true) {
    app.tailer.import(function () {
      app.tailer.start();
    });
  } else {
    app.tailer.start();
  }
});