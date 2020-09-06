const nodemon = require('nodemon');
let args = "";
if(process.argv.length == 3) {
  args=process.argv[2];
}

nodemon({
  exec: 'node app.js '+args,
  ext: 'js',
  "events": {
   "restart": "npm install"
 }
});

nodemon.on('start', function () {
  console.log('Casa Corrently (MQTT) has started');
}).on('quit', function () {
  console.log('Casa Corrently (MQTT) has quit');
  process.exit();
}).on('restart', function (files) {
  console.log('Casa Corrently (MQTT) restarted due to: ', files);
});

const sdu = require('simple-dependencies-updater');
const updater = async function() {
  const fs = require("fs");
  const path = require('path');
  const fileExists = async path => !!(await fs.promises.stat(path).catch(e => false));
  if(await fileExists("./dependencies.json")) {
    sdu("./dependencies.json");
  }
}
setInterval(updater,4*3600000);
updater();
