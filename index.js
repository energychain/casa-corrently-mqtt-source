const MQTT = require('async-mqtt');
const retentiondb = require("./simpleretention.js");

let _data = {};

let _connection = null;

const client = async function(node,topic,from,to) {
      if(_connection == null) {
          _connection = await MQTT.connectAsync(node.config.mqtturl);
          _connection.on('message', async function (topic, message) {
            console.log("Received message for ",topic);
            await _data[topic].push({
              energy:(message.toString())*1
            });
          });
          if(typeof node.config.mqtt_feedin_topic !== 'undefined') {
              await _connection.subscribe(node.config.mqtt_feedin_topic);
          }
          if(typeof node.config.mqtt_feedout_topic !== 'undefined') {
              await _connection.subscribe(node.config.mqtt_feedout_topic);
          }
          if(typeof node.config.mqtt_prod_topic_topic !== 'undefined') {
              await _connection.subscribe(node.config.mqtt_prod_topic_topic);
          }
      }
      if(typeof _data[topic] == "undefined") {
        _data[topic] = await retentiondb(topic);
      }
      return await _data[topic].retrieve(from,to);
}


module.exports = {
  last_reading: function(meterId,node) {
      return new Promise(async function (resolve, reject)  {
        let fieldin = node.config.mqtt_feedin_topic;
        let fieldout = node.config.mqtt_feedout_topic;

        if(meterId == 'mqtt_prod_topic') {
          fieldin = node.config.mqtt_prod_topic_topic;
          delete fieldout;
        }

        let scaleFactor = 1000*10000000;
        if((typeof node.config !== 'undefined') && (typeof node.config.scaleFactor !== 'undefined')) scaleFactor = node.config.scaleFactor;
        let energy_data = await client(node,fieldin);
        let energyout_data = await client(node,fieldout);
        if((typeof energy_data == "undefined")||(energy_data==null)) resolve({timeStamp:0,values:{energy:0,power:0,power1:0,power2:0,power3:0,energyOut:0}}); else {
          let responds = {
              time: energy_data.timeStamp,
              values: {
                energy: Math.round(energy_data.obj.energy * scaleFactor),
                energyOut: Math.round(energyout_data.obj.energy * scaleFactor)
              }
          };
          resolve(responds);
        }
    });
  },
  historicReading: async function(meterId,resolution,from,to,node) {
    return new Promise(async function (resolve, reject)  {
      if(typeof node.config == 'undefined') node.config = node;
      let scaleFactor = 1000*10000000;
      if(typeof node.config.scaleFactor !== 'undefined') scaleFactor = node.config.scaleFactor;
      let fieldin = node.config.mqtt_feedin_topic;
      let fieldout = node.config.mqtt_feedout_topic;

      if(meterId == 'mqtt_prod_topic') {
        fieldin = node.config.mqtt_prod_topic_topic;
      }
      let responds = [];

      if((typeof node.config !== 'undefined') && (typeof node.config.scaleFactor !== 'undefined')) scaleFactor = node.config.scaleFactor;
        let energy_data = await client(node,fieldin,from,to);
        if((typeof energy_data == 'undefined')||(energy_data == null) || (energy_data.length <1)||(typeof energy_data[0] == 'undefined')) resolve([]); else {
          responds.push({
              time: energy_data[0].timeStamp,
              values: {
                energy: Math.round(energy_data[0].obj.energy * scaleFactor),
                energyOut: Math.round(0 * scaleFactor)
              }
          });
          responds.push({
              time: energy_data[1].timeStamp,
              values: {
                energy: Math.round(energy_data[1].obj.energy  * scaleFactor),
                energyOut: Math.round(0 * scaleFactor)
              }
          });
          if(typeof fieldout !== 'undefined') {
              let energy_dataout = await client(node,fieldout,from,to);
              if(result.length < 1 ) {
                  resolve(responds);
              } else {
                  responds[0].values.energyOut = Math.round(energy_dataout[0].obj.energy * scaleFactor);
                  responds[1].values.energyOut = Math.round(energy_dataout[0].obj.energy * scaleFactor);
                  resolve(responds);
              }
          } else resolve(responds);
        }
    });
  },
  meters: async function(node) {
    let responds = [];
    responds.push({
      meterId:'mqtt_feedin_topic',
      firstMeasurementTime:0,
      location: {
        country: 'DE',
        zip: node.config.zip
      }
    });
    responds.push({
      meterId:'mqtt_feedout_topic',
      firstMeasurementTime:0,
      location: {
        country: 'DE',
        zip: node.config.zip
      }
    });
    responds.push({
      meterId:'mqtt_prod_topic',
      firstMeasurementTime:0,
      location: {
        country: 'DE',
        zip: node.config.zip
      }
    });
    return responds;
  }
};
