const md5 = require('md5');

module.exports = async function(keyname) {

      const retentions = [0,1000,60000,900000,3600000,86400000];
      let db = [];
      let protected = [];
      let first_start = 0;

      const fs = require("fs");
      const fileExists = async path => !!(await fs.promises.stat(path).catch(e => false));

      const persist = async function(dbidx) {
        console.log("Persisting DB ",dbidx);
        fs.writeFileSync("data_"+md5(keyname)+"_"+dbidx+".json",JSON.stringify(db[dbidx]));
      }

      return {
        push:function(obj) {
            let _obj = {
              timeStamp: new Date().getTime(),
              obj:obj
            }
            if(first_start == 0) first_start = _obj.timeStamp;

            for(let i=0;i<retentions.length;i++) {
                if(typeof db[i] == 'undefined') {
                  if(fs.existsSync("data_"+md5(keyname)+"_"+i+".json")) {
                    try {
                    let data = JSON.parse(fs.readFileSync("data_"+md5(keyname)+"_"+i+".json"));
                    db.push(data);
                    if(data[0].timeStamp < first_start ) first_start = data[0].timeStamp;
                  } catch(e) {
                    console.log(e);
                    db.push([]);
                  }
                  } else {
                    db.push([]);
                  }
                }
                if(typeof protected[i] == 'undefined') protected[i] = 0;
                db[i].push(_obj);
                if((i<retentions.length-1) && ((retentions[i+1]*2)< (_obj.timeStamp) - (db[i][0]).timeStamp)) {
                    db[i] = db[i].slice(1);
                }
                let rms = -1;
                for(let j=db[i].length-1;((j>0)&&(rms==-1));j--) {
                    if(_obj.timeStamp - db[i][j].timeStamp > retentions[i]) rms = j;
                }
                if((rms !== -1)&&(rms<db[i].length-2)&&(rms>protected[i])) {
                  protected[i]++;
                  db[i] = db[i].slice(0,rms);
                  db[i].push(_obj);
                  persist(i);
                }
            }
        },
        retrieve:function(from,to) {
          if((from < first_start)&&(from !== null)) {
            return [];
          }
          if((from == null) || (to == null)) {
            let selection = null;
            for(let x=0;x<db.length;x++) {
                if((typeof db[x] !== 'undefined')&&(db[x].length>0)) {
                  selection = db[x][db[x].length-1];
                }
            }
            return selection;
          }
          let delta = to-from;
          let best_start = {};
          let best_start_delta = delta;
          let best_end = {};
          let best_end_delta = delta;

          for(let i=0;i<retentions.length;i++) {
            if(db.length > i) {
                for(let j=0;j<db[i].length;j++) {
                  if(Math.abs(from-db[i][j].timeStamp) < best_start_delta) {
                    best_start_delta = Math.abs(from-db[i][j].timeStamp);
                    best_start = db[i][j];
                  }
                  if(Math.abs(to-db[i][j].timeStamp) < best_end_delta) {
                    best_end_delta = Math.abs(to-db[i][j].timeStamp);
                    best_end = db[i][j];
                  }
              }
            } else {
              console.log("DB Length Issue",i);
            }
          }
          if((typeof best_end.timeStamp == 'undefined') || (typeof best_start.timeStamp == 'undefined')) {
            return [];
          }
          let deltat = best_end.timeStamp - best_start.timeStamp;
          let delta_start = best_start.timeStamp - from;
          let delta_end = best_end.timeStamp - to;
          for (const [key, value] of Object.entries(best_start.obj)) {
            if(!isNaN(value)) {
                let deltav = (best_end.obj[key] - best_start.obj[key])/deltat;
                best_start.obj[key] += deltav * delta_start;
            }
          }
          for (const [key, value] of Object.entries(best_end.obj)) {
            if(!isNaN(value)) {
                let deltav = (best_end.obj[key] - best_start.obj[key])/deltat;
                best_end.obj[key] -= deltav * delta_end;
            }
          }
          console.log(best_start,best_end);
          return [best_start,best_end];
        }
    }
};
