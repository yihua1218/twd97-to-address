const fs = require('fs');
const csv = require('csvtojson');
const NodeGeocoder = require('node-geocoder');
const twd97tolatlng = require('twd97-to-latlng');
const merge = require('merge');
const Json2csvParser = require('json2csv').Parser;


var options = {
  provider: 'google',
  httpAdapter: 'https',
  apiKey: process.env.GOOGLE_MAP_API_KEY,
  formatter: null,
};

const geocoder = NodeGeocoder(options);

const inputCsvFile = './data/BanqiaoPublicParkingSpaceInfo.csv';
const inputJsonFile = './data/BanqiaoPublicParkingSpaceInfo.json';
const convertedJsonFile = './data/BanqiaoPublicParkingSpaceInfoTwd97Converted.json';
const geocoderReversedJson = './data/geocoderReversed.json';
const geocoderReversedCsv = './data/geocoderReversed.csv';
const geocoderReverseCachePath = './data/geocoderReverse';


const parseCsv = (event) => {
  return new Promise((resolve, reject) => {
    const rows = [];
    if (fs.existsSync(event.inputJsonFile)) {
      const json = fs.readFileSync(event.inputJsonFile);
      event.rows = JSON.parse(json);
      resolve(event);
    } else {
      csv()
      .fromFile(event.inputCsvFile)
      .on('json', (obj) => {
        rows.push(obj);
      })
      .on('done', (error) => {
        if (error) {
          event.error = error;
          reject(event);
        } else {
          fs.writeFileSync(event.inputJsonFile, JSON.stringify(rows));
          event.rows = rows;
          resolve(event);
        }
      });  
    }
  });
};

const convertTwd97 = (event) => {
  return new Promise((resolve) => {
    const rows = [];
    if (fs.existsSync(event.convertedJsonFile)) {
      const json = fs.readFileSync(event.convertedJsonFile);
      event.rows = JSON.parse(json);
      resolve(event);
    } else {
      event.rows.forEach((row) => {
        const result = twd97tolatlng(row.TW97X, row.TW97Y);
        rows.push(merge(row, result));
        fs.writeFileSync(event.convertedJsonFile, JSON.stringify(rows));
        event.rows = rows;
        resolve(event);
      });
    }
  });
};

const geocoderReverse = (event) => {
  return new Promise((resolve) => {
    const lat = event.lat;
    const lng = event.lng;
    const cacheFile = `${event.geocoderReverseCachePath}/${lat}:${lng}.json`;
    if (fs.existsSync(cacheFile)) {
      const json = fs.readFileSync(cacheFile);
      event.results = JSON.parse(json);
      resolve(event);
    } else {
      const input = {
        lat: lat,
        lon: lng,
        language: 'zh-TW',
      };
      geocoder.reverse(input)
      .then((results) => {
        fs.writeFileSync(cacheFile, JSON.stringify(results));
        event.results = results;
        resolve(event);
      })
      .catch((error) => {
        event.error = error;
        resolve(event);
      });
    }
  });
};

const geocoderReverseAll = (event) => {
  const reversedFields = [
    'ID',
    'AREA',
    'NAME',
    'TYPE',
    'SUMMARY',
    'ADDRESS',
    'TEL',
    'PAYEX',
    'SERVICETIME',
    'TW97X',
    'TW97Y',
    'TOTALCAR',
    'TOTALMOTOR',
    'lat',
    'lng',
    'formattedAddress',
  ];

  const json2csvParser = new Json2csvParser({ fields: reversedFields });

  return new Promise((resolve) => {
    if (fs.existsSync(event.geocoderReversedJson)) {
      const json = fs.readFileSync(event.geocoderReversedJson);
      event.rows = JSON.parse(json);
      fs.writeFileSync(event.geocoderReversedCsv,
        JSON.stringify(json2csvParser.parse(event.rows)));
      resolve(event);
    } else {
      const promises = [];
      event.rows.forEach((row) => {
        row.geocoderReverseCachePath = event.geocoderReverseCachePath;
        const promise = geocoderReverse(row);
        promises.push(promise);
      });

      Promise.all(promises)
      .then((rows) => {
        const new_rows = [];
        rows.forEach((row) => {
          if (row.results && row.results[0] && row.results[0].formattedAddress) {
            row.formattedAddress = row.results[0].formattedAddress;  
          } else {
            console.log('row:', row);
          }
          
          const new_row = {};
          reversedFields.forEach((field) => {
            if (row[field]) {
              new_row[field] = row[field];
            }
          });
          new_rows.push(new_row);
        });
        fs.writeFileSync(event.geocoderReversedJson, JSON.stringify(new_rows));
        fs.writeFileSync(event.geocoderReversedCsv,
          JSON.stringify(json2csvParser.parse(new_rows)));
        event.rows = new_rows;
        resolve(event);
      });
    }
  });
};

const input = {
  inputCsvFile: inputCsvFile,
  inputJsonFile: inputJsonFile,
  convertedJsonFile: convertedJsonFile,
  geocoderReverseCachePath: geocoderReverseCachePath,
  geocoderReversedJson: geocoderReversedJson,
  geocoderReversedCsv: geocoderReversedCsv,
  rows: [],
};

parseCsv(input)
.then(convertTwd97)
.then(geocoderReverseAll)
.then((output) => {
  // console.log(output);
})
.catch((output) => {
  console.log(output);
});
