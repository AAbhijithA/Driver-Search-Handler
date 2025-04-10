const dotenv = require('dotenv');
const express = require('express');
const { MongoClient } = require('mongodb');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());
dotenv.config({ path : './.env' });

/**
 * Environment Variables
 * access them in the .env file
 */
const Port = process.env.PORT;
const Database = process.env.DB_NAME;
const Collection = process.env.COLLECTION_DL;
const mongoUri = process.env.MONGO_URI;
const boundaryDistance = 10000;
const freeStatus = "F";
const featureDBPath = process.env.SQLITE_DB;
const T = 0.3;
const MAX_DRIVERS = 10;
const smqEndpoint = process.env.SMQ_ENDPOINT;

/**
 * MongoDB Client is Connection Pooled
 * Pool Sizes in the .env file
 */
const mongoClient = new MongoClient(mongoUri, {
    minPoolSize : process.env.MONGO_MIN_POOL_SIZE,
    maxPoolSize : process.env.MONGO_MAX_POOL_SIZE,
    useNewUrlParser: true,
});

const db = new sqlite3.Database(featureDBPath, (err) => {
  if (err) return console.error(err.message);
  console.log('Connected to feature database.');
});

const createUserTableQuery = `
  CREATE TABLE IF NOT EXISTS user (
    user_id TEXT PRIMARY KEY,
    trust_rating REAL NOT NULL,
    cleanliness_rating REAL NOT NULL,
    punctuality REAL NOT NULL,
    cancelled_ride REAL NOT NULL,
    overall_rating REAL NOT NULL
  )
`;

const createDriverTableQuery = `
  CREATE TABLE IF NOT EXISTS driver (
    driver_id TEXT PRIMARY KEY,
    safety_rating REAL NOT NULL,
    cleanliness_rating REAL NOT NULL,
    punctuality REAL NOT NULL,
    cancelled_ride REAL NOT NULL,
    overall_rating REAL NOT NULL
  )
`;

db.run(createUserTableQuery, (err) => {
  if (err) return console.error('Error creating table:', err.message);
  console.log('User table ensured.');
});

db.run(createDriverTableQuery, (err) => {
  if (err) return console.error('Error creating table:', err.message);
  console.log('Driver table ensured.');
});

/**
 * Params : Latitude, Longitude
 * Makes the find part of the query for drivers in boundary distance
 * for driver searching
 */
function createDriverBoundaryQuery(lat, lon) {
    const qFind = {
        location: {
          $near: {
            $geometry: {
              type: "Point",
              coordinates: [lon, lat]
            },
            $maxDistance: boundaryDistance,
            $minDistance: 0
          }
        },
        status: freeStatus
      };
    return qFind;
}

/**
 * Params : driverIDs (list/array), userid
 * Query from features and find top drivers
 * from searched drivers
 */
async function topDriverList(driverIDs, userid) {
    let featureUser = []
    let featureDrivers = []
    db.all('SELECT * FROM user WHERE user_id = ?', [userid], (err, rows) => {
        if(err) {
            console.log(err);
        }
        rows.forEach((row) => {
          featureUser.push(row.trust_rating);
          featureUser.push(row.cleanliness_rating);
          featureUser.push(row.punctuality);
          featureUser.push(row.cancelled_ride);
          featureUser.push(row.overall_rating);
        })
    })
    for(let idx = 0; idx < driverIDs.length; idx++) {
        db.all('SELECT * FROM driver WHERE driver_id = ?', [driverIDs[idx]], (err, rows) => {
          if(err) {
              console.log(err);
          }
          rows.forEach((row) => {
            let feature = []
            feature.push(row.safety_rating);
            feature.push(row.cleanliness_rating);
            feature.push(row.punctuality);
            feature.push(row.cancelled_ride);
            feature.push(row.overall_rating);
            feature.push(row.trust_rating);
            featureDrivers.push(feature);
          })
        })
    }
    let filtered_driver_IDs = [];
    for(let idx = 0; idx < driverIDs.length; idx++) {
        if(filtered_driver_IDs.length == MAX_DRIVERS) {
            break;
        }
        let assign = true;
        for(let fidx = 0; fidx < featureUser.length; fidx++) {
            let fd = parseFloat(featureDrivers[idx][fidx]), fu = parseFloat(featureUser[fidx]);
            if(!(((fd + T) >= fu) && ((fd - T) <= fu))) {
                assign = false;
                break;
            }
        }
        if(assign === true) {
            filtered_driver_IDs.push(driverIDs[idx]);
        }
    }
    return filtered_driver_IDs;
}

/**
 * Params-Query : id, type
 * http://localhost:{port}/testAdd?id={id}&type={userType}
 * Adds a user / driver to the SQL table (for testing purposes)
 * If it's not available it won't be found in the filtering logic, add for testing purposes
 */
app.post('/testAdd', async (req, res) => {
    var id = String(req.query.id);
    var userType = String(req.query.type);

    if(userType === 'user') {
        try {
            query = `INSERT INTO user (user_id,trust_rating,cleanliness_rating,punctuality,cancelled_ride,overall_rating) VALUES (?, ?, ?, ?, ?, ?)`;
            db.run(query, [id, 4.5, 4.5, 4.5, 4.5, 4.5], function(err) {
                if(err) {
                    console.log('Add Failed');
                }
                else {
                    console.log('Added');
                }
            });
        
        }
        catch(err) {
            console.log(err);
            res.status(500).json({message : "Internal Server Error"});
            return
        }
    }
    else {
        try {
            query = `INSERT INTO driver (driver_id,safety_rating,cleanliness_rating,punctuality,cancelled_ride,overall_rating) VALUES (?, ?, ?, ?, ?, ?)`;
            db.run(query, [id, 4.5, 4.5, 4.5, 4.5, 4.5], function(err) {
                if(err) {
                    console.log('Add Failed');
                }
                else {
                    console.log('Added');
                }
            }); 
        }
        catch(err) {
            console.log(err);
            res.status(500).json({message : "Internal Server Error"});
            return
        }
    }
    res.status(200).json({message : "Added values"})
})

/**
 * Params-Query : UserID, Latitude, Longitude
 * http://localhost:{port}/driverSearch?userid={userid}&lat={lat}&lon={lon}
 * Connects to the MongoDB Client and updates (or) inserts new records
 * into LocationDB Database driverlocation collection
 */
app.post('/driverSearch', async (req, res) => {
    try {
        const Lat = parseFloat(req.query.lat), Lon = parseFloat(req.query.lon), userid = String(req.query.userid);
        const qFind = createDriverBoundaryQuery(Lat,Lon);
        const client = await mongoClient.connect();
        const db = client.db(Database);
        const dlCollection = db.collection(Collection);
        const result = await dlCollection.find(qFind).toArray();
        let driverIDs = []
        for(let idx = 0; idx < result.length; idx++) {
            driverIDs.push(result[idx].userID);
        }
        client.close();
        console.log(driverIDs);
        if(!result || Object.keys(result).length === 0) {
            throw new Error("No Drivers Found");
        }
        finalDrivers = await topDriverList(driverIDs, userid);
        console.log(finalDrivers);
        const payload = {
            userId : userid,
            driverIds : finalDrivers
        }

        axios.post(smqEndpoint, payload)
              .then(response => {console.log('Inserted in MQ Response:', response.data);})
              .catch(error => {console.error('Not added in MQ Error:', error.message);});
    }
    catch(err) {
        console.log(err);
        res.status(500).json({message : "Internal Server Error (or) Drivers not found"});
        return;
    }
    res.status(200).json({message : "Drivers found"});
    return;
})

app.listen(Port, () => {
    console.log("DSH listening @ " + Port);
})
