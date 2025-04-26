
const axios = require('axios');
const https = require('https');
require('dotenv').config(); // Load environment variables from .env file

// Configuration
const config = {
  // Base URL is now preferentially loaded from .env, otherwise uses the default.
  baseUrl: process.env.CB_COLUMNAR_URL || 'https://localhost:18095', // Adjusted default for local testing
  auth: {
    // Credentials MUST be loaded from the .env file
    username: process.env.CB_USERNAME,
    password: process.env.CB_PASSWORD
  },
  // Allow overriding SSL verification via environment variable
  // rejectUnauthorized: process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0' // Defaults to true unless overridden
  rejectUnauthorized: false // Explicitly disable SSL verification
};

// Create HTTPS agent for handling SSL verification
const httpsAgent = new https.Agent({
  rejectUnauthorized: config.rejectUnauthorized
});

// Create axios instance with authentication and the custom HTTPS agent
const api = axios.create({
  baseURL: config.baseUrl,
  auth: config.auth,
  headers: {
    'Content-Type': 'application/json'
  },
  timeout: 30000,
  httpsAgent
});

// ------ Main Functions for Listing Buckets, Scopes, Collections ------

// List all buckets using Analytics API
async function listBuckets() {
  try {
    console.log('Fetching all buckets...');
    const response = await api.post('/api/v1/request', {
      statement: "SELECT RAW b.name FROM Metadata.`Bucket` b ORDER BY b.name"
    });
    
    if (response.data && response.data.results) {
      console.log('Buckets:', response.data.results);
      return response.data.results;
    } else {
      console.log('No buckets found or unexpected response structure');
      return [];
    }
  } catch (error) {
    console.error('Error listing buckets:', error.response?.data || error.message);
    throw error;
  }
}

// List all scopes in a bucket
async function listScopes(bucketName) {
  try {
    console.log(`Fetching scopes for bucket: ${bucketName}...`);
    const response = await api.post('/api/v1/request', {
      statement: `SELECT RAW d.DataverseName 
                 FROM Metadata.\`Dataverse\` d 
                 WHERE REGEXP_CONTAINS(d.DataverseName, "^${bucketName}\\\\.")
                 ORDER BY d.DataverseName`
    });
    
    if (response.data && response.data.results) {
      // Format the scope names to remove bucket prefix
      const scopes = response.data.results.map(scope => {
        // Extract the scope name from the format "bucket.scope"
        const scopeName = scope.split('.')[1];
        return scopeName;
      });
      console.log(`Scopes in bucket ${bucketName}:`, scopes);
      return scopes;
    } else {
      console.log(`No scopes found in bucket ${bucketName} or unexpected response structure`);
      return [];
    }
  } catch (error) {
    console.error(`Error listing scopes for bucket ${bucketName}:`, error.response?.data || error.message);
    throw error;
  }
}

// List all collections in a bucket and scope
async function listCollections(bucketName, scopeName) {
  try {
    console.log(`Fetching collections for bucket: ${bucketName}, scope: ${scopeName}...`);
    const response = await api.post('/api/v1/request', {
      statement: `SELECT RAW ds.DatasetName
                 FROM Metadata.\`Dataset\` ds
                 WHERE ds.DataverseName = "${bucketName}.${scopeName}"
                 ORDER BY ds.DatasetName`
    });
    
    if (response.data && response.data.results) {
      console.log(`Collections in ${bucketName}.${scopeName}:`, response.data.results);
      return response.data.results;
    } else {
      console.log(`No collections found in ${bucketName}.${scopeName} or unexpected response structure`);
      return [];
    }
  } catch (error) {
    console.error(`Error listing collections for ${bucketName}.${scopeName}:`, error.response?.data || error.message);
    throw error;
  }
}

// Show the structure of all buckets, scopes, and collections in a tree view
async function showFullStructure() {
  try {
    console.log('\n----- Full Database Structure -----');
    const buckets = await listBuckets();
    
    for (const bucket of buckets) {
      console.log(`\nBucket: ${bucket}`);
      
      const scopes = await listScopes(bucket);
      
      for (const scope of scopes) {
        console.log(`  Scope: ${scope}`);
        
        const collections = await listCollections(bucket, scope);
        
        for (const collection of collections) {
          console.log(`    Collection: ${collection}`);
        }
      }
    }
    console.log('\n----- End of Database Structure -----');
  } catch (error) {
    console.error('Error displaying full structure:', error.message);
    throw error;
  }
}

// ------ Sample Data Retrieval Functions ------

// Fetch sample airlines from travel-sample bucket
async function getSampleAirlines(limit = 10) {
  try {
    console.log(`\nFetching ${limit} sample airlines...`);
    const response = await api.post('/api/v1/request', {
      statement: `SELECT airline.* 
                  FROM \`travel-sample\`.\`inventory\`.\`airline\` AS airline 
                  LIMIT ${limit}`
    });
    
    if (response.data && response.data.results) {
      console.log('Sample Airlines:', JSON.stringify(response.data.results, null, 2));
      return response.data.results;
    } else {
      console.log('No airlines found or unexpected response structure');
      return [];
    }
  } catch (error) {
    console.error('Error fetching sample airlines:', error.response?.data || error.message);
    throw error;
  }
}

// Fetch sample airports from travel-sample bucket
async function getSampleAirports(limit = 10) {
  try {
    console.log(`\nFetching ${limit} sample airports...`);
    const response = await api.post('/api/v1/request', {
      statement: `SELECT airport.* 
                  FROM \`travel-sample\`.\`inventory\`.\`airport\` AS airport 
                  LIMIT ${limit}`
    });
    
    if (response.data && response.data.results) {
      console.log('Sample Airports:', JSON.stringify(response.data.results, null, 2));
      return response.data.results;
    } else {
      console.log('No airports found or unexpected response structure');
      return [];
    }
  } catch (error) {
    console.error('Error fetching sample airports:', error.response?.data || error.message);
    throw error;
  }
}

// ------ Main Execution Function ------

async function runExplorer() {
  try {
    console.log('Couchbase Explorer initializing...');
    console.log(`Using connection URL: ${config.baseUrl}`);
    console.log(`Using username: ${config.auth.username}`);
    
    // First show the database structure
    await showFullStructure();
    
    // Then get sample data - comment out if only interested in structure
    await getSampleAirlines(10);
    await getSampleAirports(10);
    
    console.log('\nCouchbase Explorer completed successfully!');
  } catch (error) {
    console.error('Couchbase Explorer failed:', error.message);
    process.exitCode = 1;
  }
}

// Execute if running directly (not if being imported/required)
if (require.main === module) {
  runExplorer();
}

// Export the functions for use in other files
module.exports = {
  listBuckets,
  listScopes,
  listCollections,
  showFullStructure,
  getSampleAirlines,
  getSampleAirports,
  runExplorer
}; 