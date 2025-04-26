const dotenv = require('dotenv');
const columnarClient = require('./columnarClient');

// Load variables from .env file
dotenv.config();

// Check if the columnarClient can access Couchbase
console.log(`Connecting to Couchbase at: ${columnarClient.getBaseUrl()}`);

// Function to execute an ad-hoc query and log the results
async function executeQuery(query) {
  console.log(`\nExecuting query: ${query}`);
  try {
    const response = await columnarClient.submitRequest(query);
    console.log('Query executed successfully');
    return response;
  } catch (error) {
    console.error('Error executing query:', error.message);
    return null;
  }
}

// Show query results in a more readable format
function displayResults(response, label = 'Results') {
  if (response && response.results && response.results.length > 0) {
    console.log(`\n${label}: (${response.results.length} items)`);
    response.results.forEach((row, index) => {
      console.log(`[${index + 1}] ${JSON.stringify(row)}`);
    });
    return true;
  } else if (response) {
    console.log(`\nNo results returned for this query.`);
    return false;
  } else {
    console.log(`\nQuery failed or returned null response.`);
    return false;
  }
}

// Function to list all buckets in Couchbase
async function listBuckets() {
  console.log('\n--- LISTING BUCKETS ---');
  try {
    const response = await executeQuery('SELECT DISTINCT ARRAY_AGG(DISTINCT REGEXP_REPLACE(keyspace_id, ":[^:]*$", ""))[0] AS bucket FROM system:keyspaces');
    
    if (response && response.results && response.results.length > 0) {
      console.log('\nBuckets:');
      const buckets = new Set();
      
      response.results.forEach(row => {
        if (row.bucket) {
          buckets.add(row.bucket);
        }
      });
      
      Array.from(buckets).sort().forEach((bucket, index) => {
        console.log(`[${index + 1}] ${bucket}`);
      });
      
      return Array.from(buckets);
    } else {
      console.log('No buckets found or query returned no results.');
      return [];
    }
  } catch (error) {
    console.error('Error listing buckets:', error.message);
    return [];
  }
}

// Function to list all scopes in a bucket
async function listScopes(bucketName) {
  console.log(`\n--- LISTING SCOPES IN BUCKET: ${bucketName} ---`);
  try {
    const response = await executeQuery(`SELECT DISTINCT ARRAY_AGG(DISTINCT REGEXP_REPLACE(REGEXP_REPLACE(keyspace_id, "${bucketName}:", ""), "\\.[^.]*$", ""))[0] AS scope FROM system:keyspaces WHERE keyspace_id LIKE "${bucketName}:%"`);
    
    if (response && response.results && response.results.length > 0) {
      console.log(`\nScopes in bucket ${bucketName}:`);
      const scopes = new Set();
      
      response.results.forEach(row => {
        if (row.scope) {
          scopes.add(row.scope);
        }
      });
      
      Array.from(scopes).sort().forEach((scope, index) => {
        console.log(`[${index + 1}] ${scope}`);
      });
      
      return Array.from(scopes);
    } else {
      console.log(`No scopes found in bucket ${bucketName} or query returned no results.`);
      return [];
    }
  } catch (error) {
    console.error('Error listing scopes:', error.message);
    return [];
  }
}

// Function to list all collections in a bucket and scope
async function listCollections(bucketName, scopeName) {
  console.log(`\n--- LISTING COLLECTIONS IN BUCKET: ${bucketName}, SCOPE: ${scopeName} ---`);
  try {
    const response = await executeQuery(`SELECT DISTINCT REGEXP_REPLACE(keyspace_id, "${bucketName}:${scopeName}\\.", "") AS collection FROM system:keyspaces WHERE keyspace_id LIKE "${bucketName}:${scopeName}.%"`);
    
    if (response && response.results && response.results.length > 0) {
      console.log(`\nCollections in bucket ${bucketName}, scope ${scopeName}:`);
      const collections = [];
      
      response.results.forEach(row => {
        if (row.collection) {
          collections.push(row.collection);
        }
      });
      
      collections.sort().forEach((collection, index) => {
        console.log(`[${index + 1}] ${collection}`);
      });
      
      return collections;
    } else {
      console.log(`No collections found in bucket ${bucketName}, scope ${scopeName} or query returned no results.`);
      return [];
    }
  } catch (error) {
    console.error('Error listing collections:', error.message);
    return [];
  }
}

// Function to explore database structure
async function exploreDatabaseStructure() {
  console.log('\n--- EXPLORING DATABASE STRUCTURE ---');
  
  // List all buckets
  const buckets = await listBuckets();
  
  if (buckets.length > 0) {
    // Choose a bucket to explore (travel-sample)
    const travelSampleBucket = buckets.find(bucket => bucket === 'travel-sample');
    
    if (travelSampleBucket) {
      // List scopes in the travel-sample bucket
      const scopes = await listScopes(travelSampleBucket);
      
      if (scopes.length > 0) {
        // Choose a scope to explore (inventory)
        const inventoryScope = scopes.find(scope => scope === 'inventory');
        
        if (inventoryScope) {
          // List collections in the travel-sample.inventory scope
          await listCollections(travelSampleBucket, inventoryScope);
        }
      }
    }
  }
}

// Explore the travel-sample bucket structure
async function exploreTravelSample() {
  console.log('\n--- EXPLORING TRAVEL SAMPLE DATA ---');
  
  // Get a list of airlines
  const airlinesResponse = await executeQuery('SELECT * FROM `travel-sample`.inventory.airline LIMIT 5');
  displayResults(airlinesResponse, 'Airlines');
  
  // Get a list of airports
  const airportsResponse = await executeQuery('SELECT * FROM `travel-sample`.inventory.airport LIMIT 5');
  displayResults(airportsResponse, 'Airports');
  
  // Get a list of hotels
  const hotelsResponse = await executeQuery('SELECT * FROM `travel-sample`.inventory.hotel LIMIT 5');
  displayResults(hotelsResponse, 'Hotels');
  
  // Get a list of routes
  const routesResponse = await executeQuery('SELECT * FROM `travel-sample`.inventory.route LIMIT 5');
  displayResults(routesResponse, 'Routes');
  
  // Get a list of landmarks
  const landmarksResponse = await executeQuery('SELECT * FROM `travel-sample`.inventory.landmark LIMIT 5');
  displayResults(landmarksResponse, 'Landmarks');
}

// Try some simple analysis queries
async function runAnalysisQueries() {
  console.log('\n--- RUNNING ANALYSIS QUERIES ---');
  
  // Get count of airlines by country
  const countriesResponse = await executeQuery(`
    SELECT country, COUNT(*) as airline_count 
    FROM \`travel-sample\`.inventory.airline 
    WHERE country IS NOT NULL 
    GROUP BY country 
    ORDER BY COUNT(*) DESC 
    LIMIT 10
  `);
  displayResults(countriesResponse, 'Airlines by Country');
  
  // Find airports in United States
  const usAirportsResponse = await executeQuery(`
    SELECT city, airportname, country
    FROM \`travel-sample\`.inventory.airport
    WHERE country = "United States"
    LIMIT 10
  `);
  displayResults(usAirportsResponse, 'US Airports');
  
  // Find routes from San Francisco
  const sfRoutesResponse = await executeQuery(`
    SELECT airline, sourceairport, destinationairport, stops
    FROM \`travel-sample\`.inventory.route
    WHERE sourceairport = "SFO"
    LIMIT 10
  `);
  displayResults(sfRoutesResponse, 'Routes from SFO');
  
  // Find hotels in London
  const londonHotelsResponse = await executeQuery(`
    SELECT name, address, phone
    FROM \`travel-sample\`.inventory.hotel
    WHERE city = "London"
    LIMIT 10
  `);
  displayResults(londonHotelsResponse, 'London Hotels');
}

// Try a more complex join query
async function runJoinQueries() {
  console.log('\n--- RUNNING JOIN QUERIES ---');
  
  // Join airports and routes to find destinations from San Francisco
  const sfoJoinResponse = await executeQuery(`
    SELECT a.airportname as destination, 
           a.city as destination_city,
           a.country as destination_country,
           r.airline as airline,
           r.stops as stops
    FROM \`travel-sample\`.inventory.route r
    JOIN \`travel-sample\`.inventory.airport a
    ON r.destinationairport = a.faa
    WHERE r.sourceairport = "SFO"
    LIMIT 10
  `);
  displayResults(sfoJoinResponse, 'Destinations from SFO with Airport Details');
}

// Main function to run the explorer
async function runExplorer() {
  console.log('Initializing Couchbase Columnar Explorer...');

  try {
    // Test basic connectivity with a simple query
    const testResponse = await executeQuery('SELECT 1+1 AS sum');
    if (testResponse) {
      console.log('Basic connectivity test passed. Sum =', testResponse.results[0].sum);
    } else {
      console.error('Basic connectivity test failed. Cannot continue.');
      return;
    }
    
    // Explore database structure - buckets, scopes, collections
    await exploreDatabaseStructure();
    
    // Explore the travel-sample bucket structure
    await exploreTravelSample();
    
    // Run some analysis queries
    await runAnalysisQueries();
    
    // Run some join queries
    await runJoinQueries();
    
    console.log('\nCouchbase Columnar Explorer complete!');
  } catch (error) {
    console.error('Error running explorer:', error.message);
  }
}

// Run the explorer if this file is executed directly
if (require.main === module) {
  runExplorer();
}

// Export functions for use in other files
module.exports = {
  executeQuery,
  displayResults,
  listBuckets,
  listScopes,
  listCollections,
  exploreDatabaseStructure,
  exploreTravelSample,
  runAnalysisQueries,
  runJoinQueries,
  runExplorer
}; 