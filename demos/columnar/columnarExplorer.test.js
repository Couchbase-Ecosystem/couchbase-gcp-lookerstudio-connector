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
    console.log('Query executed successfully:', response);
    return response;
  } catch (error) {
    console.error('Error executing query:', error.message);
    return null;
  }
}

// Try to list all collections
async function listAllCollections() {
  console.log('\nAttempting to list all collections:');
  // In Columnar, collections are within keyspaces
  return await executeQuery('SELECT * FROM system:all_keyspaces');
}

// Try a simple query on travel-sample data if it exists
async function tryTravelSampleQuery() {
  console.log('\nAttempting to query travel-sample data:');
  try {
    // Try different possible paths for travel-sample
    const queries = [
      'SELECT * FROM `travel-sample`.inventory.airline LIMIT 5',
      'SELECT * FROM travel_sample.inventory.airline LIMIT 5',
      'SELECT * FROM `travel-sample`.`inventory`.`airline` LIMIT 5'
    ];
    
    for (const query of queries) {
      console.log(`\nTrying query: ${query}`);
      try {
        const response = await columnarClient.submitRequest(query);
        console.log('Query successful! Sample airline data:');
        if (response.results && response.results.length > 0) {
          response.results.forEach((row, index) => {
            console.log(`[${index + 1}] ${JSON.stringify(row)}`);
          });
          return response;
        } else {
          console.log('No data returned.');
        }
      } catch (error) {
        console.error('Query failed:', error.message);
      }
    }
    
    console.log('All travel-sample queries failed. Travel-sample may not be loaded.');
    return null;
  } catch (error) {
    console.error('Error in tryTravelSampleQuery:', error.message);
    return null;
  }
}

// Get information about the Columnar service
async function getServiceInfo() {
  console.log('\nAttempting to get Columnar service information:');
  
  // Try different system keyspaces
  const systemQueries = [
    'SELECT * FROM system:datastores',
    'SELECT * FROM system:namespaces',
    'SELECT * FROM system:keyspaces',
    'SELECT * FROM system:dual'
  ];
  
  for (const query of systemQueries) {
    try {
      console.log(`\nTrying system query: ${query}`);
      const response = await columnarClient.submitRequest(query);
      console.log('System query successful:');
      if (response.results && response.results.length > 0) {
        response.results.forEach((row, index) => {
          console.log(`[${index + 1}] ${JSON.stringify(row)}`);
        });
      } else {
        console.log('No data returned.');
      }
    } catch (error) {
      console.error(`System query ${query} failed:`, error.message);
    }
  }
}

// Try to run a simple calculation query
async function testSimpleQuery() {
  console.log('\nTesting simple calculation query:');
  return await executeQuery('SELECT 1+1 AS sum');
}

// Main function to run the explorer
async function runExplorer() {
  console.log('Initializing Couchbase Columnar Explorer...');

  try {
    // First, test if we can run a simple query
    await testSimpleQuery();
    
    // Try to get system information
    await getServiceInfo();
    
    // Try to list all collections
    await listAllCollections();
    
    // Try to query travel-sample data
    await tryTravelSampleQuery();
    
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
  listAllCollections,
  tryTravelSampleQuery,
  getServiceInfo,
  testSimpleQuery,
  runExplorer
}; 