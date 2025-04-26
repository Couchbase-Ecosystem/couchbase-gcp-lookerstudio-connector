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
    SELECT a.airline.country as country, COUNT(*) as airline_count 
    FROM \`travel-sample\`.inventory.airline a 
    WHERE a.airline.country IS NOT NULL 
    GROUP BY a.airline.country 
    ORDER BY COUNT(*) DESC 
    LIMIT 10
  `);
  displayResults(countriesResponse, 'Airlines by Country');
  
  // Find airports in United States
  const usAirportsResponse = await executeQuery(`
    SELECT a.airport.city, a.airport.airportname, a.airport.country
    FROM \`travel-sample\`.inventory.airport a
    WHERE a.airport.country = "United States"
    LIMIT 10
  `);
  displayResults(usAirportsResponse, 'US Airports');
  
  // Find routes from San Francisco
  const sfRoutesResponse = await executeQuery(`
    SELECT r.route.airline, r.route.sourceairport, r.route.destinationairport, r.route.stops
    FROM \`travel-sample\`.inventory.route r
    WHERE r.route.sourceairport = "SFO"
    LIMIT 10
  `);
  displayResults(sfRoutesResponse, 'Routes from SFO');
  
  // Find hotels in London
  const londonHotelsResponse = await executeQuery(`
    SELECT h.hotel.name, h.hotel.address, h.hotel.phone
    FROM \`travel-sample\`.inventory.hotel h
    WHERE h.hotel.city = "London"
    LIMIT 10
  `);
  displayResults(londonHotelsResponse, 'London Hotels');
}

// Try a more complex join query
async function runJoinQueries() {
  console.log('\n--- RUNNING JOIN QUERIES ---');
  
  // Join airports and routes to find destinations from San Francisco
  const sfoJoinResponse = await executeQuery(`
    SELECT a.airport.airportname as destination, 
           a.airport.city as destination_city,
           a.airport.country as destination_country,
           r.route.airline as airline,
           r.route.stops as stops
    FROM \`travel-sample\`.inventory.route r
    JOIN \`travel-sample\`.inventory.airport a
    ON r.route.destinationairport = a.airport.faa
    WHERE r.route.sourceairport = "SFO"
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
  exploreTravelSample,
  runAnalysisQueries,
  runJoinQueries,
  runExplorer
}; 