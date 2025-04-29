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

// Function to check if System.Metadata exists and is accessible
async function checkMetadataAccess() {
  console.log('\n--- CHECKING METADATA ACCESS ---');
  try {
    // Try accessing System.Metadata.Dataset
    const response = await executeQuery('SELECT COUNT(*) as count FROM System.Metadata.`Dataset` LIMIT 1');
    
    if (response && response.results && response.results.length > 0) {
      console.log('✅ System.Metadata is accessible');
      return true;
    } else {
      console.log('⚠️ Unable to access System.Metadata');
      return false;
    }
  } catch (error) {
    console.error('❌ Error accessing System.Metadata:', error.message);
    return false;
  }
}

// Function to list all databases
async function listDatabases() {
  console.log('\n--- LISTING DATABASES ---');
  try {
    // First try using System.Metadata approach
    const response = await executeQuery('SELECT DISTINCT DatabaseName FROM System.Metadata.`Dataset`');
    
    if (response && response.results && response.results.length > 0) {
      console.log('\nDatabases:');
      const databases = new Set();
      
      response.results.forEach(row => {
        if (row.DatabaseName) {
          databases.add(row.DatabaseName);
        }
      });
      
      Array.from(databases).sort().forEach((database, index) => {
        console.log(`[${index + 1}] ${database}`);
      });
      
      return Array.from(databases);
    } else {
      console.log('No databases found using System.Metadata - falling back to legacy approach');
      // Fall back to the legacy approach
      return listBucketsLegacy();
    }
  } catch (error) {
    console.error('Error listing databases:', error.message);
    console.log('Falling back to legacy approach');
    return listBucketsLegacy();
  }
}

// Legacy function to list all buckets in Couchbase (as a fallback)
async function listBucketsLegacy() {
  console.log('\n--- LISTING BUCKETS (LEGACY METHOD) ---');
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

// Function to list all scopes in a database/bucket
async function listScopes(databaseName) {
  console.log(`\n--- LISTING SCOPES IN DATABASE: ${databaseName} ---`);
  try {
    // First try using System.Metadata approach
    const response = await executeQuery(`SELECT DISTINCT DataverseName FROM System.Metadata.\`Dataset\` WHERE DatabaseName = "${databaseName}"`);
    
    if (response && response.results && response.results.length > 0) {
      console.log(`\nScopes in database ${databaseName}:`);
      const scopes = new Set();
      
      response.results.forEach(row => {
        if (row.DataverseName) {
          scopes.add(row.DataverseName);
        }
      });
      
      Array.from(scopes).sort().forEach((scope, index) => {
        console.log(`[${index + 1}] ${scope}`);
      });
      
      return Array.from(scopes);
    } else {
      console.log(`No scopes found in database ${databaseName} using System.Metadata - falling back to legacy approach`);
      // Fall back to the legacy approach
      return listScopesLegacy(databaseName);
    }
  } catch (error) {
    console.error('Error listing scopes:', error.message);
    console.log('Falling back to legacy approach');
    return listScopesLegacy(databaseName);
  }
}

// Legacy function to list all scopes in a bucket (as a fallback)
async function listScopesLegacy(bucketName) {
  console.log(`\n--- LISTING SCOPES IN BUCKET: ${bucketName} (LEGACY METHOD) ---`);
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

// Function to list all collections in a database, scope
async function listCollections(databaseName, scopeName) {
  console.log(`\n--- LISTING COLLECTIONS IN DATABASE: ${databaseName}, SCOPE: ${scopeName} ---`);
  try {
    // First try using System.Metadata approach
    const response = await executeQuery(`SELECT DatasetName FROM System.Metadata.\`Dataset\` WHERE DatabaseName = "${databaseName}" AND DataverseName = "${scopeName}"`);
    
    if (response && response.results && response.results.length > 0) {
      console.log(`\nCollections in database ${databaseName}, scope ${scopeName}:`);
      const collections = [];
      
      response.results.forEach(row => {
        if (row.DatasetName) {
          collections.push(row.DatasetName);
        }
      });
      
      collections.sort().forEach((collection, index) => {
        console.log(`[${index + 1}] ${collection}`);
      });
      
      return collections;
    } else {
      console.log(`No collections found in database ${databaseName}, scope ${scopeName} using System.Metadata - falling back to legacy approach`);
      // Fall back to the legacy approach
      return listCollectionsLegacy(databaseName, scopeName);
    }
  } catch (error) {
    console.error('Error listing collections:', error.message);
    console.log('Falling back to legacy approach');
    return listCollectionsLegacy(databaseName, scopeName);
  }
}

// Legacy function to list all collections in a bucket and scope (as a fallback)
async function listCollectionsLegacy(bucketName, scopeName) {
  console.log(`\n--- LISTING COLLECTIONS IN BUCKET: ${bucketName}, SCOPE: ${scopeName} (LEGACY METHOD) ---`);
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

// Function to get schema information for a collection by inference
async function getCollectionSchemaByInference(databaseName, scopeName, collectionName) {
  console.log(`\n--- INFERRING SCHEMA FOR: ${databaseName}.${scopeName}.${collectionName} ---`);
  try {
    // Try to get one row to infer schema
    const response = await executeQuery(`SELECT * FROM \`${databaseName}\`.\`${scopeName}\`.\`${collectionName}\` LIMIT 1`);
    
    if (response && response.results && response.results.length > 0) {
      const sampleRow = response.results[0];
      console.log(`\nInferred schema fields for ${databaseName}.${scopeName}.${collectionName}:`);
      
      const schema = {};
      Object.keys(sampleRow).sort().forEach((field, index) => {
        const value = sampleRow[field];
        const type = value === null ? 'null' : typeof value;
        console.log(`[${index + 1}] ${field}: ${type}`);
        schema[field] = type; // Store field and type
      });
      
      return schema;
    } else {
      console.log(`No data found in ${databaseName}.${scopeName}.${collectionName} to infer schema.`);
      return null;
    }
  } catch (error) {
    console.error('Error inferring collection schema:', error.message);
    return null;
  }
}

// Function to get schema information for a collection from Metadata
async function getCollectionSchemaFromMetadata(databaseName, scopeName, collectionName) {
  console.log(`\n--- GETTING SCHEMA FROM METADATA FOR: ${databaseName}.${scopeName}.${collectionName} ---`);
  try {
    // Query System.Metadata to get schema information
    const response = await executeQuery(`
      SELECT FieldName, DataType
      FROM System.Metadata.\`Dataset\` d
      WHERE d.DatabaseName = "${databaseName}"
        AND d.DataverseName = "${scopeName}"
        AND d.DatasetName = "${collectionName}"
    `);
    
    if (response && response.results && response.results.length > 0) {
      const fields = response.results;
      console.log(`\nSchema fields for ${databaseName}.${scopeName}.${collectionName}:`);
      
      if (fields.length > 0) {
        // Return a schema object { fieldName: dataType } for comparison
        const schema = {};
        fields.forEach(field => {
          schema[field.FieldName] = field.DataType;
        });
        return schema;
      } else {
        console.log(`Metadata found for ${databaseName}.${scopeName}.${collectionName}, but no Fields array present.`);
        return null;
      }
    } else {
      console.log(`No schema information found in System.Metadata for ${databaseName}.${scopeName}.${collectionName}.`);
      // Optionally, you could fall back to the inference method here
      return null;
    }
  } catch (error) {
    console.error('Error getting collection schema from Metadata:', error.message);
    // Optionally, you could fall back to the inference method here
    return null;
  }
}

// Function to compare schema results from inference and metadata
async function compareSchemaMethods(databaseName, scopeName, collectionName) {
  console.log(`\n--- COMPARING SCHEMA METHODS for ${databaseName}.${scopeName}.${collectionName} ---`);

  const inferredSchema = await getCollectionSchemaByInference(databaseName, scopeName, collectionName);
  const metadataSchema = await getCollectionSchemaFromMetadata(databaseName, scopeName, collectionName);

  if (!inferredSchema && !metadataSchema) {
    console.log('Both schema methods failed or returned no results.');
    return;
  }
  if (!inferredSchema) {
    console.log('Schema inference failed or returned no results.');
    // Optionally display metadata schema here
    return;
  }
  if (!metadataSchema) {
    console.log('Schema retrieval from metadata failed or returned no results.');
    // Optionally display inferred schema here
    return;
  }

  const inferredFields = Object.keys(inferredSchema).sort();
  const metadataFields = Object.keys(metadataSchema).sort();

  console.log('\nComparison Results:');
  let differences = false;

  // Check fields present in inference but not metadata
  const onlyInferred = inferredFields.filter(f => !metadataSchema.hasOwnProperty(f));
  if (onlyInferred.length > 0) {
    console.log('  Fields only in Inferred Schema:', onlyInferred.join(', '));
    differences = true;
  }

  // Check fields present in metadata but not inference
  const onlyMetadata = metadataFields.filter(f => !inferredSchema.hasOwnProperty(f));
  if (onlyMetadata.length > 0) {
    console.log('  Fields only in Metadata Schema:', onlyMetadata.join(', '));
    differences = true;
  }

  // Compare types for common fields
  const commonFields = inferredFields.filter(f => metadataSchema.hasOwnProperty(f));
  for (const field of commonFields) {
    const inferredType = inferredSchema[field];
    // Note: Metadata types might be more specific (e.g., BIGINT vs number)
    // Basic comparison for now
    const metadataType = metadataSchema[field]; // Assuming direct type string
    if (inferredType.toLowerCase() !== metadataType.toLowerCase()) {
        // Simple type comparison - might need refinement (e.g., number vs INT, BIGINT)
       if (!(inferredType === 'number' && ['int', 'bigint', 'double', 'float'].includes(metadataType.toLowerCase()))) {
          console.log(`  Type mismatch for field '${field}': Inferred='${inferredType}', Metadata='${metadataType}'`);
          differences = true;
       }
    }
  }

  if (!differences) {
    console.log('  Schemas appear consistent (based on field names and basic type comparison).');
  }
}

// Function to explore database structure
async function exploreDatabaseStructure() {
  console.log('\n--- EXPLORING DATABASE STRUCTURE ---');
  
  // Check if System.Metadata is accessible
  const hasMetadata = await checkMetadataAccess();
  console.log(`Using System.Metadata: ${hasMetadata ? 'Yes' : 'No (using legacy method)'}`);
  
  // List all databases
  const databases = await listDatabases();
  
  if (databases.length > 0) {
    // Choose a database to explore (travel-sample)
    const travelSampleDatabase = databases.find(db => db === 'travel-sample');
    
    if (travelSampleDatabase) {
      // List scopes in the travel-sample database
      const scopes = await listScopes(travelSampleDatabase);
      
      if (scopes.length > 0) {
        // Choose a scope to explore (inventory)
        const inventoryScope = scopes.find(scope => scope === 'inventory');
        
        if (inventoryScope) {
          // List collections in the travel-sample.inventory scope
          const collections = await listCollections(travelSampleDatabase, inventoryScope);
          
          // Get schema for a sample collection and compare methods
          if (collections.length > 0) {
            // Choose the first collection for comparison
            const sampleCollection = collections[0]; 
            await compareSchemaMethods(travelSampleDatabase, inventoryScope, sampleCollection);
            // await getCollectionSchemaFromMetadata(travelSampleDatabase, inventoryScope, collections[0]); // Old call
          }
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

// Function to explore metadata
async function exploreMetadata() {
  console.log('\n--- EXPLORING SYSTEM METADATA ---');
  
  // Get one example of each metadata type
  const metadataTypes = ['Dataset', 'Dataverse', 'Link', 'Function', 'Index', 'Synonym'];
  
  for (const type of metadataTypes) {
    const response = await executeQuery(`SELECT * FROM System.Metadata.\`${type}\` LIMIT 1`);
    displayResults(response, `Sample ${type} Metadata`);
  }
  
  // List all collections
  const collectionsResponse = await executeQuery(`
    SELECT VALUE d.DatabaseName || '.' || d.DataverseName || '.' || d.DatasetName
    FROM System.Metadata.\`Dataset\` d
    WHERE d.DataverseName <> "Metadata"
  `);
  displayResults(collectionsResponse, 'All Collections (excluding System.Metadata)');
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
    
    // Try exploring metadata (if available)
    await exploreMetadata();
    
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
  checkMetadataAccess,
  listDatabases,
  listScopes,
  listCollections,
  getCollectionSchemaByInference,
  getCollectionSchemaFromMetadata,
  compareSchemaMethods,
  exploreDatabaseStructure,
  exploreTravelSample,
  runAnalysisQueries,
  runJoinQueries,
  exploreMetadata,
  runExplorer
}; 