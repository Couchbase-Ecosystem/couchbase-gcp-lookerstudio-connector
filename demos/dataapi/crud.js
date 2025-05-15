require('dotenv').config();

/**
 * Couchbase Data API Demo - Basic CRUD and Schema Inference
 * 
 * This file demonstrates how to:
 * 1. Authenticate with the Couchbase Data API
 * 2. Retrieve documents from travel-sample.inventory.airline
 * 3. Infer schema from retrieved documents
 */

// Configuration for Couchbase Data API
const config = {
  endpoint: process.env.DATA_API_ENDPOINT, // Use environment variable
  username: process.env.DATA_API_USERNAME,   // Use environment variable
  password: process.env.DATA_API_PASSWORD,        // Use environment variable
  bucket: 'travel-sample',
  scope: 'inventory',
  collection: 'airline'
};

// Encode credentials for Basic Auth
const encodeCredentials = (username, password) => {
  return Buffer.from(`${username}:${password}`).toString('base64');
};

/**
 * Fetch a specific document by its key
 */
async function getDocument(documentKey) {
  const url = `https://${config.endpoint}/v1/buckets/${config.bucket}/scopes/${config.scope}/collections/${config.collection}/documents/${documentKey}`;
  
  console.log(`Fetching document: ${url}`);
  
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': `Basic ${encodeCredentials(config.username, config.password)}`,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API request failed with status ${response.status}: ${errorText}`);
    }

    const data = await response.json();
    console.log('Document retrieved successfully:', data);
    return data;
  } catch (error) {
    console.error('Error fetching document:', error);
    throw error;
  }
}

/**
 * List multiple documents from a collection using the Query Service
 */
async function listDocuments(limit = 10) {
  const queryServiceUrl = `https://${config.endpoint}/_p/query/query/service`;
  const statement = `SELECT RAW ${config.collection} FROM \`${config.bucket}\`.\`${config.scope}\`.\`${config.collection}\` LIMIT ${limit}`;
  // Note: Using SELECT RAW ${config.collection} to directly get the document contents.
  // If you need metadata alongside the document (like the old /docs endpoint),
  // you might use: `SELECT META(${config.collection}).id, ${config.collection}.* FROM \`${config.bucket}\`.\`${config.scope}\`.\`${config.collection}\` LIMIT ${limit}`
  // and then adjust the response processing.

  console.log(`Fetching documents via Query Service: ${queryServiceUrl}`);
  console.log(`Statement: ${statement}`);
  
  try {
    const response = await fetch(queryServiceUrl, {
      method: 'POST', // Changed to POST
      headers: {
        'Authorization': `Basic ${encodeCredentials(config.username, config.password)}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ statement: statement }) // Send statement in payload
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API request failed with status ${response.status}: ${errorText}`);
    }

    const data = await response.json();
    // The query service response contains a 'results' array with the documents
    console.log(`Retrieved ${data.results ? data.results.length : 0} documents from query results`);
    return data.results || []; // Returns an array of documents directly
  } catch (error) {
    console.error('Error listing documents via Query Service:', error);
    throw error;
  }
}

/**
 * Infer schema from a collection by sampling documents
 */
async function inferSchema(sampleSize = 10, numSampleValues = 3) {
  const queryServiceUrl = `https://${config.endpoint}/_p/query/query/service`;
  // Construct the INFER N1QL statement
  // Ensure bucket, scope, and collection names are properly backticked for N1QL
  const keyspacePath = `\`${config.bucket}\`.\`${config.scope}\`.\`${config.collection}\``;
  const inferStatement = `INFER ${keyspacePath} WITH {\"sample_size\": ${sampleSize}, \"num_sample_values\": ${numSampleValues}, \"similarity_metric\": 0.1}`;

  console.log(`Inferring schema via Query Service: ${queryServiceUrl}`);
  console.log(`Statement: ${inferStatement}`);
  
  try {
    const response = await fetch(queryServiceUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${encodeCredentials(config.username, config.password)}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ statement: inferStatement })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API request for INFER failed with status ${response.status}: ${errorText}`);
    }

    const inferApiResult = await response.json();
    
    // Check if results are present and have the expected structure
    if (!inferApiResult.results || inferApiResult.results.length === 0 || !inferApiResult.results[0] || inferApiResult.results[0].length === 0) {
      console.error('INFER query returned no flavors or empty result:', JSON.stringify(inferApiResult));
      throw new Error('INFER query returned no flavors or an empty result structure.');
    }

    // The INFER output is an array of "flavors" (schemas). We process the first flavor.
    // Structure: results: [ [ flavor1, flavor2, ... ] ], so results[0] is the array of flavors.
    const firstFlavor = inferApiResult.results[0][0];

    if (!firstFlavor || !firstFlavor.properties) {
      console.error('First flavor in INFER result has no properties:', JSON.stringify(firstFlavor));
      throw new Error('First flavor in INFER result is missing properties.');
    }

    console.log(`Successfully fetched INFER schema, processing first flavor with ${Object.keys(firstFlavor.properties).length} top-level properties.`);

    const lookerSchema = [];

    function parseInferProperties(properties, prefix = '') {
      Object.keys(properties).forEach(key => {
        const fieldDef = properties[key];
        const fieldName = prefix ? `${prefix}.${key}` : key;
        let dataType = 'STRING'; // Default Looker Studio type
        let isMetric = false; // Default semantics

        const inferTypes = Array.isArray(fieldDef.type) ? fieldDef.type : [fieldDef.type];

        if (inferTypes.includes('number') || inferTypes.includes('integer')) {
          dataType = 'NUMBER';
          isMetric = true;
        } else if (inferTypes.includes('boolean')) {
          dataType = 'BOOLEAN';
        } else if (inferTypes.includes('string')) {
          // Check samples for URL pattern
          if (fieldDef.samples && fieldDef.samples.length > 0 && typeof fieldDef.samples[0] === 'string') {
            if (fieldDef.samples[0].startsWith('http://') || fieldDef.samples[0].startsWith('https://')) {
              dataType = 'URL';
            }
          }
          // Default to STRING if not URL
        } 
        // Handle object and array types from INFER
        else if (inferTypes.includes('object') && fieldDef.properties) {
          // Recursively parse nested properties
          parseInferProperties(fieldDef.properties, fieldName);
          return; // Skip adding the parent object itself as a field in Looker schema
        } else if (inferTypes.includes('array')) {
          // Arrays are generally represented as STRING in Looker Studio
          // Could inspect fieldDef.items for more detail if needed for specific cases
          dataType = 'STRING'; 
        }
        // 'null' type usually appears with other types; if standalone, defaults to STRING

        lookerSchema.push({
          name: fieldName,
          label: fieldName, // Using fieldName as label
          dataType: dataType,
          semantics: {
            conceptType: isMetric ? 'METRIC' : 'DIMENSION'
          }
        });
      });
    }

    parseInferProperties(firstFlavor.properties);
    
    if (lookerSchema.length === 0) {
        console.warn('Schema inference from INFER resulted in an empty Looker schema.');
    }

    console.log('Inferred schema (Looker format):', JSON.stringify(lookerSchema, null, 2));
    return lookerSchema;

  } catch (error) {
    console.error('Error in inferSchema function:', error);
    throw error;
  }
}

/**
 * Creates a sample document
 */
async function createDocument(documentKey, documentData) {
  const url = `https://${config.endpoint}/v1/buckets/${config.bucket}/scopes/${config.scope}/collections/${config.collection}/documents/${documentKey}`;
  
  console.log(`Creating document: ${url}`);
  
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${encodeCredentials(config.username, config.password)}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(documentData)
    });

    // Data API returns 201 Created for POST if document key is provided and doesn't exist
    // or 200 OK if it replaces an existing document (upsert behavior)
    if (!(response.status === 201 || response.status === 200)) {
      const errorText = await response.text();
      throw new Error(`API request failed with status ${response.status}: ${errorText}`);
    }

    // Response for POST might not have a body or could be minimal (e.g. just CAS)
    let data = {};
    try {
      data = await response.json();
    } catch (e) {
      // If no JSON body, that's fine for a successful POST/PUT
      console.log('Create/Update document: No JSON body in response, status: ', response.status);
    }
    console.log('Document created/replaced successfully:', data);
    return data;
  } catch (error) {
    console.error('Error creating document:', error);
    throw error;
  }
}

/**
 * Updates an existing document
 */
async function updateDocument(documentKey, documentData) {
  const url = `https://${config.endpoint}/v1/buckets/${config.bucket}/scopes/${config.scope}/collections/${config.collection}/documents/${documentKey}`;
  
  console.log(`Updating document: ${url}`);
  
  try {
    const response = await fetch(url, {
      method: 'PUT',
      headers: {
        'Authorization': `Basic ${encodeCredentials(config.username, config.password)}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(documentData)
    });

    // Data API returns 200 OK for successful PUT, or 204 No Content if CAS matches and content is unchanged
    if (!(response.status === 200 || response.status === 204)) {
      const errorText = await response.text();
      throw new Error(`API request failed with status ${response.status}: ${errorText}`);
    }

    let data = {};
    if (response.status === 200) { // Only try to parse JSON if status is 200
      try {
        data = await response.json();
      } catch (e) {
        console.log('Update document: No JSON body in 200 response, status: ', response.status);
      }
    }
    console.log('Document updated successfully:', data);
    return data;
  } catch (error) {
    console.error('Error updating document:', error);
    throw error;
  }
}

/**
 * Deletes a document
 */
async function deleteDocument(documentKey) {
  const url = `https://${config.endpoint}/v1/buckets/${config.bucket}/scopes/${config.scope}/collections/${config.collection}/documents/${documentKey}`;
  
  console.log(`Deleting document: ${url}`);
  
  try {
    const response = await fetch(url, {
      method: 'DELETE',
      headers: {
        'Authorization': `Basic ${encodeCredentials(config.username, config.password)}`,
        'Content-Type': 'application/json'
      }
    });

    // Data API returns 204 No Content or 200 OK for successful DELETE
    if (!(response.status === 204 || response.status === 200)) {
      const errorText = await response.text();
      throw new Error(`API request failed with status ${response.status}: ${errorText}`);
    }

    console.log('Document deleted successfully');
    return true;
  } catch (error) {
    console.error('Error deleting document:', error);
    throw error;
  }
}

// Example usage
async function runExamples() {
  try {
    // Example 1: Get a specific airline document
    console.log('\n=== Example 1: Get a specific airline document ===');
    const airline = await getDocument('airline_10');
    // console.log(JSON.stringify(airline, null, 2)); // Document content is directly returned
    
    // Example 2: List documents using Query Service
    console.log('\n=== Example 2: List airline documents via Query ===');
    const airlines = await listDocuments(5);
    console.log(JSON.stringify(airlines, null, 2));
    
    // Example 3: Infer schema
    console.log('\n=== Example 3: Infer schema from airline documents ===');
    const schema = await inferSchema(10);
    // console.log(JSON.stringify(schema, null, 2)); // Schema is logged within inferSchema
    
    // Example 4: Create a new document
    console.log('\n=== Example 4: Create a new airline ===');
    const newAirlineKey = 'airline_example_crud';
    const newAirlineData = {
      id: Date.now(), // Add an id for consistency with travel-sample
      type: 'airline',
      name: "CRUD Airlines",
      iata: "CR",
      icao: "CRUD",
      callsign: "CRUD-AIR",
      country: "Testland"
    };
    await createDocument(newAirlineKey, newAirlineData);
    await getDocument(newAirlineKey); // Verify creation
    
    // Example 5: Update the airline we just created
    console.log('\n=== Example 5: Update the airline we just created ===');
    const updatedAirlineData = { ...newAirlineData, country: "Updated Testland" };
    await updateDocument(newAirlineKey, updatedAirlineData);
    await getDocument(newAirlineKey); // Verify update
    
    // Example 6: Delete the airline we created
    console.log('\n=== Example 6: Delete the airline we created ===');
    await deleteDocument(newAirlineKey);
    try {
      await getDocument(newAirlineKey); // Verify deletion (should fail)
    } catch (e) {
      console.log(`Document ${newAirlineKey} successfully deleted, GET failed as expected: ${e.message}`);
    }
    
  } catch (error) {
    console.error('Error running examples:', error.message);
  }
}

// Uncomment to run the examples
runExamples();

// Export functions for reuse
module.exports = {
  getDocument,
  listDocuments,
  inferSchema,
  createDocument,
  updateDocument,
  deleteDocument
};
