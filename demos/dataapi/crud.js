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
async function inferSchema(sampleSize = 10) {
  try {
    // Get sample documents
    // listDocuments now returns an array of documents directly thanks to SELECT RAW
    const documents = await listDocuments(sampleSize);
    
    if (!documents || documents.length === 0) {
      throw new Error('No documents found to infer schema');
    }
    
    console.log(`Inferring schema from ${documents.length} documents`);
    
    // Create a schema map to track field types
    const schemaMap = {};
    
    // Function to recursively process fields and build schema
    function processFields(obj, prefix = '') {
      if (!obj || typeof obj !== 'object') {
        return;
      }
      
      Object.keys(obj).forEach(key => {
        const fieldName = prefix ? `${prefix}.${key}` : key;
        const value = obj[key];
        
        if (!(fieldName in schemaMap)) {
          // Initialize field info
          schemaMap[fieldName] = {
            name: fieldName,
            types: new Set(),
            isMetric: false
          };
        }
        
        // Track the type of this value
        if (value === null || value === undefined) {
          schemaMap[fieldName].types.add('null');
        } else if (typeof value === 'number') {
          schemaMap[fieldName].types.add('number');
          schemaMap[fieldName].isMetric = true;
        } else if (typeof value === 'boolean') {
          schemaMap[fieldName].types.add('boolean');
        } else if (typeof value === 'string') {
          if (value.startsWith('http://') || value.startsWith('https://')) {
            schemaMap[fieldName].types.add('url');
          } else {
            schemaMap[fieldName].types.add('string');
          }
        } else if (Array.isArray(value)) {
          schemaMap[fieldName].types.add('array');
          
          // Process array items if they are objects
          if (value.length > 0 && typeof value[0] === 'object' && value[0] !== null) {
            value.forEach((item, index) => {
              // Avoid excessively deep recursion for arrays of objects in schema
              // Represent as a general array type for Looker Studio
            });
          }
        } else if (typeof value === 'object') {
          schemaMap[fieldName].types.add('object');
          // Recursively process nested objects
          processFields(value, fieldName);
        }
      });
    }
    
    // Process all documents to build schema
    documents.forEach(doc => {
      processFields(doc);
    });
    
    // Convert schema map to array and determine final types
    const schema = Object.values(schemaMap).map(field => {
      const typeArray = Array.from(field.types);
      let dataType;
      if (typeArray.includes('number')) {
        dataType = 'NUMBER';
      } else if (typeArray.includes('boolean')) {
        dataType = 'BOOLEAN';
      } else if (typeArray.includes('url')) {
        dataType = 'URL';
      } else {
        dataType = 'STRING';
      }
      
      return {
        name: field.name,
        label: field.name,
        dataType: dataType,
        semantics: {
          conceptType: field.isMetric ? 'METRIC' : 'DIMENSION'
        }
      };
    });
    
    console.log('Inferred schema:', JSON.stringify(schema, null, 2));
    return schema;
  } catch (error) {
    console.error('Error inferring schema:', error);
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
