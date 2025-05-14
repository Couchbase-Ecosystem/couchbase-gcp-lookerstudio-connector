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
  endpoint: 'localhost:18094', // Replace with your Couchbase server endpoint
  username: 'Administrator',   // Replace with your username
  password: 'password',        // Replace with your password
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
 * List multiple documents from a collection
 */
async function listDocuments(limit = 10) {
  const url = `https://${config.endpoint}/v1/buckets/${config.bucket}/scopes/${config.scope}/collections/${config.collection}/docs?limit=${limit}`;
  
  console.log(`Fetching documents: ${url}`);
  
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
    console.log(`Retrieved ${data.results ? data.results.length : 0} documents`);
    
    // In the Data API, documents are returned in an array under the 'results' key
    // Each result has document metadata and the actual document under 'document' key
    return data.results || [];
  } catch (error) {
    console.error('Error listing documents:', error);
    throw error;
  }
}

/**
 * Infer schema from a collection by sampling documents
 */
async function inferSchema(sampleSize = 10) {
  try {
    // Get sample documents
    const documentSamples = await listDocuments(sampleSize);
    
    if (!documentSamples || documentSamples.length === 0) {
      throw new Error('No documents found to infer schema');
    }
    
    console.log(`Inferring schema from ${documentSamples.length} documents`);
    
    // Extract actual document data
    const documents = documentSamples.map(result => result.document);
    
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
              processFields(item, `${fieldName}[${index}]`);
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
      // Convert Set to Array for easier viewing
      const typeArray = Array.from(field.types);
      
      // Determine the most specific type
      let dataType;
      if (typeArray.includes('number')) {
        dataType = 'NUMBER';
      } else if (typeArray.includes('boolean')) {
        dataType = 'BOOLEAN';
      } else if (typeArray.includes('url')) {
        dataType = 'URL';
      } else {
        dataType = 'STRING'; // Default for strings, objects, arrays, etc.
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

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API request failed with status ${response.status}: ${errorText}`);
    }

    const data = await response.json();
    console.log('Document created successfully:', data);
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

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API request failed with status ${response.status}: ${errorText}`);
    }

    const data = await response.json();
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

    if (!response.ok) {
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
    console.log(airline);
    
    // Example 2: List documents
    console.log('\n=== Example 2: List airline documents ===');
    const airlines = await listDocuments(5);
    console.log(JSON.stringify(airlines, null, 2));
    
    // Example 3: Infer schema
    console.log('\n=== Example 3: Infer schema from airline documents ===');
    const schema = await inferSchema(10);
    console.log(JSON.stringify(schema, null, 2));
    
    // Example 4: Create a new document
    console.log('\n=== Example 4: Create a new airline ===');
    const newAirline = {
      name: "Example Airlines",
      iata: "EX",
      icao: "EXAM",
      callsign: "EXAMPLE",
      country: "United States"
    };
    await createDocument('airline_example', newAirline);
    
    // Example 5: Update a document
    console.log('\n=== Example 5: Update the airline we just created ===');
    newAirline.country = "Canada";
    await updateDocument('airline_example', newAirline);
    
    // Example 6: Delete a document
    console.log('\n=== Example 6: Delete the airline we created ===');
    await deleteDocument('airline_example');
    
  } catch (error) {
    console.error('Error running examples:', error);
  }
}

// Uncomment to run the examples
// runExamples();

// Export functions for reuse
module.exports = {
  getDocument,
  listDocuments,
  inferSchema,
  createDocument,
  updateDocument,
  deleteDocument
};
