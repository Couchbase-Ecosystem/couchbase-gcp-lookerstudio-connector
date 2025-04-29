/**
 * Couchbase Columnar Connector for Google Looker Studio
 * This connector allows users to connect to a Couchbase database and run Columnar queries.
 */

// ==========================================================================
// ===                       AUTHENTICATION FLOW                          ===
// ==========================================================================

/**
 * Returns the authentication method required by the connector.
 */
function getAuthType() {
  const cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.PATH_USER_PASS) 
    .setHelpUrl('https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html') 
    .build();
}

/**
 * Attempts to validate credentials by making a minimal query to Couchbase Columnar.
 * Called by isAuthValid.
 */
function validateCredentials(path, username, password) {
  // Log the raw path received from isAuthValid
  Logger.log('validateCredentials received path: %s', path);
  
  Logger.log('Attempting to validate credentials against Columnar Service for path: %s, username: %s', path, username);
  if (!path || !username || !password) {
    Logger.log('Validation failed: Missing path, username, or password.');
    return false; 
  }

  // Use constructApiUrl for consistent URL handling
  const columnarUrl = constructApiUrl(path, 18095);
  const queryUrl = columnarUrl + '/api/v1/request';
  Logger.log('validateCredentials constructed Columnar queryUrl: %s', queryUrl);

  const queryPayload = {
    statement: 'SELECT 1 AS test;',
    timeout: '5s'
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(queryPayload),
    headers: {
      Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
    },
    muteHttpExceptions: true, 
    validateHttpsCertificates: false 
  };

  try {
    Logger.log('Sending validation request...');
    const response = UrlFetchApp.fetch(queryUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText(); 
    Logger.log('Validation response code: %s', responseCode);

    if (responseCode === 200) {
      Logger.log('Credential validation successful.');
      return true;
    } else {
      Logger.log('Credential validation failed. Code: %s, Response: %s', responseCode, responseText);
      return false;
    }
  } catch (e) {
    Logger.log('Credential validation failed with exception: %s', e.toString());
    Logger.log('Exception details: %s', e.stack); 
    return false;
  }
}

/**
 * Returns true if the auth service has access (credentials are stored and valid).
 */
function isAuthValid() {
  Logger.log('isAuthValid called.');
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password'); 

  Logger.log('isAuthValid: Path: %s, Username: %s, Password: %s', path, username, '********'); // Mask password in log
  if (!path || !username || !password) {
     Logger.log('isAuthValid: Credentials not found in storage.');
     return false;
  }
  
  // Re-enable live validation now that URL handling is fixed
  Logger.log('isAuthValid: Found credentials. Performing live validation test.');
  const isValid = validateCredentials(path, username, password);
  Logger.log('isAuthValid: Validation result: %s', isValid);
  return isValid;
}

/**
 * Sets the credentials entered by the user.
 */
function setCredentials(request) {
  Logger.log('setCredentials called.');
  const creds = request.pathUserPass;
  const path = creds.path;
  const username = creds.username;
  const password = creds.password;

  Logger.log('Received path: %s, username: %s, password: %s', path, username, '*'.repeat(password.length));

  try {
    const userProperties = PropertiesService.getUserProperties();
    userProperties.setProperty('dscc.path', path);
    userProperties.setProperty('dscc.username', username);
    userProperties.setProperty('dscc.password', password);
    Logger.log('Credentials stored successfully.');
  } catch (e) {
    Logger.log('Error storing credentials: %s', e.toString());
    return {
      errorCode: 'SystemError', 
      errorText: 'Failed to store credentials: ' + e.toString()
    };
  }
  
  Logger.log('setCredentials finished successfully.');
  return {
    errorCode: 'NONE'
  };
}

/**
 * Resets the auth service (clears stored credentials).
 */
function resetAuth() {
  Logger.log('resetAuth called.');
  try {
    const userProperties = PropertiesService.getUserProperties();
    userProperties.deleteProperty('dscc.path');
    userProperties.deleteProperty('dscc.username');
    userProperties.deleteProperty('dscc.password');
    Logger.log('Auth properties deleted.');
  } catch (e) {
    Logger.log('Error during resetAuth: %s', e.toString());
  }
}

// ==========================================================================
// ===                      CONFIGURATION FLOW                           ===
// ==========================================================================

/**
 * Fetches available buckets, scopes, and collections from Couchbase.
 * Used to populate dropdowns in the config UI.
 */
function fetchCouchbaseMetadata() {
  // Get stored credentials from PropertiesService
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  Logger.log('fetchCouchbaseMetadata: Starting fetch with path: %s, username: %s', path, username);
  
  if (!path || !username || !password) {
    Logger.log('fetchCouchbaseMetadata: Authentication credentials missing from storage.');
    return {
      buckets: [],
      scopesCollections: {}
    };
  }
  
  // Construct API URL - for columnar, use only the columnar direct query endpoint
  const columnarUrl = constructApiUrl(path, 18095);
  const queryUrl = columnarUrl + '/api/v1/request';
  Logger.log('fetchCouchbaseMetadata: Using Columnar URL: %s', queryUrl);

  const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);
  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: authHeader
    },
    muteHttpExceptions: true,
    validateHttpsCertificates: false
  };
  
  // Initialize empty result structure
  let bucketNames = [];
  const scopesCollections = {};
  
  try {
    // First check if System.Metadata is accessible
    Logger.log('fetchCouchbaseMetadata: Checking if System.Metadata is accessible');
    const metadataCheckPayload = {
      statement: "SELECT COUNT(*) as count FROM System.Metadata.`Dataset` LIMIT 1",
      timeout: "10000ms"
    };
    
    options.payload = JSON.stringify(metadataCheckPayload);
    
    let hasSystemMetadata = false;
    try {
      const metadataCheckResponse = UrlFetchApp.fetch(queryUrl, options);
      if (metadataCheckResponse.getResponseCode() === 200) {
        const metadataCheckData = JSON.parse(metadataCheckResponse.getContentText());
        if (metadataCheckData.results && metadataCheckData.results.length > 0) {
          hasSystemMetadata = true;
          Logger.log('fetchCouchbaseMetadata: System.Metadata is accessible');
        }
      }
    } catch (e) {
      Logger.log('fetchCouchbaseMetadata: Error checking System.Metadata access: %s', e.toString());
    }
    
    if (hasSystemMetadata) {
      // Use System.Metadata approach
      Logger.log('fetchCouchbaseMetadata: Using System.Metadata approach');
      
      // Get databases (buckets)
      const databaseQueryPayload = {
        statement: "SELECT DISTINCT DatabaseName FROM System.Metadata.`Dataset`",
        timeout: "10000ms"
      };
      
      options.payload = JSON.stringify(databaseQueryPayload);
      
      const databaseResponse = UrlFetchApp.fetch(queryUrl, options);
      
      if (databaseResponse.getResponseCode() === 200) {
        const databaseData = JSON.parse(databaseResponse.getContentText());
        
        if (databaseData.results && Array.isArray(databaseData.results)) {
          bucketNames = databaseData.results
            .filter(item => item.DatabaseName && item.DatabaseName !== 'System') // Filter out System database
            .map(item => item.DatabaseName);
          
          Logger.log('fetchCouchbaseMetadata: Found databases/buckets: %s', bucketNames.join(', '));
        }
      }
      
      // Get all collections and their scope/bucket info
      const collectionsQueryPayload = {
        statement: "SELECT DatabaseName, DataverseName, DatasetName FROM System.Metadata.`Dataset` WHERE DatabaseName != 'System'",
        timeout: "10000ms"
      };
      
      options.payload = JSON.stringify(collectionsQueryPayload);
      
      const collectionsResponse = UrlFetchApp.fetch(queryUrl, options);
      
      if (collectionsResponse.getResponseCode() === 200) {
        const collectionsData = JSON.parse(collectionsResponse.getContentText());
        
        if (collectionsData.results && Array.isArray(collectionsData.results)) {
          Logger.log('fetchCouchbaseMetadata: Found %s collections in metadata', collectionsData.results.length);
          
          // Initialize bucket structures
          bucketNames.forEach(bucket => {
            scopesCollections[bucket] = {};
          });
          
          // Process collections data
          collectionsData.results.forEach(item => {
            if (item.DatabaseName && item.DataverseName && item.DatasetName) {
              const bucket = item.DatabaseName;
              const scope = item.DataverseName;
              const collection = item.DatasetName;
              
              // Skip non-matching buckets
              if (!bucketNames.includes(bucket)) {
                return;
              }
              
              // Initialize scope if not exists
              if (!scopesCollections[bucket][scope]) {
                scopesCollections[bucket][scope] = [];
              }
              
              // Add collection if not already added
              if (!scopesCollections[bucket][scope].includes(collection)) {
                scopesCollections[bucket][scope].push(collection);
                Logger.log('fetchCouchbaseMetadata: Added: %s.%s.%s', 
                          bucket, scope, collection);
              }
            }
          });
        }
      }
    } else {
      // Fall back to legacy approach
      Logger.log('fetchCouchbaseMetadata: System.Metadata is not accessible, using legacy approach');
      
      // For Columnar, we need to query system:keyspaces directly
      const bucketQueryPayload = {
        statement: "SELECT DISTINCT SPLIT_PART(keyspace_id, ':', 1) AS bucket FROM system:keyspaces WHERE SPLIT_PART(keyspace_id, ':', 1) != 'system';",
        timeout: "10000ms"
      };
      
      // First get all buckets
      options.payload = JSON.stringify(bucketQueryPayload);
      Logger.log('fetchCouchbaseMetadata: Querying for buckets (legacy)');
      
      const bucketResponse = UrlFetchApp.fetch(queryUrl, options);
      
      if (bucketResponse.getResponseCode() === 200) {
        const bucketData = JSON.parse(bucketResponse.getContentText());
        
        if (bucketData.results && Array.isArray(bucketData.results)) {
          bucketNames = bucketData.results
            .filter(item => item.bucket) // Filter out any null or undefined
            .map(item => item.bucket);
          
          Logger.log('fetchCouchbaseMetadata: Found buckets: %s', bucketNames.join(', '));
        } else {
          Logger.log('fetchCouchbaseMetadata: Bucket query result format unexpected or empty.');
        }
      } else {
        Logger.log('Error fetching buckets. Code: %s, Response: %s', 
                  bucketResponse.getResponseCode(), bucketResponse.getContentText());
      }
      
      // Now get all keyspaces
      const keyspaceQueryPayload = {
        statement: "SELECT keyspace_id FROM system:keyspaces WHERE SPLIT_PART(keyspace_id, ':', 1) != 'system';",
        timeout: "10000ms"
      };
      
      options.payload = JSON.stringify(keyspaceQueryPayload);
      Logger.log('fetchCouchbaseMetadata: Querying for keyspaces (legacy)');
      
      const keyspaceResponse = UrlFetchApp.fetch(queryUrl, options);
      
      if (keyspaceResponse.getResponseCode() === 200) {
        const keyspaceData = JSON.parse(keyspaceResponse.getContentText());
        Logger.log('fetchCouchbaseMetadata: Keyspace response received');
        
        // Initialize bucket structures
        bucketNames.forEach(bucket => {
          scopesCollections[bucket] = {};
        });
        
        if (keyspaceData.results && Array.isArray(keyspaceData.results)) {
          keyspaceData.results.forEach(item => {
            if (item.keyspace_id) {
              const keyspaceParts = item.keyspace_id.split(':');
              if (keyspaceParts.length >= 2) {
                const bucket = keyspaceParts[0];
                
                // Skip non-matching buckets
                if (!bucketNames.includes(bucket)) {
                  return;
                }
                
                // Parse scope and collection from keyspace_id
                // Format is typically bucket:scope.collection
                const scopeCollectionParts = keyspaceParts[1].split('.');
                let scope, collection;
                
                if (scopeCollectionParts.length >= 2) {
                  scope = scopeCollectionParts[0];
                  collection = scopeCollectionParts[1];
                } else {
                  // Default if format is unexpected
                  scope = '_default';
                  collection = scopeCollectionParts[0] || '_default';
                }
                
                // Initialize scope if not exists
                if (!scopesCollections[bucket][scope]) {
                  scopesCollections[bucket][scope] = [];
                }
                
                // Add collection if not already added
                if (!scopesCollections[bucket][scope].includes(collection)) {
                  scopesCollections[bucket][scope].push(collection);
                  Logger.log('fetchCouchbaseMetadata: Added: %s.%s.%s', 
                            bucket, scope, collection);
                }
              }
            }
          });
        } else {
          Logger.log('fetchCouchbaseMetadata: Keyspace query result format unexpected or empty.');
        }
      } else {
        Logger.log('Error fetching keyspaces. Code: %s, Response: %s', 
                  keyspaceResponse.getResponseCode(), keyspaceResponse.getContentText());
      }
    }
    
    // Add _default._default if no other collections were found for a bucket
    bucketNames.forEach(bucketName => {
      if (Object.keys(scopesCollections[bucketName]).length === 0) {
        scopesCollections[bucketName] = { '_default': ['_default'] };
        Logger.log('fetchCouchbaseMetadata: Added default keyspace for bucket %s', bucketName);
      }
    });

    return {
      buckets: bucketNames,
      scopesCollections: scopesCollections
    };
    
  } catch (e) {
    Logger.log('Error in fetchCouchbaseMetadata: %s', e.toString());
    Logger.log('Exception details: %s', e.stack);
    return {
      buckets: [],
      scopesCollections: {}
    };
  }
}

/**
 * Returns the user configurable options for the connector.
 */
function getConfig(request) {
  const cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText('Select a collection OR enter a custom Columnar query below. If a custom query is entered, the collection selection will be ignored.');

  // Fetch buckets, scopes, and collections
  const metadata = fetchCouchbaseMetadata();
  Logger.log('getConfig: Metadata fetch returned buckets: %s', JSON.stringify(metadata.buckets));
  
  // Use Single Select for the collection, as only the first is used by getSchema/getData
  const collectionSelect = config
    .newSelectSingle()
    .setId('collection')
    .setName('Couchbase Collection')
    .setHelpText('Select the collection to query data from (ignored if Custom Query is entered).')
    .setAllowOverride(true);
  
  // Build a list of all fully qualified collection paths
  const collectionPaths = [];
  
  // Loop through all buckets, scopes, collections to build paths
  Object.keys(metadata.scopesCollections).forEach(bucket => {
    Object.keys(metadata.scopesCollections[bucket]).forEach(scope => {
      metadata.scopesCollections[bucket][scope].forEach(collection => {
        // Create a fully qualified path: bucket.scope.collection
        const path = `${bucket}.${scope}.${collection}`;
        const label = `${bucket} > ${scope} > ${collection}`;
        collectionPaths.push({ path: path, label: label });
        
        Logger.log('getConfig: Added collection path: %s', path);
      });
    });
  });
  
  // Sort collection paths alphabetically
  collectionPaths.sort((a, b) => a.label.localeCompare(b.label));
  
  // Add options for each collection path
  collectionPaths.forEach(item => {
    collectionSelect.addOption(
      config.newOptionBuilder().setLabel(item.label).setValue(item.path)
    );
  });

  // Always show query textarea
  config
    .newTextArea()
    .setId('query')
    .setName('Custom Columnar Query')
    .setHelpText('Enter a valid Columnar query. If entered, this query will be used instead of the collection selection above.')
    .setPlaceholder('SELECT airline.name, airline.iata, airline.country FROM `travel-sample`.`inventory`.`airline` AS airline WHERE airline.country = "France" LIMIT 100')
    .setAllowOverride(true);
  
  // Add max rows option
  config
    .newTextInput()
    .setId('maxRows')
    .setName('Maximum Rows')
    .setHelpText('Maximum number of rows to return (default: 1000)')
    .setPlaceholder('1000')
    .setAllowOverride(true);

  return config.build();
}

/**
 * Validates the user configuration and returns the validated configuration object.
 *
 * @param {Object} configParams The user configuration parameters.
 * @return {Object} The validated configuration object.
 */
function validateConfig(configParams) {
  Logger.log('Validating config parameters: %s', JSON.stringify(configParams));
  
  if (!configParams) {
    throwUserError('No configuration provided');
  }
  
  // Get credentials from user properties
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  if (!path || !username || !password) {
    throwUserError('Authentication credentials missing. Please reauthenticate.');
  }
  
  // Check that either a collection or query is provided
  if ((!configParams.collection || configParams.collection.trim() === '') && 
      (!configParams.query || configParams.query.trim() === '')) {
    throwUserError('Either a collection or a custom query must be specified');
  }
  
  // Create a validated config object with defaults
  const validatedConfig = {
    path: path,
    username: username,
    password: password,
    collection: configParams.collection ? configParams.collection.trim() : '',
    query: configParams.query ? configParams.query.trim() : '',
    maxRows: configParams.maxRows && parseInt(configParams.maxRows) > 0 ? 
             parseInt(configParams.maxRows) : 1000
  };
  
  Logger.log('Config validation successful');
  return validatedConfig;
}

// ==========================================================================
// ===                        SCHEMA & DATA FLOW                          ===
// ==========================================================================

/**
 * Gets the requested fields from the request.
 *
 * @param {Object} request The request.
 * @return {Fields} The requested fields.
 */
function getRequestedFields(request) {
  const cc = DataStudioApp.createCommunityConnector();
  const requestedFields = cc.getFields(); // Start with an empty Fields object
  
  // Log the raw request fields for inspection
  Logger.log('getRequestedFields: Raw request.fields from Looker Studio: %s', JSON.stringify(request.fields));

  // Populate the Fields object using the information provided in the request
  request.fields.forEach(fieldInfo => {
    // Looker Studio provides the name and the inferred type/aggregation.
    // We need to respect this when building the Fields object for the getData response.
    Logger.log('getRequestedFields: Adding field [%s] to response schema', fieldInfo.name);
    
    // Fetch the full schema first
    const fullSchema = getSchema(request).schema; // Assuming getSchema is idempotent and fast enough
    
    // Find the definition for the current requested field
    const fieldDefinition = fullSchema.find(field => field.name === fieldInfo.name);
    
    if (fieldDefinition) {
       Logger.log('getRequestedFields: Found definition for [%s]: Type=%s, Concept=%s', 
                  fieldInfo.name, fieldDefinition.dataType, fieldDefinition.semantics.conceptType);
                  
       // Map schema string type to Apps Script FieldType enum
       let fieldTypeEnum;
       switch (fieldDefinition.dataType) {
         case 'NUMBER':
           // Covers NUMBER, CURRENCY, PERCENT, DURATION (as seconds)
           // Looker Studio applies formatting based on the type chosen in the UI.
           fieldTypeEnum = cc.FieldType.NUMBER;
           break;
         case 'BOOLEAN':
           fieldTypeEnum = cc.FieldType.BOOLEAN;
           break;
         case 'URL':
           fieldTypeEnum = cc.FieldType.URL;
           break;
         case 'STRING': // Fallthrough for STRING and any other unhandled types
         case 'TEXT': // Explicitly handle TEXT if getDataType returns it
         case 'DATE': // Handle DATE if getDataType were to return it
         case 'DATETIME': // Handle DATETIME if getDataType were to return it
         case 'GEO': // Handle GEO if getDataType were to return it (currently maps to STRING)
         default:
           fieldTypeEnum = cc.FieldType.TEXT; // Default to TEXT
           break;
       }
       
       if (fieldDefinition.semantics.conceptType === 'METRIC') {
         requestedFields.newMetric()
           .setId(fieldDefinition.name)
           .setName(fieldDefinition.name) 
           .setType(fieldTypeEnum); // Use the mapped enum
           // .setAggregation(fieldDefinition.semantics.aggregationType); // If available
       } else { // DIMENSION
         requestedFields.newDimension()
           .setId(fieldDefinition.name)
           .setName(fieldDefinition.name)
           .setType(fieldTypeEnum); // Use the mapped enum
       }
    } else {
       // Fallback if field definition not found (should not happen ideally)
       Logger.log('getRequestedFields: WARNING - Field definition not found for [%s] in full schema. Defaulting to TEXT Dimension.', fieldInfo.name);
       requestedFields.newDimension()
         .setId(fieldInfo.name)
         .setName(fieldInfo.name)
         .setType(cc.FieldType.TEXT);
    } 
  });

  // Log the fields object *before* returning
  Logger.log('getRequestedFields: Constructed Fields object for response: %s', JSON.stringify(requestedFields.asArray()));

  return requestedFields;
}

/**
 * Processes the result document and fixes field names for Columnar response format.
 * 
 * @param {Object} document The document from query results
 * @return {Object} Document with correctly formatted field names
 */
function processDocument(document) {
  // Check if the document is a result with a document prefix 
  // (e.g., airline: { id: "123", name: "Air France" })
  Logger.log('processDocument: Input document: %s', JSON.stringify(document));
  
  // Add check for non-object input
  if (typeof document !== 'object' || document === null) {
     Logger.log('processDocument: Input is not a valid object, returning empty object.');
     return {}; // Return an empty object to avoid errors downstream
  }

  const keys = Object.keys(document);
  
  // If there's only one top-level key and its value is an object, it might be a document prefix
  if (keys.length === 1 && typeof document[keys[0]] === 'object' && document[keys[0]] !== null) {
    const prefix = keys[0];
    const nestedObj = document[keys[0]];
    const result = {};
    
    // Create fields with the format prefix.field (e.g., airline.id)
    Object.keys(nestedObj).forEach(key => {
      result[`${prefix}.${key}`] = nestedObj[key];
    });
    
    Logger.log('Processed document with prefix %s: %s', prefix, JSON.stringify(result));
    return result;
  }
  
  // For documents without prefix, we still need to process arrays properly
  const result = {};
  
  // Helper function to flatten arrays and nested objects
  function flattenObject(obj, parentKey = '') {
    if (typeof obj !== 'object' || obj === null) {
      return { [parentKey]: obj };
    }
    
    let flattened = {};
    
    if (Array.isArray(obj)) {
      // For arrays of objects, generate fields for each key in each item
      if (obj.length > 0 && typeof obj[0] === 'object' && obj[0] !== null) {
        // Find all possible keys in the array objects
        const allKeys = new Set();
        obj.forEach(item => {
          if (item && typeof item === 'object') {
            Object.keys(item).forEach(key => allKeys.add(key));
          }
        });
        
        // Create a flattened field for each key in each item
        Array.from(allKeys).forEach(key => {
          obj.forEach((item, index) => {
            if (item && typeof item === 'object' && key in item) {
              const flatKey = `${parentKey}[${index}].${key}`;
              flattened[flatKey] = item[key];
            }
          });
        });
      } else {
        // For arrays of primitives
        obj.forEach((item, index) => {
          flattened[`${parentKey}[${index}]`] = item;
        });
      }
    } else {
      // For regular objects
      Object.keys(obj).forEach(key => {
        const newKey = parentKey ? `${parentKey}.${key}` : key;
        
        if (typeof obj[key] === 'object' && obj[key] !== null) {
          // Recursively flatten nested objects/arrays
          Object.assign(flattened, flattenObject(obj[key], newKey));
        } else {
          flattened[newKey] = obj[key];
        }
      });
    }
    
    return flattened;
  }
  
  // Flatten the top-level properties
  Object.keys(document).forEach(key => {
    if (typeof document[key] === 'object' && document[key] !== null) {
      // For objects and arrays, use the flattening helper
      Object.assign(result, flattenObject(document[key], key));
    } else {
      // For primitives, just copy the value
      result[key] = document[key];
    }
  });
  
  Logger.log('processDocument: Final flattened document: %s', JSON.stringify(result));
  return result;
}

/**
 * Returns the schema for the given request.
 *
 * @param {Object} request The request.
 * @return {Object} The schema response.
 */
function getSchema(request) {
  Logger.log('getSchema request: %s', JSON.stringify(request));
  
  try {
    // Get credentials from user properties
    const userProperties = PropertiesService.getUserProperties();
    const path = userProperties.getProperty('dscc.path');
    const username = userProperties.getProperty('dscc.username');
    const password = userProperties.getProperty('dscc.password');
    
    if (!path || !username || !password) {
      Logger.log('getSchema: Missing credentials');
      throwUserError('Authentication credentials missing. Please reauthenticate.');
    }
    
    const configParams = request.configParams || {};
    
    // Check for provided schema fields (takes highest priority)
    if (configParams.schemaFields && configParams.schemaFields.trim() !== '') {
      try {
        const schemaFields = JSON.parse(configParams.schemaFields);
        Logger.log('Using provided schema fields: %s', configParams.schemaFields);
        return { schema: schemaFields };
      } catch (e) {
        Logger.log('Error parsing schema fields: %s. Proceeding to query metadata/inference.', e.message);
      }
    }
    
    // --- Attempt 1: Query System.Metadata --- 
    let schemaFromMetadata = null;
    let canUseMetadata = configParams.collection && configParams.collection.trim() !== '' && 
                         !configParams.query; // Only use metadata if collection selected and no custom query
                        
    if (canUseMetadata) {
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length === 3) {
        const [dbName, dvName, dsName] = collectionParts;
        Logger.log('Attempting schema retrieval from System.Metadata for %s.%s.%s', dbName, dvName, dsName);
        
        // Construct the API URL
        const columnarUrl = constructApiUrl(path, 18095);
        const apiUrl = columnarUrl + '/api/v1/request';
        
        const metadataQuery = `
          SELECT Fields.FieldName, Fields.DataType 
          FROM System.Metadata.\`Dataset\` d 
          UNNEST d.Fields AS Fields 
          WHERE d.DatabaseName = \"${dbName}\"\n            AND d.DataverseName = \"${dvName}\"\n            AND d.DatasetName = \"${dsName}\"\n          ORDER BY Fields.FieldName; \n        `; // Select from the UNNEST alias 'Fields'
        
        const payload = {
          statement: metadataQuery,
          timeout: '15s' // Shorter timeout for metadata query
        };
        
        const options = {
          method: 'post',
          contentType: 'application/json',
          headers: {
            'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password)
          },
          payload: JSON.stringify(payload),
          muteHttpExceptions: true,
          validateHttpsCertificates: false
        };
        
        try {
          const response = UrlFetchApp.fetch(apiUrl, options);
          const responseCode = response.getResponseCode();
          const responseBody = response.getContentText();

          if (responseCode === 200) {
            const parsedResponse = JSON.parse(responseBody);
            if (parsedResponse.results && parsedResponse.results.length > 0) {
              schemaFromMetadata = parsedResponse.results.map(field => ({
                name: field.FieldName, 
                label: field.FieldName, // Use FieldName for both
                dataType: getDataType(null, field.DataType), // Map SQL type to Looker type
                semantics: {
                  conceptType: getConceptType(null, field.DataType) // Map SQL type to concept
                }
              }));
              Logger.log('Successfully retrieved schema from System.Metadata: %s fields', schemaFromMetadata.length);
            } else {
              Logger.log('System.Metadata query successful but returned no fields for %s.%s.%s', dbName, dvName, dsName);
            }
          } else {
            Logger.log('Failed to query System.Metadata (%s): %s', responseCode, responseBody);
          }
        } catch (e) {
          Logger.log('Error querying System.Metadata: %s', e.toString());
        }
      } else {
         Logger.log('Selected collection [%s] does not have the expected format bucket.scope.collection for metadata lookup.', configParams.collection);
      }
    }
    
    // If metadata successful, return it
    if (schemaFromMetadata) {
      return { schema: schemaFromMetadata };
    }
    
    // --- Attempt 2: Fallback to Inference (LIMIT 1) --- 
    Logger.log('Falling back to schema inference using LIMIT 1 query.');
    
    // Helper function to process fields from a single sample object (for inference)
    function processFieldsForInference(obj, prefix = '') {
      Logger.log('processFieldsForInference: Processing object/value at prefix \'%s\'', prefix);
      const fields = [];
      
      // Handle null objects
      if (obj === null || obj === undefined) {
        return [{
          name: prefix || 'value',
          label: prefix || 'Value',
          dataType: 'STRING',
          semantics: {
            conceptType: 'DIMENSION'
          }
        }];
      }
      
      // Process based on type
      if (Array.isArray(obj)) {
        // --- Array Handling for Inference (Keep aggregated string representation) ---
        if (obj.length > 0 && typeof obj[0] === 'object' && obj[0] !== null) {
          const arrayPrefix = prefix ? `${prefix}` : 'array'; 
          const allKeys = new Set();
          Object.keys(obj[0]).forEach(key => allKeys.add(key));
          Logger.log('processFieldsForInference (Array of Objects): Found keys [%s] for prefix \'%s\'', Array.from(allKeys).join(', '), arrayPrefix);
          Array.from(allKeys).forEach(key => {
            const fieldName = `${arrayPrefix}.${key}`;
            fields.push({
              name: fieldName,
              label: fieldName,
              dataType: 'STRING',
              semantics: { conceptType: 'DIMENSION' }
            });
          });
          return fields;
        } else if (obj.length > 0) {
           const fieldName = prefix || 'array';
           fields.push({
             name: fieldName,
             label: fieldName,
             dataType: 'STRING', 
             semantics: { conceptType: 'DIMENSION' }
           });
           Logger.log('processFieldsForInference (Array of Primitives): Added aggregated field: %s', fieldName);
           return fields;
        } else {
          Logger.log('processFieldsForInference: Skipping empty array at prefix \'%s\'', prefix || 'root');
          return []; 
        }
      } else if (typeof obj === 'object') {
        // Process each property in the object
        Object.keys(obj).forEach(key => {
          const value = obj[key];
          const newPrefix = prefix ? `${prefix}.${key}` : key;
          
          if (value === null || value === undefined) {
            fields.push({
              name: newPrefix,
              label: newPrefix,
              dataType: 'STRING',
              semantics: { conceptType: 'DIMENSION' }
            });
          } else if (typeof value === 'object') {
            // Recursively process nested objects and arrays
            fields.push(...processFieldsForInference(value, newPrefix));
          } else {
            // Add field for primitive values
            const dataType = getDataType(value); // Use updated helper
            const fieldDef = {
              name: newPrefix,
              label: newPrefix,
              dataType: dataType,
              semantics: {
                conceptType: getConceptType(value, dataType) // Use updated helper
              }
            };
            Logger.log('processFieldsForInference: Adding primitive field: %s', JSON.stringify(fieldDef));
            fields.push(fieldDef);
          }
        });
        return fields;
      } else {
        // Handle primitive value
        const dataType = getDataType(obj); // Use updated helper
        const fieldDef = {
          name: prefix || 'value',
          label: prefix || 'Value',
          dataType: dataType,
          semantics: {
            conceptType: getConceptType(obj, dataType) // Use updated helper
          }
        };
        Logger.log('processFieldsForInference: Adding primitive field: %s', JSON.stringify(fieldDef));
        return [fieldDef];
      }
    }
    
    // Helper function to determine Data Studio data type (handles SQL types)
    function getDataType(value, sqlDataType = null) {
      if (sqlDataType) {
        const upperSqlType = sqlDataType.toUpperCase();
        // Map SQL types from Metadata
        if (upperSqlType.includes('INT') || upperSqlType.includes('DECIMAL') || upperSqlType.includes('DOUBLE') || upperSqlType.includes('FLOAT') || upperSqlType.includes('REAL') || upperSqlType.includes('NUMERIC')) {
          return 'NUMBER';
        } else if (upperSqlType.includes('BOOL')) {
          return 'BOOLEAN';
        } else if (upperSqlType.includes('TIME') || upperSqlType.includes('DATE')) {
          // Could map to specific Looker date/time types if needed, but STRING is safer for now
          return 'STRING'; 
        } else {
          return 'STRING'; // Default for VARCHAR, CHAR, TEXT, JSON, ARRAY, OBJECT etc.
        }
      } else {
        // Original inference logic based on JS type
        const type = typeof value;
        if (value === null || value === undefined) return 'STRING';
        if (type === 'number') return 'NUMBER';
        if (type === 'boolean') return 'BOOLEAN';
        if (type === 'string') {
          if (value.startsWith('http://') || value.startsWith('https://')) return 'URL';
          return 'STRING';
        }
        return 'STRING'; // Default for objects/arrays in inference
      }
    }
    
    // Helper function to determine concept type (handles SQL types)
    function getConceptType(value, lookerDataType, sqlDataType = null) {
      // Determine based on the *final* Looker data type
      if (lookerDataType === 'NUMBER') {
        return 'METRIC';
      } else {
        return 'DIMENSION';
      }
    }
    
    // --- Inference Query Logic --- 
    let sampleQuery = '';
    
    if (configParams.query && configParams.query.trim() !== '') {
      // If a custom query is provided, add LIMIT 1 if not present
      sampleQuery = configParams.query.trim();
      if (!sampleQuery.toLowerCase().includes('limit')) {
        sampleQuery += ' LIMIT 1';
      }
    } else if (configParams.collection && configParams.collection.trim() !== '') {
      // Construct query based on collection
      const collectionParts = configParams.collection.split('.');
      let collectionPath;
      if (collectionParts.length === 3) {
        collectionPath = '`' + collectionParts[0] + '`.' + '`' + collectionParts[1] + '`.' + '`' + collectionParts[2] + '`';
      } else {
        // Fallback for potentially incomplete path? Or error? For now, try direct name
        Logger.log('Warning: Collection path [%s] might be incomplete. Trying direct query.', configParams.collection);
        collectionPath = '`' + configParams.collection + '`'; 
      }
      sampleQuery = `SELECT * FROM ${collectionPath} LIMIT 1`;
    } else {
      throwUserError('Cannot infer schema: Neither collection nor custom query specified.');
    }
    
    Logger.log('Schema inference query: %s', sampleQuery);
    
    // Construct the API URL
    const columnarUrl = constructApiUrl(path, 18095);
    const apiUrl = columnarUrl + '/api/v1/request';
    
    // Setup the API request
    const payload = {
      statement: sampleQuery,
      timeout: '30s'
    };
    
    const options = {
      method: 'post',
      contentType: 'application/json',
      headers: {
        'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password)
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      validateHttpsCertificates: false
    };
    
    // Make the API request for inference
    const response = UrlFetchApp.fetch(apiUrl, options);
    const responseCode = response.getResponseCode();
    const responseBody = response.getContentText(); 

    Logger.log('getSchema (Inference): Raw API response (code %s): %s...', responseCode, responseBody.substring(0, 500));
    
    if (responseCode !== 200) {
      Logger.log('API error during schema inference: %s, Error: %s', responseCode, responseBody);
      throwUserError(`Couchbase API error during schema inference (${responseCode}): ${responseBody}`);
    }
    
    // Parse the inference response
    const parsedResponse = JSON.parse(responseBody);
    let results = parsedResponse.results || [];
    
    if (results.length > 0) {
      // Use the first row to infer schema using the dedicated helper
      const firstRow = processDocument(results[0]); // processDocument flattens potential prefixes
      const fields = processFieldsForInference(firstRow);
      
      Logger.log('getSchema: Final inferred schema: %s', JSON.stringify(fields));
      return { schema: fields };
    } else {
      // No results returned from inference query
      Logger.log('No results returned from inference query, returning default schema');
      return {
        schema: [
          {
            name: configParams.collection || 'value',
            label: configParams.collection || 'Value',
            dataType: 'STRING',
            semantics: {
              conceptType: 'DIMENSION'
            }
          }
        ]
      };
    }
  } catch (e) {
    Logger.log('Error in getSchema: %s', e.message);
    throwUserError(`Error inferring schema: ${e.message}`);
  }
}

/**
 * Returns the data for the given request.
 *
 * @param {Object} request The request.
 * @return {Object} The data response.
 */
function getData(request) {
  Logger.log('getData request: %s', JSON.stringify(request));
  
  try {
    // Get credentials from user properties
    const userProperties = PropertiesService.getUserProperties();
    const path = userProperties.getProperty('dscc.path');
    const username = userProperties.getProperty('dscc.username');
    const password = userProperties.getProperty('dscc.password');
    
    if (!path || !username || !password) {
      Logger.log('getData: Missing credentials');
      throwUserError('Authentication credentials missing. Please reauthenticate.');
    }
    
    // Get configuration
    const configParams = request.configParams || {};
    
    if ((!configParams.collection || configParams.collection.trim() === '') && 
        (!configParams.query || configParams.query.trim() === '')) {
      throwUserError('Either collection or custom query must be specified.');
    }
    
    // Get requested fields
    const requestedFieldsObject = getRequestedFields(request); // Renamed for clarity
    const requestedFieldsArray = requestedFieldsObject.asArray(); // Array of Field objects
    const requestedFieldIds = requestedFieldsArray.map(field => field.getId()); // Get the array of IDs

    Logger.log('Requested fields object: %s', JSON.stringify(requestedFieldsArray));
    Logger.log('Requested field IDs array: %s', JSON.stringify(requestedFieldIds)); // Log the IDs
    
    // Determine max rows
    const maxRows = parseInt(configParams.maxRows, 10) || 1000;
    
    // Construct the API URL
    const columnarUrl = constructApiUrl(path, 18095);
    const apiUrl = columnarUrl + '/api/v1/request';
    
    // Check if there are nested fields in the requested fields
    const hasNestedFields = requestedFieldIds.some(fieldId => fieldId.includes('.') || fieldId.includes('['));
    Logger.log('hasNestedFields check result: %s', hasNestedFields); // Add log
    
    // Prepare the query
    let query = '';
    
    if (configParams.query && configParams.query.trim() !== '') {
      // Use custom query
      query = configParams.query.trim();
      
      // Add LIMIT clause if not present
      if (!query.toLowerCase().includes('limit')) {
        query += ` LIMIT ${maxRows}`;
      }
    } else {
      // Construct query based on collection and requested fields
      const collectionParts = configParams.collection.split('.');
      let collectionPath;
      if (collectionParts.length === 3) {
        // Escape each part with backticks
        collectionPath = '`' + collectionParts[0] + '`.`' + collectionParts[1] + '`.`' + collectionParts[2] + '`';
      } else {
        throwUserError('Invalid collection path specified. Use format: bucket.scope.collection');
      }

      // Construct the SELECT clause
      let selectClause = '*'; // Default to * if no fields requested
      const requiredSourceFields = new Set(); // Keep track of top-level fields needed from Couchbase

      if (requestedFieldIds && requestedFieldIds.length > 0) { // Use the extracted array
          // Identify the base fields needed for the requested aggregated/nested fields
          requestedFieldIds.forEach(fieldId => {
            // If fieldId is like 'schedule.day', we need the 'schedule' field.
            // If fieldId is like 'address.city', we need the 'address' field.
            // If fieldId is just 'airline', we need the 'airline' field.
            const baseField = fieldId.split('.')[0].split('[')[0]; // Get the part before the first '.' or '['
            requiredSourceFields.add(baseField);
          });
          
          // Select only the required base fields, escaping them
          selectClause = Array.from(requiredSourceFields)
                          .map(baseField => '`' + baseField + '`')
                          .join(', ');

          // If requiredSourceFields is empty (e.g., query error), default back to *
          if (selectClause.trim() === '') {
              Logger.log('getData: Warning - Could not determine base fields, defaulting SELECT to *');
              selectClause = '*';
          }
          
      } else {
         Logger.log('getData: No specific fields requested, using SELECT *');
      }

      // Use standard string concatenation
      query = 'SELECT ' + selectClause + ' FROM ' + collectionPath + ' LIMIT ' + maxRows;
    }
    
    Logger.log('Executing query: %s', query);
    
    // Setup the API request
    const payload = {
      statement: query,
      timeout: '60s'  // Increased timeout for larger queries
    };
    
    const options = {
      method: 'post',
      contentType: 'application/json',
      headers: {
        'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password)
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      validateHttpsCertificates: false
    };
    
    // Make the API request
    const response = UrlFetchApp.fetch(apiUrl, options);
    const responseCode = response.getResponseCode();
    
    if (responseCode !== 200) {
      const errorText = response.getContentText();
      Logger.log('API error in getData: %s, Error: %s', responseCode, errorText);
      throwUserError(`Couchbase API error (${responseCode}): ${errorText}`);
    }
    
    // Parse the response
    const responseBody = response.getContentText();
    let parsedResponse;
    
    try {
      parsedResponse = JSON.parse(responseBody);
    } catch (e) {
      Logger.log('Error parsing API response: %s', e.message);
      throwUserError('Invalid response from Couchbase API: ' + e.message);
    }
    
    // Helper function to get nested values by path including arrays
    function getNestedValue(obj, path) {
      // Handle array notation like "schedule[0].day"
      const parts = path.replace(/\[(\d+)\]/g, '.$1').split('.');
      let current = obj;
      
      for (let i = 0; i < parts.length; i++) {
        if (current === null || current === undefined) {
          return null;
        }
        
        // Handle array index when the key is a number
        const key = parts[i];
        if (!isNaN(key) && Array.isArray(current)) {
          const index = parseInt(key, 10);
          current = index < current.length ? current[index] : null;
        } else {
          current = current[key];
        }
      }
      
      return current;
    }
    
    // Process the results
    const results = parsedResponse.results || [];
    const rows = [];
    
    results.forEach(result => {
      // --- Modification Start: Determine actual data object ---
      // Handle cases where results are nested under a single key (common pattern)
      let dataObject = result;
      const keys = Object.keys(result);
      // If the result has exactly one key, and the value under that key is an object,
      // assume the actual document data is nested under that key.
      let keyPrefix = ''; // Keep track if we extracted data from a nested key
      if (keys.length === 1 && result[keys[0]] !== null && typeof result[keys[0]] === 'object') {
          dataObject = result[keys[0]];
          keyPrefix = keys[0] + '.'; // e.g., "route."
          if (results.indexOf(result) < 3) { // Log first 3 rows
              Logger.log('getData (Row %s): Using nested data under key: %s', results.indexOf(result), keys[0]);
          }
      } else {
         if (results.indexOf(result) < 3) { // Log first 3 rows
             Logger.log('getData (Row %s): Using top-level result object.', results.indexOf(result));
         }
      }
      // --- Modification End ---
      
      const values = [];
      
      // Log the raw result for the row
      if (results.indexOf(result) < 3) { // Log first 3 rows
        Logger.log('getData (Row %s): Raw dataObject being used: %s', results.indexOf(result), JSON.stringify(dataObject));
      }

      requestedFieldsObject.asArray().forEach(field => { // Use the Fields object here
        const fieldName = field.getId(); // e.g., airline or schedule.day
        const fieldType = field.getType(); // Should be STRING for aggregated fields
        let value = null; // Default value

        // Log field being processed
        if (results.indexOf(result) < 3) {
           Logger.log('getData (Row %s): Processing field: %s (Type: %s)', results.indexOf(result), fieldName, fieldType);
        }

        // --- Modification Start: Final Value Extraction Logic ---
        // Determine the path relative to the dataObject
        let relativePath = fieldName;
        if (keyPrefix && fieldName.startsWith(keyPrefix)) {
          relativePath = fieldName.substring(keyPrefix.length); // e.g., schedule.day or stops
        } else if (keyPrefix && !fieldName.startsWith(keyPrefix)){
          // This case shouldn't happen if schema/requests are consistent, but log a warning
          Logger.log('getData (Row %s): WARNING - fieldName [%s] does not match result key prefix [%s]', results.indexOf(result), fieldName, keyPrefix);
          // Attempt to use the full fieldName against the nested object anyway
          relativePath = fieldName; 
        }

        const relativeParts = relativePath.split('.');
        const basePart = relativeParts[0]; // e.g., schedule or stops
        const nestedPart = relativeParts.length > 1 ? relativeParts.slice(1).join('.') : null; // e.g., day or null
        
        // Get the data corresponding to the base part *within* the dataObject
        const sourceForCheck = dataObject ? dataObject[basePart] : undefined;

        if (nestedPart && Array.isArray(sourceForCheck)) {
            // CASE 1: Aggregated Array Field (e.g., schedule.day)
            // Base part (schedule) points to an array in dataObject.
            const aggregatedValues = sourceForCheck
                .map(item => {
                    // Extract the value using the nestedPart (day) from each item
                    let itemValue = (item && typeof item === 'object') ? getNestedValue(item, nestedPart) : item; 
                    return (itemValue === null || itemValue === undefined) ? '' : String(itemValue); // Ensure string conversion
                })
                .join(', '); // Join with comma separator
            value = aggregatedValues;
            if (results.indexOf(result) < 3) {
                Logger.log('getData (Row %s): [Aggregated Array] Value for [%s]: %s', results.indexOf(result), fieldName, value);
            }
        } else {
             // CASE 2: Simple Field or Nested Object Field (e.g., airline, address.city, stops)
             // Use getNestedValue on the dataObject using the relativePath.
             value = getNestedValue(dataObject, relativePath); 
              if (results.indexOf(result) < 3) {
                 // Log differently based on whether it was simple or truly nested
                 if (relativePath.includes('.')) {
                     Logger.log('getData (Row %s): [Nested Field] Value for [%s]: %s', results.indexOf(result), fieldName, value);
                 } else {
                     Logger.log('getData (Row %s): [Simple Field] Value for [%s]: %s', results.indexOf(result), fieldName, value);
                 }
             }
        }
        // --- Modification End ---

        // Process and push value based on schema type
        let formattedValue = null;
        if (value === null || value === undefined) {
          formattedValue = ''; // Use empty string for null/undefined to match previous behavior
        } else {
          switch (fieldType) {
            case DataStudioApp.createCommunityConnector().FieldType.NUMBER:
              formattedValue = Number(value);
              if (isNaN(formattedValue)) {
                formattedValue = null; // Send null if conversion fails
                Logger.log('getData (Row %s): Failed to convert value "%s" to NUMBER for field [%s]. Sending null.', results.indexOf(result), value, fieldName);
              }
              break;
            case DataStudioApp.createCommunityConnector().FieldType.BOOLEAN:
              // Handle common string representations of boolean
              if (typeof value === 'string') {
                const lowerValue = value.toLowerCase();
                if (lowerValue === 'true') {
                  formattedValue = true;
                } else if (lowerValue === 'false') {
                  formattedValue = false;
                } else {
                   formattedValue = null; // Or some default? Sending null if ambiguous.
                   Logger.log('getData (Row %s): Ambiguous boolean string "%s" for field [%s]. Sending null.', results.indexOf(result), value, fieldName);
                }
              } else {
                 formattedValue = Boolean(value); // Standard JS boolean conversion
              }
              break;
            // Add cases for YEAR_MONTH_DAY etc. if needed, formatting to YYYYMMDDhhmmss
            // case DataStudioApp.createCommunityConnector().FieldType.YEAR_MONTH_DAY:
            //   // Attempt to format 'value' into YYYYMMDD string
            //   break; 
            default: // STRING and others
              if (typeof value === 'object') {
                formattedValue = JSON.stringify(value);
              } else {
                formattedValue = value.toString();
              }
              break;
          }
        }

        values.push(formattedValue); 
      });
      
      rows.push({ values });
    });
    
    // Log final rows sample
    Logger.log('getData: Final rows sample (first %s rows): %s', 
              Math.min(3, rows.length), 
              JSON.stringify(rows.slice(0, 3)));

    return {
      schema: requestedFieldsObject.build(),
      rows: rows
    };
  } catch (e) {
    // Improved error logging and handling
    const errorMessage = e.message ? e.message : 'An unspecified error occurred';
    Logger.log('Error in getData: %s. Full error object: %s', errorMessage, JSON.stringify(e)); 
    throwUserError(`Error retrieving data: ${errorMessage}`);
  }
}

/**
 * Processes the API response and formats it for Data Studio.
 *
 * @param {Object} response The API response to process.
 * @param {Fields} requestedFields The requested fields.
 * @return {Array} The formatted rows.
 */
function processResults(response, requestedFields) {
  Logger.log('Processing results from API response');
  Logger.log('processResults: Input response results sample (first 3): %s', JSON.stringify((response.results || []).slice(0, 3)));
  Logger.log('processResults: Requested fields: %s', JSON.stringify(requestedFields.asArray()));
  
  if (!response.results || !Array.isArray(response.results)) {
    Logger.log('No results found in API response');
    return [];
  }
  
  // Map the results to the requested fields
  return response.results.map(function(row) {
    const values = [];
    requestedFields.asArray().forEach(function(field) {
      const fieldId = field.getId();
      let value = row[fieldId];
      
      // Handle null values
      if (value === null || value === undefined) {
        values.push('');
        return;
      }
      
      // Format values based on field type if needed
      switch (field.getType()) {
        case cc.FieldType.NUMBER:
          // Ensure numeric values are passed as numbers
          values.push(typeof value === 'number' ? value : Number(value));
          break;
        case cc.FieldType.BOOLEAN:
          // Ensure boolean values are passed as booleans
          values.push(!!value);
          break;
        default:
          // For strings and other types, convert to string
          values.push(String(value));
      }
    });
    
    return { values: values };
  });
}

// ==========================================================================
// ===                            UTILITIES                               ===
// ==========================================================================

/**
 * Constructs a full API URL from a user-provided path, ensuring HTTPS
 * and adding a default port if none is specified, *including* for Capella URLs.
 */
function constructApiUrl(path, defaultPort) {
  let hostAndPort = path;
  const isCapella = path.includes('cloud.couchbase.com');

  // Standardize scheme and strip it
  if (hostAndPort.startsWith('couchbases://')) {
    hostAndPort = hostAndPort.substring('couchbases://'.length);
  } else if (hostAndPort.startsWith('couchbase://')) {
    hostAndPort = hostAndPort.substring('couchbase://'.length);
  } else if (hostAndPort.startsWith('https://')) {
     hostAndPort = hostAndPort.substring('https://'.length);
  } else if (hostAndPort.startsWith('http://')) {
     hostAndPort = hostAndPort.substring('http://'.length);
  }

  // Remove trailing slash if present
  hostAndPort = hostAndPort.replace(/\/$/, '');

  // Check if port is already present (handles IPv4 and IPv6)
  const hasPort = /:\d+$|]:\d+$/.test(hostAndPort);

  // Add default port regardless of whether it's Capella
  if (!hasPort && defaultPort) {
    hostAndPort += ':' + defaultPort;
    if (isCapella) {
      Logger.log('constructApiUrl: Added port %s for Capella URL: %s', defaultPort, hostAndPort);
    } else {
      Logger.log('constructApiUrl: Added default port %s for URL: %s', defaultPort, hostAndPort);
    }
  } else if (hasPort) {
    Logger.log('constructApiUrl: Port already present in URL: %s', hostAndPort);
  }

  return 'https://' + hostAndPort;
}

/**
 * Helper function to get schema fields based on the request object.
 * Used when getData returns no results but schema is needed.
 */
function getFieldsFromRequest(request) {
  const fields = cc.getFields();
  const requestedFieldIds = request.fields.map(field => field.name);
  
  return fields.forIds(requestedFieldIds);
}

/**
 * Throws a user-friendly error message.
 */
function throwUserError(message) {
  DataStudioApp.createCommunityConnector()
    .newUserError()
    .setText(message)
    .throwException();
}

/**
 * Returns whether the current user is an admin user (currently unused).
 */
function isAdminUser() {
  return false;
}
