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

  // Construct the base API URL using the helper function for Columnar (port 18095)
  const apiBaseUrl = constructApiUrl(path, 18095); // Default Columnar port is 18095
  Logger.log('validateCredentials: Constructed Columnar API Base URL: %s', apiBaseUrl);

  const queryUrl = apiBaseUrl + '/api/v1/request'; // Use Columnar service path
  // Log the final URL being used for the fetch call
  Logger.log('validateCredentials constructed Columnar queryUrl: %s', queryUrl);
  Logger.log('Validation query URL (using port 18095): %s', queryUrl);

  const queryPayload = {
    statement: 'SELECT 1;', // Simple query compatible with Columnar
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
  
  // Construct base URLs using the helper function
  const queryBaseUrl = constructApiUrl(path, 18095); // Columnar port
  const mgmtBaseUrl = constructApiUrl(path, 18091);  // Default management port
  Logger.log('fetchCouchbaseMetadata: Query Base URL: %s, Mgmt Base URL: %s', queryBaseUrl, mgmtBaseUrl);

  // Endpoint to fetch buckets uses the management URL
  const bucketUrl = mgmtBaseUrl + '/pools/default/buckets';
  
  const options = {
    method: 'get',
    headers: {
      Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
    },
    muteHttpExceptions: true,
    validateHttpsCertificates: false
  };
  
  try {
    // Fetch buckets
    Logger.log('fetchCouchbaseMetadata: Fetching buckets from %s', bucketUrl);
    const response = UrlFetchApp.fetch(bucketUrl, options);
    
    if (response.getResponseCode() !== 200) {
      Logger.log('Error fetching buckets. Code: %s, Response: %s', 
                response.getResponseCode(), response.getContentText());
      return {
        buckets: [],
        scopesCollections: {}
      };
    }
    
    let bucketNames = [];
    try {
      const buckets = JSON.parse(response.getContentText());
      
      if (Array.isArray(buckets)) {
        bucketNames = buckets.map(bucket => bucket.name);
        Logger.log('fetchCouchbaseMetadata: Found buckets: %s', bucketNames.join(', '));
      } else {
        Logger.log('fetchCouchbaseMetadata: Unexpected bucket response format');
      }
    } catch (e) {
      Logger.log('Error parsing bucket response: %s', e.toString());
      return {
        buckets: [],
        scopesCollections: {}
      };
    }
    
    // Use Columnar API to get keyspaces
    const queryUrl = queryBaseUrl + '/api/v1/request';
    
    // Initialize the scopesCollections structure
    const scopesCollections = {};
    bucketNames.forEach(bucketName => {
      scopesCollections[bucketName] = {}; // Initialize empty object for each bucket
    });
    
    Logger.log('fetchCouchbaseMetadata: Querying using system:keyspaces');
    
    // For Columnar, we need to query system:keyspaces differently
    const keyspaceQueryPayload = {
      statement: "SELECT DISTINCT keyspace_id FROM system:keyspaces",
      timeout: "10000ms"
    };
    
    const queryOptions = {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(keyspaceQueryPayload),
      headers: {
        Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
      },
      muteHttpExceptions: true,
      validateHttpsCertificates: false
    };
    
    try {
      const keyspaceResponse = UrlFetchApp.fetch(queryUrl, queryOptions);
      
      if (keyspaceResponse.getResponseCode() === 200) {
        const keyspaceData = JSON.parse(keyspaceResponse.getContentText());
        Logger.log('fetchCouchbaseMetadata: Keyspace response received from system:keyspaces');
        
        if (keyspaceData.results && Array.isArray(keyspaceData.results)) {
          keyspaceData.results.forEach(item => {
            if (item.keyspace_id) {
              const keyspaceParts = item.keyspace_id.split(':');
              if (keyspaceParts.length >= 2) {
                const bucket = keyspaceParts[0];
                
                // Skip system keyspaces and non-matching buckets
                if (bucket === 'system' || !bucketNames.includes(bucket)) {
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
    } catch (e) {
      Logger.log('Error during keyspace query: %s', e.toString());
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
    .setText('Select one or more collections OR enter a custom Columnar query below. If a custom query is entered, the collection selection will be ignored.');

  // Fetch buckets, scopes, and collections
  const metadata = fetchCouchbaseMetadata();
  Logger.log('getConfig: Metadata fetch returned buckets: %s', JSON.stringify(metadata.buckets));
  
  // Use Single Select for the collection, as only the first is used by getSchema/getData
  const collectionSingleSelect = config.newSelectSingle()
    .setId('collection')
    .setName('Couchbase Collection (Required if no Custom Query)')
    .setHelpText('Select the single collection to query data from (ignored if Custom Query is entered).')
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
    collectionSingleSelect.addOption(
      config.newOptionBuilder().setLabel(item.label).setValue(item.path)
    );
  });

  // Always show query textarea
  config
    .newTextArea()
    .setId('query')
    .setName('Custom Columnar Query (Overrides Collection Selection)')
    .setHelpText('Enter a valid Columnar query. If entered, this query will be used instead of the collection selection above.')
    .setPlaceholder('SELECT airline.name, airline.iata, airline.country FROM `travel-sample`.`inventory`.`airline` AS airline WHERE airline.country = "France" AND airline.name LIKE "A%" LIMIT 10 OFFSET 20')
    .setAllowOverride(true);

  return config.build();
}

/**
 * Validates config, retrieves stored credentials, and prepares config for fetching.
 */
function validateConfig(configParams) {
  configParams = configParams || {};
  
  // Get stored credentials from PropertiesService
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  if (!path || !username || !password) {
     Logger.log('validateConfig Error: Authentication credentials missing from storage.');
     throwUserError('Authentication credentials are missing. Please use "EDIT CONNECTION" to reconnect to Couchbase.');
  }
  
  // Add credentials to configParams for use in fetchData
  configParams.baseUrl = path;
  configParams.username = username;
  configParams.password = password;
  
  // Validate configuration: Use query if provided, otherwise require a collection
  const hasQuery = configParams.query && configParams.query.trim() !== '';
  // Use the singular 'collection' ID now
  const hasCollection = configParams.collection && configParams.collection.trim() !== ''; 

  if (hasQuery) {
    // Custom query is provided, ignore collection selection
    configParams.collection = ''; // Clear collection explicitly
    Logger.log('validateConfig: Using custom query: %s', configParams.query);
  } else if (hasCollection) {
    // Collection is selected, query is empty
    configParams.query = ''; // Ensure query is empty
    Logger.log('validateConfig: Using collection: %s', configParams.collection); 
  } else {
    // Neither custom query nor collection is provided
    throwUserError('Configuration Error: Please select at least one collection OR enter a Custom Columnar Query.');
  }

  // Optional: Check Capella URL format
  if (path.includes('cloud.couchbase.com') && !path.startsWith('couchbases://') && !path.startsWith('https://')) {
      Logger.log('validateConfig Warning: Capella URL found without secure prefix: %s', path);
  }
  
  // Log success based on which input was used
  if (hasQuery) {
      Logger.log('validateConfig successful (used custom query).');
  } else {
      Logger.log('validateConfig successful (used collection: %s).', configParams.collection);
  }
  return configParams;
}

// ==========================================================================
// ===                        SCHEMA & DATA FLOW                          ===
// ==========================================================================

/**
 * Returns the schema for the given request.
 */
function getSchema(request) {
  request.configParams = validateConfig(request.configParams);
  
  try {
    const hasCustomQuery = request.configParams.query && request.configParams.query.trim() !== '';

    // If a custom query exists, use it directly
    if (hasCustomQuery) {
      Logger.log('getSchema: Using custom query: %s', request.configParams.query);
      const result = fetchData(request.configParams); 
      
      // Check if the custom query returned any results for schema inference
      if (!result?.results?.length) {
          Logger.log('getSchema Error: Custom query returned no results. Cannot build schema.');
          throwUserError(
            'Custom query returned no results. Schema cannot be determined. ' + 
            'Please ensure your custom query returns at least one row, or use the collection selector instead.'
          );
          // throwUserError stops execution, but return just in case
          return; 
      }
      
      // Proceed with building schema from the custom query results
      const schema = buildSchema(result);
      return { schema: schema };
    } else {
      // Otherwise, generate query based on the selected collection
      const collectionPath = request.configParams.collection; // Use singular 'collection'
      if (!collectionPath) {
         // This should ideally be caught by validateConfig, but double-check
         throwUserError('Configuration Error: No collection selected.'); 
         return; // Should not proceed
      }
      const [bucket, scope, collectionName] = collectionPath.split('.');
      
      // Set the specific collection details for the generated query context in fetchData
      const schemaParams = {
        ...request.configParams, // Keep other params like credentials
        bucket: bucket,
        scope: scope,
        collection: collectionName // Used for context, query is generated below
      };
      
      // Generate the SELECT * query, limited for schema inference performance
      const formattedBucket = '`' + bucket + '`';
      const formattedScope = '`' + scope + '`';
      const formattedCollection = '`' + collectionName + '`';
      schemaParams.query = 'SELECT * FROM ' + formattedBucket + '.' + formattedScope + '.' + formattedCollection + ' LIMIT 100'; // Limit for schema performance
      
      Logger.log('getSchema: Using collection %s for schema with generated query (LIMIT 100): %s', collectionPath, schemaParams.query);
      
      const result = fetchData(schemaParams);
      const schema = buildSchema(result);
      return { schema: schema };
    }
  } catch (e) {
    Logger.log('Error during getSchema: %s', e.toString());
    Logger.log('getSchema Exception details: %s', e.stack);
    throwUserError('Failed to get schema: ' + e.message);
  }
}

/**
 * Builds schema from the query results.
 */
function buildSchema(result) {
  if (!result?.results?.length) {
    const status = result?.status || 'unknown';
    const errors = result?.errors ? JSON.stringify(result.errors) : 'No details provided.';
    Logger.log('buildSchema failed: Query returned status %s with errors: %s', status, errors);
    throw new Error('Query returned no results or failed. Cannot build schema. Status: ' + status + ', Errors: ' + errors);
  }
  
  Logger.log(JSON.stringify(result.results.slice(0, 10), null, 2)); 
  
  const schema = [];
  // Helper function defined inside buildSchema to access the schema array easily
  function addFieldToSchema(key, value, parentKey = '') {
    const fieldName = parentKey ? parentKey + '.' + key : key;
    
    // --- Determine potential type from current value --- 
    let potentialDataType = 'STRING'; // Default to STRING
    let potentialSemantics = { conceptType: 'DIMENSION' };

    // Check specific types first
    if (value === null || value === undefined) {
        // If the first time we see a field it's null, assume STRING for flexibility
        potentialDataType = 'STRING'; 
        potentialSemantics = { conceptType: 'DIMENSION' };
    } else if (typeof value === 'number') {
      potentialDataType = 'NUMBER';
      potentialSemantics = { conceptType: 'METRIC', isReaggregatable: true };
    } else if (typeof value === 'boolean') {
      potentialDataType = 'BOOLEAN';
      potentialSemantics = { conceptType: 'DIMENSION' };
    } else if (value instanceof Date || (typeof value === 'string' && value.length > 10 && !isNaN(Date.parse(value)))) {
      potentialDataType = 'YEAR_MONTH_DAY_HOUR';
      potentialSemantics = { conceptType: 'DIMENSION', semanticGroup: 'DATETIME' };
    } else if (typeof value === 'object' && !Array.isArray(value)) {
       // Handle nested objects recursively
       for (const nestedKey in value) {
         addFieldToSchema(nestedKey, value[nestedKey], fieldName);
       }
       return; // Stop processing this level for nested objects
    } else if (Array.isArray(value)){
        // Skip arrays
        return;
    } // Otherwise, it remains STRING/DIMENSION
    
    // --- Check against existing schema entry --- 
    const existingFieldIndex = schema.findIndex(field => field.name === fieldName);

    if (existingFieldIndex === -1) {
      // Field doesn't exist, add it
      schema.push({
        name: fieldName,
        label: fieldName, 
        dataType: potentialDataType,
        semantics: potentialSemantics
      });
      Logger.log('buildSchema: Added field [%s] with type [%s]', fieldName, potentialDataType);
    } else {
      // Field exists, check for type merge/update
      const existingField = schema[existingFieldIndex];
      const currentDataType = existingField.dataType;

      if (currentDataType !== potentialDataType) {
        // Types differ, apply merging rules
        let mergedDataType = currentDataType;
        let needsUpdate = false;

        // If either is STRING, the result is STRING
        if (potentialDataType === 'STRING') {
            mergedDataType = 'STRING';
            needsUpdate = true;
        } else if (currentDataType !== 'STRING') {
             // If current isn't STRING and potential isn't STRING, but they differ 
             // (e.g., NUMBER vs BOOLEAN, NUMBER vs DATE), default to STRING for safety.
             mergedDataType = 'STRING'; 
             needsUpdate = true;
        } // If current is already STRING, mergedDataType remains STRING, no update needed based on this rule.
        
        if (needsUpdate && existingField.dataType !== mergedDataType) {
          Logger.log('buildSchema: Updating field [%s] type from [%s] to [%s] due to merge.', 
                    fieldName, existingField.dataType, mergedDataType);
          existingField.dataType = mergedDataType;
          // When merging to STRING, reset semantics to basic DIMENSION
          existingField.semantics = { conceptType: 'DIMENSION' }; 
        }
      }
    }
  }
  
  Logger.log('buildSchema: Iterating through %s documents to build merged schema...', result.results.length);

  // Iterate through ALL documents in the sample to build the schema
  result.results.forEach((row, index) => {
    let dataObject = row;
    const keys = Object.keys(row);
    
    // Check if data is nested under a single key (common pattern)
    if (keys.length === 1 && typeof row[keys[0]] === 'object' && row[keys[0]] !== null) {
      dataObject = row[keys[0]];
      // Logger.log('buildSchema (Doc %s): Data is nested under key: %s', index, keys[0]); // Optional log
    } else {
      // Logger.log('buildSchema (Doc %s): Data is at the top level.', index); // Optional log
    }

    // Process all top-level fields in the current dataObject
    for (const key in dataObject) {
      addFieldToSchema(key, dataObject[key]);
    }
  });

  if (schema.length === 0) {
    throw new Error('Could not determine any fields from the first row of results. Check query and data structure.');
  }
  
  Logger.log('buildSchema successful. Detected %s fields: %s', schema.length, schema.map(f => f.name).join(', '));
  return schema;
}

/**
 * Returns the tabular data for the given request.
 */
function getData(request) {
  request.configParams = validateConfig(request.configParams);
  
  try {
    const hasCustomQuery = request.configParams.query && request.configParams.query.trim() !== '';
    let result;
    let queryToRun;

    if (hasCustomQuery) {
      Logger.log('getData: Using custom query: %s', request.configParams.query);
      queryToRun = request.configParams.query;
      // Pass the full configParams, fetchData handles the query context
      result = fetchData(request.configParams); 
    } else { 
      // Generate query based on the selected collection
      const collectionPath = request.configParams.collection;
      if (!collectionPath) {
         throwUserError('Configuration Error: No collection selected.'); 
         return; 
      }
      const [bucket, scope, collectionName] = collectionPath.split('.');
      
      // Set the specific collection details for the generated query context in fetchData
      const dataParams = {
        ...request.configParams, 
        bucket: bucket,
        scope: scope,
        collection: collectionName 
      };
      
      // Generate the SELECT * query for this collection (no limit here)
      const formattedBucket = '`' + bucket + '`';
      const formattedScope = '`' + scope + '`';
      const formattedCollection = '`' + collectionName + '`';
      dataParams.query = 'SELECT * FROM ' + formattedBucket + '.' + formattedScope + '.' + formattedCollection;
      queryToRun = dataParams.query;
      
      Logger.log('getData: Using collection %s for data with generated query: %s', collectionPath, dataParams.query);
      result = fetchData(dataParams);
    }

    // --- Process results --- 
    if (!result?.results?.length) {
      // If query returned no results, return empty rows based on the fields Looker Studio requested
      const requestedFieldsSchema = getFieldsFromRequest(request); // Use helper to get schema structure
      Logger.log('getData: Query [%s] returned no results. Returning empty rows with schema.', queryToRun);
      return {
        schema: requestedFieldsSchema,
        rows: []
      };
    }

    // Get the field IDs requested by Looker Studio IN THEIR ORDER
    const requestedFieldIds = request.fields.map(field => field.name);
    
    // Build the schema for the response IN THE ORDER requested by Looker Studio
    // We use the schema information provided in the request itself, rather than re-building from data
    let responseSchema = [];
    try {
       // Attempt to get the full schema definition to ensure correct data types
       const fullSchema = getSchema(request).schema; 
       responseSchema = requestedFieldIds.map(fieldId => {
          const fieldDefinition = fullSchema.find(f => f.name === fieldId);
          // Return a minimal schema object if somehow not found in full schema (fallback)
          return fieldDefinition || { name: fieldId, label: fieldId, dataType: 'STRING' }; 
       });
    } catch (e) {
       // Fallback if getSchema fails during this phase (should be rare)
       Logger.log('getData: Error getting full schema during response building: %s. Using basic schema.', e);
       responseSchema = requestedFieldIds.map(fieldId => ({ name: fieldId, label: fieldId, dataType: 'STRING' }));
    }

    // Map data rows to the expected { values: [...] } format
    const rows = result.results.map(row => {
      // Handle potentially nested results (common in Couchbase N1QL)
      let dataObject = row;
      const keys = Object.keys(row);
      if (keys.length === 1 && typeof row[keys[0]] === 'object' && row[keys[0]] !== null) {
         dataObject = row[keys[0]];
      }
      
      // Create values array IN THE SAME ORDER as requestedFieldIds (and responseSchema)
      const values = requestedFieldIds.map(fieldId => { 
         // Handle nested field access (e.g., 'geo.lat')
         let value = null;
         if (fieldId.includes('.')) {
            try {
               value = fieldId.split('.').reduce((obj, key) => obj && obj[key] !== undefined ? obj[key] : null, dataObject);
            } catch (e) {
               value = null; // Handle cases where reduce fails
            }
         } else {
            value = dataObject[fieldId];
         }
         return value !== undefined ? value : null;
       });
      return { values };
    });
    
    Logger.log('getData: Returning %s rows based on query: %s', rows.length, queryToRun);
    return {
      schema: responseSchema, // Use the schema ordered according to the request
      rows: rows
    };

  } catch (e) {
    Logger.log('Error during getData: %s', e.toString());
    Logger.log('getData Exception details: %s', e.stack);
    throwUserError('Failed to get data: ' + e.message);
  }
}

/**
 * Fetches data from Couchbase Columnar using the provided configuration.
 */
function fetchData(configParams) {
  // Get credentials and URL from configParams (populated by validateConfig)
  const username = configParams.username;
  const password = configParams.password;
  const baseUrl = configParams.baseUrl;
  
  if (!username || !password || !baseUrl) {
     Logger.log('fetchData Error: Missing baseUrl, username, or password in configParams.');
     throw new Error('Configuration error: Connection details missing.');
  }

  // Bucket and scope are not directly used in Columnar API call payload
  const query = configParams.query; 
  const timeout = 30000; 

  // Construct the base API URL using the helper function for Columnar (port 18095)
  const apiBaseUrl = constructApiUrl(baseUrl, 18095); // Default Columnar port is 18095
  Logger.log('fetchData: Constructed Columnar API Base URL: %s', apiBaseUrl);
  
  const queryUrl = apiBaseUrl + '/api/v1/request'; // Use Columnar service path
  
  const queryPayload = {
    statement: query,
    timeout: timeout + "ms"
  };

  // query_context is not typically used for Columnar API
  Logger.log('fetchData: Columnar API call does not use query_context.');

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
    Logger.log('fetchData: Sending query to %s', queryUrl);
    Logger.log('fetchData: Query: %s', query);
    const response = UrlFetchApp.fetch(queryUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    if (responseCode !== 200) {
      Logger.log('Error querying Couchbase. URL: %s, Code: %s, Response: %s', queryUrl, responseCode, responseText);
      throw new Error('Error querying Couchbase: [Code: ' + responseCode + '] ' + responseText);
    }

    Logger.log('fetchData successful for URL: %s', queryUrl);
    const result = JSON.parse(responseText);
    if (result.status && result.status !== 'success' && result.errors) {
      Logger.log('Couchbase query failed. Status: %s, Errors: %s', result.status, JSON.stringify(result.errors));
      throw new Error('Couchbase query failed: ' + JSON.stringify(result.errors));
    }
    return result;

  } catch (e) {
    Logger.log('Error connecting to Couchbase during fetchData. URL: %s, Exception: %s', queryUrl, e.toString());
    Logger.log('fetchData Exception details: %s', e.stack);
    throw new Error('Error connecting to Couchbase: ' + e.toString());
  }
}

// ==========================================================================
// ===                            UTILITIES                               ===
// ==========================================================================

/**
 * Constructs a full API URL from a user-provided path, ensuring HTTPS
 * and adding a default port if none is specified, *except* for Capella URLs.
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

  // Add default port ONLY if it's not Capella and no port is specified
  if (!isCapella && !hasPort && defaultPort) {
    hostAndPort += ':' + defaultPort;
    Logger.log('constructApiUrl: Added default port %s for non-Capella URL.', defaultPort);
  } else if (isCapella) {
     Logger.log('constructApiUrl: Using standard HTTPS port (443 implied) for Capella URL: %s', hostAndPort);
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
  const fields = [];
  request.fields.forEach(field => {
    fields.push({ name: field.name }); 
  });
  // Attempt to get full schema definition if possible (requires a fetch)
  try {
    const fullSchema = getSchema(request).schema;
    const requestedFieldsSchema = fullSchema.filter(f => fields.map(rf => rf.name).includes(f.name));
    return requestedFieldsSchema.length > 0 ? requestedFieldsSchema : fields.map(f => ({ name: f.name, label: f.name, dataType: 'STRING' })); 
  } catch (e) {
    // Log the error and fallback if getSchema fails here
    Logger.log('getFieldsFromRequest: Failed to get full schema, falling back to basic schema. Error: %s', e.toString());
    return fields.map(f => ({ name: f.name, label: f.name, dataType: 'STRING' }));
  }
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
