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
 * Returns the schema for the given request.
 * 
 * @param {Object} request Schema request parameters.
 * @return {Object} Schema for the given request.
 */
function getSchema(request) {
  Logger.log('getSchema called with request: %s', JSON.stringify(request));
  
  try {
    const configParams = request.configParams;
    
    // If schema fields are provided in the configuration, use them
    if (configParams && configParams.schemaFields) {
      try {
        const schemaFields = JSON.parse(configParams.schemaFields);
        Logger.log('Using predefined schema fields: %s', JSON.stringify(schemaFields));
        return { schema: schemaFields };
      } catch (e) {
        Logger.log('Error parsing schema fields: %s', e.message);
        throwUserError('Invalid schema fields format: ' + e.message);
      }
    }
    
    // Otherwise, try to infer the schema from a sample query
    if (!configParams) {
      // Return a default schema if no configuration is provided
      return {
        schema: [
          { name: 'id', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' } },
          { name: 'value', dataType: 'NUMBER', semantics: { conceptType: 'METRIC' } }
        ]
      };
    }
    
    // Get credentials
    const userProperties = PropertiesService.getUserProperties();
    const path = userProperties.getProperty('dscc.path');
    const username = userProperties.getProperty('dscc.username');
    const password = userProperties.getProperty('dscc.password');
    
    if (!path || !username || !password) {
      Logger.log('getSchema: Missing credentials');
      throwUserError('Authentication credentials missing. Please reauthenticate.');
    }
    
    // Determine the query to execute for schema detection
    // We'll use LIMIT 1 to get just one row for schema detection
    let query;
    if (configParams.query && configParams.query.trim() !== '') {
      // Add a LIMIT 1 clause if not already present
      const baseQuery = configParams.query.trim();
      query = baseQuery.toLowerCase().includes('limit') ? 
        baseQuery : 
        `${baseQuery} LIMIT 1`;
    } else if (configParams.collection) {
      // Collection path needs to be properly formatted with separate backticks for each part
      const collectionParts = configParams.collection.split('.');
      
      if (collectionParts.length === 3) {
        // Properly format as `bucket`.`scope`.`collection`
        query = `SELECT * FROM \`${collectionParts[0]}\`.\`${collectionParts[1]}\`.\`${collectionParts[2]}\` LIMIT 1`;
      } else {
        // Fallback if the format is unexpected
        query = `SELECT * FROM \`${configParams.collection}\` LIMIT 1`;
      }
    } else {
      throwUserError('Configuration Error: No query or collection specified');
    }
    
    Logger.log('Schema detection query: %s', query);
    
    // Use constructApiUrl for consistent URL handling
    const columnarUrl = constructApiUrl(path, 18095);
    const queryUrl = columnarUrl + '/api/v1/request';
    
    // Set up the API request payload
    const payload = {
      statement: query,
      timeout: '10s'
    };
    
    // Create authentication header value
    const auth = Utilities.base64Encode(`${username}:${password}`);
    
    // Set up the API request options
    const options = {
      method: 'post',
      contentType: 'application/json',
      headers: {
        'Authorization': 'Basic ' + auth
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      validateHttpsCertificates: false
    };
    
    // Execute the request
    Logger.log('Executing schema detection request to %s', queryUrl);
    const response = UrlFetchApp.fetch(queryUrl, options);
    
    // Check response code
    const responseCode = response.getResponseCode();
    if (responseCode !== 200) {
      const errorText = response.getContentText();
      Logger.log('API error in schema detection: %s, Error: %s', responseCode, errorText);
      throwUserError(`Couchbase API error (${responseCode}): ${errorText}`);
    }
    
    // Parse the response
    const responseText = response.getContentText();
    let parsedResponse;
    
    try {
      parsedResponse = JSON.parse(responseText);
    } catch (e) {
      Logger.log('Error parsing API response for schema: %s', e.message);
      throwUserError('Invalid response from Couchbase API: ' + e.message);
    }
    
    // Extract the results - we should have at least one row for schema detection
    const results = parsedResponse.results || [];
    
    if (results.length === 0) {
      Logger.log('No data returned for schema detection');
      return {
        schema: [
          { name: 'id', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' } },
          { name: 'value', dataType: 'NUMBER', semantics: { conceptType: 'METRIC' } }
        ]
      };
    }
    
    // Infer the schema from the first row
    const firstRow = results[0];
    const fields = [];
    
    // Process each field in the result to determine its data type
    // Format according to Looker Studio requirements in looker-studio.txt and looker-studio-config.txt
    Object.keys(firstRow).forEach(key => {
      const value = firstRow[key];
      let dataType = 'STRING';
      let conceptType = 'DIMENSION';
      
      // Determine data type based on the value
      if (value === null || value === undefined) {
        dataType = 'STRING';
      } else if (typeof value === 'number') {
        dataType = 'NUMBER';
        conceptType = 'METRIC';
        // Add isReaggregatable for metrics as shown in looker-studio.txt
        fields.push({
          name: key,
          label: key,
          dataType: dataType,
          semantics: { 
            conceptType: conceptType,
            isReaggregatable: true
          }
        });
        return; // Skip the normal push at the end
      } else if (typeof value === 'boolean') {
        dataType = 'BOOLEAN';
      } else if (value instanceof Date || (typeof value === 'string' && !isNaN(Date.parse(value)))) {
        dataType = 'STRING';
        conceptType = 'DIMENSION';
      } else if (typeof value === 'object') {
        // Objects and arrays are converted to strings
        dataType = 'STRING';
      }
      
      fields.push({
        name: key,
        label: key,
        dataType: dataType,
        semantics: { conceptType: conceptType }
      });
    });
    
    Logger.log('Inferred schema: %s', JSON.stringify(fields));
    return { schema: fields };
  } catch (e) {
    Logger.log('Error in getSchema: %s', e.message);
    if (e.name === 'UserError') {
      throw e;
    }
    throwUserError(`Error getting schema: ${e.message}`);
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
 * Returns tabular data for the given request.
 *
 * @param {Object} request The request object.
 * @return {Object} The response object containing the data.
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
    const configParams = request.configParams;
    if (!configParams) {
      throwUserError('No configuration provided');
    }
    
    // Check required config parameters
    if ((!configParams.collection || configParams.collection.trim() === '') && 
        (!configParams.query || configParams.query.trim() === '')) {
      throwUserError('Either a collection or a custom query must be specified');
    }
    
    // Get requested fields from the request
    const requestedFields = request.fields || [];
    const maxRows = configParams.maxRows && parseInt(configParams.maxRows) > 0 ? 
                   parseInt(configParams.maxRows) : 1000;
    
    // Initialize fields schema
    let dataSchema = [];
    
    // For each requested field, find the matching schema field
    // Based on looker-studio2.txt example
    requestedFields.forEach(function(field) {
      // Find the schema definition for this field
      for (const schemaField of getSchema(request).schema) {
        if (schemaField.name === field.name) {
          dataSchema.push(schemaField);
          break;
        }
      }
    });
    
    // Construct the API URL
    const columnarUrl = constructApiUrl(path, 18095);
    const apiUrl = columnarUrl + '/api/v1/request';
    Logger.log('API URL: %s', apiUrl);
    
    // Construct query
    let query;
    
    if (configParams.query && configParams.query.trim() !== '') {
      // Use the provided custom SQL query
      query = configParams.query.trim();
      
      // Add a LIMIT clause if maxRows is specified and not already present
      if (maxRows && !query.toLowerCase().includes('limit')) {
        query += ` LIMIT ${maxRows}`;
      }
    } else {
      // Get the field names for the query
      const fieldList = requestedFields.map(field => 
        `\`${field.name}\``
      ).join(', ');
      
      // Collection path needs to be properly formatted with separate backticks for each part
      const collectionParts = configParams.collection.split('.');
      
      if (collectionParts.length === 3) {
        // Properly format as `bucket`.`scope`.`collection`
        query = `SELECT ${fieldList} FROM \`${collectionParts[0]}\`.\`${collectionParts[1]}\`.\`${collectionParts[2]}\``;
      } else {
        // Fallback if the format is unexpected
        query = `SELECT ${fieldList} FROM \`${configParams.collection}\``;
      }
      
      // Add a LIMIT clause
      query += ` LIMIT ${maxRows}`;
    }
    
    Logger.log('Executing query: %s', query);
    
    // Setup the API request
    const payload = {
      statement: query,
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
    
    Logger.log('Making API request with options: %s', JSON.stringify({
      url: apiUrl,
      method: options.method,
      headers: options.headers
    }));
    
    // Make the API request
    const response = UrlFetchApp.fetch(apiUrl, options);
    const responseCode = response.getResponseCode();
    const responseBody = response.getContentText();
    
    Logger.log('Response code: %s', responseCode);
    
    if (responseCode !== 200) {
      Logger.log('Error response: %s', responseBody);
      throwUserError(`Error from Couchbase Columnar API: ${responseBody}`);
    }
    
    const parsedResponse = JSON.parse(responseBody);
    
    if (parsedResponse.status === 'errors') {
      Logger.log('API reported errors: %s', JSON.stringify(parsedResponse.errors));
      throwUserError(`Query error: ${parsedResponse.errors[0].message}`);
    }
    
    // Format results based on Looker Studio requirements - similar to looker-studio2.txt example
    const rows = [];
    
    // Process each row of data
    if (parsedResponse.results && Array.isArray(parsedResponse.results)) {
      parsedResponse.results.forEach(function(item) {
        const row = [];
        
        // Add the value for each requested field
        requestedFields.forEach(function(field) {
          const fieldName = field.name;
          let value = item[fieldName];
          
          // Handle null values
          if (value === null || value === undefined) {
            row.push('');
          } else if (typeof value === 'number') {
            row.push(value);
          } else if (typeof value === 'boolean') {
            row.push(value);
          } else {
            row.push(String(value));
          }
        });
        
        rows.push({ values: row });
      });
    }
    
    // Return data in the format required by Looker Studio
    return {
      schema: dataSchema,
      rows: rows
    };
  } catch (e) {
    Logger.log('Error in getData: %s', e.message);
    throwUserError(`Error retrieving data: ${e.message}`);
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
