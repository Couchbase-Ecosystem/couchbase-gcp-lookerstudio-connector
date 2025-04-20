/**
 * Couchbase Connector for Google Looker Studio
 * This connector allows users to connect to a Couchbase database and run N1QL queries.
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
 * Attempts to validate credentials by making a minimal query to Couchbase.
 * Called by isAuthValid.
 */
function validateCredentials(path, username, password) {
  // Log the raw path received from isAuthValid
  Logger.log('validateCredentials received path: %s', path);
  
  Logger.log('Attempting to validate credentials for path: %s, username: %s', path, username);
  if (!path || !username || !password) {
    Logger.log('Validation failed: Missing path, username, or password.');
    return false; 
  }

  // Construct the base API URL using the helper function
  const apiBaseUrl = constructApiUrl(path, 18093); // Default query port is 18093
  Logger.log('validateCredentials: Constructed API Base URL: %s', apiBaseUrl);

  const queryUrl = apiBaseUrl + '/query/service'; // Append the service path
  // Log the final URL being used for the fetch call
  Logger.log('validateCredentials constructed queryUrl: %s', queryUrl);
  Logger.log('Validation query URL (using port 18093): %s', queryUrl);

  const queryPayload = {
    statement: 'SELECT 1;',
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
  const queryBaseUrl = constructApiUrl(path, 18093); // Default query port
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
    
    // Use query service URL to get all keyspaces (buckets, scopes, collections)
    const queryUrl = queryBaseUrl + '/query/service';
    
    // This query will return all keyspaces (namespaces)
    const keyspaceQueryPayload = {
      statement: "SELECT RAW CONCAT(CONCAT(CONCAT(namespace_id, '.'), bucket_id), '.' || ks.scope_id || '.' || ks.id) FROM system:keyspaces ks",
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
    
    Logger.log('fetchCouchbaseMetadata: Querying keyspaces: %s', keyspaceQueryPayload.statement);
    
    // Initialize the scopesCollections structure with at least _default for each bucket
    const scopesCollections = {};
    bucketNames.forEach(bucketName => {
      scopesCollections[bucketName] = {
        '_default': ['_default']
      };
    });
    
    try {
      const keyspaceResponse = UrlFetchApp.fetch(queryUrl, queryOptions);
      
      if (keyspaceResponse.getResponseCode() === 200) {
        const keyspaceData = JSON.parse(keyspaceResponse.getContentText());
        Logger.log('fetchCouchbaseMetadata: Keyspace response: %s', keyspaceResponse.getContentText());
        
        if (keyspaceData.results && Array.isArray(keyspaceData.results)) {
          // Process each keyspace result
          keyspaceData.results.forEach(keyspace => {
            // Ensure keyspace is not null before processing
            if (keyspace) { 
              try {
                // Split the keyspace string to extract parts: namespace.bucket.scope.collection
                const parts = keyspace.split('.');
                if (parts.length === 4) {
                  const namespace = parts[0];
                  const bucket = parts[1];
                  const scope = parts[2];
                  const collection = parts[3];
                  
                  // Only process for known buckets
                  if (bucketNames.includes(bucket)) {
                    // Initialize scope if not exists
                    if (!scopesCollections[bucket][scope]) {
                      scopesCollections[bucket][scope] = [];
                    }
                    
                    // Add collection if not already added
                    if (!scopesCollections[bucket][scope].includes(collection)) {
                      scopesCollections[bucket][scope].push(collection);
                      Logger.log('fetchCouchbaseMetadata: Added %s.%s.%s', bucket, scope, collection);
                    }
                  }
                }
              } catch (e) {
                Logger.log('Error processing keyspace %s: %s', keyspace, e.toString());
              }
            } else {
              Logger.log('fetchCouchbaseMetadata: Skipping null keyspace entry.');
            }
          });
        }
      } else {
        Logger.log('Error fetching keyspaces. Code: %s, Response: %s', 
                  keyspaceResponse.getResponseCode(), keyspaceResponse.getContentText());
      }
    } catch (e) {
      Logger.log('Error in keyspace query: %s', e.toString());
    }
    
    // If nothing was found with the keyspace query, try another approach with system:all_keyspaces
    if (Object.keys(scopesCollections).every(bucket => 
        Object.keys(scopesCollections[bucket]).length <= 1 && 
        Object.keys(scopesCollections[bucket])[0] === '_default')) {
      
      Logger.log('fetchCouchbaseMetadata: First keyspace query didn\'t find scopes/collections, trying alternate query');
      
      const alternateQueryPayload = {
        statement: "SELECT * FROM system:all_keyspaces",
        timeout: "10000ms"
      };
      
      try {
        const alternateResponse = UrlFetchApp.fetch(queryUrl, {
          method: 'post',
          contentType: 'application/json',
          payload: JSON.stringify(alternateQueryPayload),
          headers: {
            Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
          },
          muteHttpExceptions: true,
          validateHttpsCertificates: false
        });
        
        if (alternateResponse.getResponseCode() === 200) {
          const alternateData = JSON.parse(alternateResponse.getContentText());
          Logger.log('fetchCouchbaseMetadata: Alternate keyspace response received');
          
          if (alternateData.results && Array.isArray(alternateData.results)) {
            alternateData.results.forEach(item => {
              if (item.all_keyspaces) {
                const keyspace = item.all_keyspaces;
                
                if (keyspace.bucket && bucketNames.includes(keyspace.bucket) && 
                    keyspace.scope && keyspace.name) {
                  
                  // Initialize scope if not exists
                  if (!scopesCollections[keyspace.bucket][keyspace.scope]) {
                    scopesCollections[keyspace.bucket][keyspace.scope] = [];
                  }
                  
                  // Add collection if not already added
                  if (!scopesCollections[keyspace.bucket][keyspace.scope].includes(keyspace.name)) {
                    scopesCollections[keyspace.bucket][keyspace.scope].push(keyspace.name);
                    Logger.log('fetchCouchbaseMetadata: Added from alternate: %s.%s.%s', 
                              keyspace.bucket, keyspace.scope, keyspace.name);
                  }
                }
              }
            });
          }
        }
      } catch (e) {
        Logger.log('Error in alternate keyspace query: %s', e.toString());
      }
    }
    
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
    .setText('Select one or more collections from your Couchbase database. The connector will generate queries to retrieve data.');

  // Fetch buckets, scopes, and collections
  const metadata = fetchCouchbaseMetadata();
  Logger.log('getConfig: Metadata fetch returned buckets: %s', JSON.stringify(metadata.buckets));
  
  // Create a multi-select for collections with fully qualified paths
  const collectionsSelect = config.newSelectMultiple()
    .setId('collections')
    .setName('Couchbase Collections')
    .setHelpText('Select one or more collections to query data from')
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
    collectionsSelect.addOption(
      config.newOptionBuilder().setLabel(item.label).setValue(item.path)
    );
  });
  
  // Add result limit field
  config
    .newTextInput()
    .setId('limit')
    .setName('Result Limit')
    .setHelpText('Maximum number of records to return per collection (default: 1000)')
    .setPlaceholder('1000')
    .setAllowOverride(true);
  
  // Add custom query checkbox
  const useCustomQuery = config
    .newCheckbox()
    .setId('useCustomQuery')
    .setName('Use Custom Query')
    .setHelpText('Check this box to write your own N1QL query instead of using the generated ones')
    .setAllowOverride(true);
  
  // Only show query textarea if custom query is checked
  if (request && request.configParams && request.configParams.useCustomQuery === 'true') {
    config
      .newTextArea()
      .setId('query')
      .setName('Custom N1QL Query')
      .setHelpText('Enter a valid N1QL query (e.g., SELECT * FROM bucket.scope.collection LIMIT 100)')
      .setPlaceholder('SELECT * FROM bucket.scope.collection LIMIT 100')
      .setAllowOverride(true);
  }

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
  
  // Validate required connector-specific config parameters
  if (!configParams.collections) {
    throwUserError('At least one collection must be selected.');
  }
  
  // Convert collections string to array if it's a string
  if (typeof configParams.collections === 'string') {
    configParams.collections = configParams.collections.split(',');
  }
  
  // Set default limit if not provided
  if (!configParams.limit) {
    configParams.limit = 1000;
  } else {
    // Ensure limit is a number
    configParams.limit = parseInt(configParams.limit, 10);
    if (isNaN(configParams.limit) || configParams.limit <= 0) {
      configParams.limit = 1000;
    }
  }
  
  // If useCustomQuery is checked, verify that query is provided
  if (configParams.useCustomQuery === 'true' && !configParams.query) {
    throwUserError('Custom query is required when "Use Custom Query" is checked.');
  }
  
  // Generate query automatically if custom query not being used
  if (configParams.useCustomQuery !== 'true') {
    // We'll generate the query in fetchData for each collection
    Logger.log('validateConfig: Will generate queries for collections: %s', configParams.collections.join(', '));
  }
  
  // Optional: Check Capella URL format
  if (path.includes('cloud.couchbase.com') && !path.startsWith('couchbases://') && !path.startsWith('https://')) {
      Logger.log('validateConfig Warning: Capella URL found without secure prefix: %s', path);
  }
  
  Logger.log('validateConfig successful for collections: %s', configParams.collections.join(', '));
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
    // If using custom query, fetch schema from that query
    if (request.configParams.useCustomQuery === 'true') {
      const result = fetchData(request.configParams);
      const schema = buildSchema(result);
      return { schema: schema };
    } else {
      // Get the first collection and use it for schema
      const collection = request.configParams.collections[0];
      const [bucket, scope, collectionName] = collection.split('.');
      
      // Set the first collection for schema retrieval using object spread
      const schemaParams = {
        ...request.configParams,
        bucket: bucket,
        scope: scope,
        collection: collectionName
      };
      
      // Generate a query for this collection
      const formattedBucket = '`' + bucket + '`';
      const formattedScope = '`' + scope + '`';
      const formattedCollection = '`' + collectionName + '`';
      schemaParams.query = 'SELECT * FROM ' + formattedBucket + '.' + formattedScope + '.' + formattedCollection + ' LIMIT ' + schemaParams.limit;
      
      Logger.log('getSchema: Using collection %s for schema with query: %s', collection, schemaParams.query);
      
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
  
  const schema = [];
  Logger.log('buildSchema: Analyzing first result row to determine schema');
  
  // Try to find the most complete row in the first few results for better schema detection
  const rowsToCheck = Math.min(result.results.length, 10);
  let mostCompleteRow = result.results[0];
  let maxProperties = 0;
  
  for (let i = 0; i < rowsToCheck; i++) {
    const row = result.results[i];
    let propertyCount = 0;
    
    // Count properties in this row (including nested ones)
    function countProperties(obj) {
      if (typeof obj !== 'object' || obj === null) return 0;
      let count = 0;
      for (const key in obj) {
        count++;
        if (typeof obj[key] === 'object' && obj[key] !== null) {
          // Don't count nested objects as additional properties
          // Just count the parent key
        }
      }
      return count;
    }
    
    // Check if data is nested under a key
    const keys = Object.keys(row);
    if (keys.length === 1 && typeof row[keys[0]] === 'object' && row[keys[0]] !== null) {
      propertyCount = countProperties(row[keys[0]]);
    } else {
      propertyCount = countProperties(row);
    }
    
    if (propertyCount > maxProperties) {
      mostCompleteRow = row;
      maxProperties = propertyCount;
    }
  }
  
  // Use the most complete row to build schema
  let dataObject = mostCompleteRow;
  const keys = Object.keys(mostCompleteRow);
  if (keys.length === 1 && typeof mostCompleteRow[keys[0]] === 'object' && mostCompleteRow[keys[0]] !== null) {
    dataObject = mostCompleteRow[keys[0]];
    Logger.log('buildSchema: Data appears nested under key: %s', keys[0]);
  } else {
    Logger.log('buildSchema: Data appears to be at the top level.');
  }

  // Function to determine field type and add it to schema
  function addFieldToSchema(key, value, parentKey = '') {
    const fieldName = parentKey ? parentKey + '.' + key : key;
    
    if (typeof value === 'object' && value !== null && !Array.isArray(value) && !(value instanceof Date)) {
      // For nested objects, flatten them with dot notation
      Logger.log('buildSchema: Found nested object for field %s', fieldName);
      for (const nestedKey in value) {
        addFieldToSchema(nestedKey, value[nestedKey], fieldName);
      }
      return;
    }
    
    // Skip arrays for now
    if (Array.isArray(value)) {
      Logger.log('buildSchema: Skipping array field %s', fieldName);
      return;
    }
    
    let dataType = 'STRING';
    let semantics = { conceptType: 'DIMENSION' };

    if (typeof value === 'number') {
      dataType = 'NUMBER';
      semantics = { conceptType: 'METRIC', isReaggregatable: true };
    } else if (typeof value === 'boolean') {
      dataType = 'BOOLEAN';
      semantics = { conceptType: 'DIMENSION' };
    } else if (value instanceof Date || (typeof value === 'string' && value.length > 10 && !isNaN(Date.parse(value)))) {
      dataType = 'YEAR_MONTH_DAY_HOUR';
      semantics = { conceptType: 'DIMENSION', semanticGroup: 'DATETIME' };
    }

    // Check if this field already exists
    if (!schema.some(field => field.name === fieldName)) {
      schema.push({
        name: fieldName,
        label: fieldName,
        dataType: dataType,
        semantics: semantics
      });
      Logger.log('buildSchema: Added field %s with type %s', fieldName, dataType);
    }
  }

  // Process all top-level fields
  for (const key in dataObject) {
    addFieldToSchema(key, dataObject[key]);
  }

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
    // If using custom query, return data from that query
    if (request.configParams.useCustomQuery === 'true') {
      const result = fetchData(request.configParams);
      
      if (!result?.results?.length) {
        const requestedFieldsSchema = getFieldsFromRequest(request);
        Logger.log('getData: Query returned no results. Returning empty rows with schema.');
        return {
          schema: requestedFieldsSchema,
          rows: []
        };
      }
      
      const schema = buildSchema(result);
      const requestedFieldIds = request.fields.map(field => field.name);
      const requestedFields = schema.filter(field => requestedFieldIds.indexOf(field.name) > -1);

      const rows = result.results.map(row => {
        let dataObject = row;
        const keys = Object.keys(row);
        if (keys.length === 1 && typeof row[keys[0]] === 'object' && row[keys[0]] !== null) {
           dataObject = row[keys[0]];
        }
        const values = requestedFieldIds.map(fieldId => dataObject[fieldId] !== undefined ? dataObject[fieldId] : null);
        return { values };
      });
      
      Logger.log('getData successful. Returning %s rows.', rows.length);
      return {
        schema: requestedFields,
        rows: rows
      };
    } else {
      // Get the first collection for now (in the future we could union multiple collections)
      // Currently Looker Studio doesn't handle multiple schemas well, so use only the first selected collection
      const collection = request.configParams.collections[0];
      const [bucket, scope, collectionName] = collection.split('.');
      
      // Set the collection for data retrieval using object spread
      const dataParams = {
        ...request.configParams,
        bucket: bucket,
        scope: scope,
        collection: collectionName
      };
      
      // Generate a query for this collection
      const formattedBucket = '`' + bucket + '`';
      const formattedScope = '`' + scope + '`';
      const formattedCollection = '`' + collectionName + '`';
      dataParams.query = 'SELECT * FROM ' + formattedBucket + '.' + formattedScope + '.' + formattedCollection + ' LIMIT ' + dataParams.limit;
      
      Logger.log('getData: Using collection %s for data with query: %s', collection, dataParams.query);
      
      const result = fetchData(dataParams);
      
      if (!result?.results?.length) {
        const requestedFieldsSchema = getFieldsFromRequest(request);
        Logger.log('getData: Query returned no results. Returning empty rows with schema.');
        return {
          schema: requestedFieldsSchema,
          rows: []
        };
      }
      
      const schema = buildSchema(result);
      const requestedFieldIds = request.fields.map(field => field.name);
      const requestedFields = schema.filter(field => requestedFieldIds.indexOf(field.name) > -1);

      const rows = result.results.map(row => {
        let dataObject = row;
        const keys = Object.keys(row);
        if (keys.length === 1 && typeof row[keys[0]] === 'object' && row[keys[0]] !== null) {
           dataObject = row[keys[0]];
        }
        const values = requestedFieldIds.map(fieldId => dataObject[fieldId] !== undefined ? dataObject[fieldId] : null);
        return { values };
      });
      
      Logger.log('getData successful. Returning %s rows.', rows.length);
      return {
        schema: requestedFields,
        rows: rows
      };
    }
  } catch (e) {
    Logger.log('Error during getData: %s', e.toString());
    Logger.log('getData Exception details: %s', e.stack);
    throwUserError('Failed to get data: ' + e.message);
  }
}

/**
 * Fetches data from Couchbase using the provided configuration.
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

  const bucket = configParams.bucket;
  const scope = configParams.scope || '_default';
  const query = configParams.query; // This will be the auto-generated query if useCustomQuery is not true
  const timeout = 30000; 

  // Construct the base API URL using the helper function
  const apiBaseUrl = constructApiUrl(baseUrl, 18093); // Default query port is 18093
  Logger.log('fetchData: Constructed API Base URL: %s', apiBaseUrl);
  
  const queryUrl = apiBaseUrl + '/query/service'; // Append the service path
  let queryContext = `default:\`${bucket}\``;
  if (scope && scope !== '_default') {
    queryContext += `.\`${scope}\``;
  }

  const queryPayload = {
    statement: query,
    query_context: queryContext,
    timeout: timeout + "ms"
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
    Logger.log('fetchData: Sending query to %s for context %s', queryUrl, queryContext);
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
 * and adding a default port if none is specified.
 */
function constructApiUrl(path, defaultPort) {
  let hostAndPort = path;

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

  if (!hasPort && defaultPort) {
    hostAndPort += ':' + defaultPort;
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
