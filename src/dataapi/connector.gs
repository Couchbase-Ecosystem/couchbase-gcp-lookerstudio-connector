/**
 * Couchbase Data API Connector for Google Looker Studio
 * This connector allows users to connect to Couchbase Data API and run queries against it.
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
 * Attempts to validate credentials by making a minimal request to Couchbase Data API.
 * Called by isAuthValid.
 */
function validateCredentials(path, username, password) {
  Logger.log('validateCredentials received path: %s', path);
  
  Logger.log('Attempting to validate credentials against Data API for path: %s, username: %s', path, username);
  if (!path || !username || !password) {
    Logger.log('Validation failed: Missing path, username, or password.');
    return false;
  }

  // Construct API URL for Data API
  const apiUrl = constructApiUrl(path);
  
  // Test endpoint - we'll use /v1/callerIdentity which requires valid credentials
  const validationUrl = apiUrl + '/v1/callerIdentity';
  Logger.log('validateCredentials constructed Data API URL for validation: %s', validationUrl);

  const options = {
    method: 'get',
    contentType: 'application/json',
    headers: {
      Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
    },
    muteHttpExceptions: true,
    validateHttpsCertificates: false
  };

  try {
    Logger.log('Sending validation request...');
    const response = UrlFetchApp.fetch(validationUrl, options);
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
 * Helper function to execute N1QL queries.
 */
function executeN1qlQuery(apiUrl, authHeader, statement) {
  const queryServiceUrl = apiUrl + '/_p/query/query/service';
  Logger.log('executeN1qlQuery: URL: %s, Statement: %s', queryServiceUrl, statement);

  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: authHeader },
    payload: JSON.stringify({ statement: statement }),
    muteHttpExceptions: true,
    validateHttpsCertificates: false // Consistent with other fetch calls
  };

  try {
    const response = UrlFetchApp.fetch(queryServiceUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    if (responseCode === 200) {
      const queryResult = JSON.parse(responseText);
      if (queryResult.results) {
        Logger.log('executeN1qlQuery: Success, %s results.', queryResult.results.length);
        return queryResult.results; // This is an array of results
      } else if (queryResult.status === 'success' && queryResult.results === undefined) {
        // Some queries might return success with no results field if empty, treat as empty array
        Logger.log('executeN1qlQuery: Success but no "results" field, assuming empty. Response: %s', responseText);
        return [];
      } else {
        Logger.log('executeN1qlQuery: Query successful but response format unexpected. Code: %s, Response: %s', responseCode, responseText);
        // Consider how to handle this - could be an error or just an empty set for this API version
        return null; // Indicate an issue or unexpected format
      }
    } else {
      Logger.log('executeN1qlQuery: Error. Code: %s, Response: %s', responseCode, responseText);
      return null; // Indicate error
    }
  } catch (e) {
    Logger.log('executeN1qlQuery: Exception during fetch: %s. Statement: %s', e.toString(), statement);
    return null; // Indicate error
  }
}

/**
 * Fetches available buckets, scopes, and collections from Couchbase Data API using N1QL.
 * Used to populate dropdowns in the config UI.
 */
function fetchCouchbaseMetadata() {
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  Logger.log('fetchCouchbaseMetadata (N1QL): Starting fetch with path: %s, username: %s', path, username);
  
  if (!path || !username || !password) {
    Logger.log('fetchCouchbaseMetadata (N1QL): Auth credentials missing.');
    return { buckets: [], scopesCollections: {} };
  }
  
  const apiUrl = constructApiUrl(path);
  const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);
  
  const scopesCollections = {}; // Structure: { bucket: { scope: [collection1, collection2] } }
  let bucketNames = []; // To keep track of unique bucket names for the return structure

  try {
    // Use the more direct N1QL query joining system catalogs
    const n1qlQuery = 'SELECT b.name AS `bucket`, s.name AS `scope`, k.name AS `collection` ' +
                      'FROM system:buckets AS b ' +
                      'JOIN system:all_scopes AS s ON s.`bucket` = b.name ' +
                      'JOIN system:keyspaces AS k ON k.`bucket` = b.name AND k.`scope` = s.name ' +
                      'ORDER BY b.name, s.name, k.name;';

    const results = executeN1qlQuery(apiUrl, authHeader, n1qlQuery);

    if (results === null) {
      Logger.log('fetchCouchbaseMetadata (N1QL): Failed to fetch keyspace information or system catalogs not accessible.');
      return { buckets: [], scopesCollections: {} };
    }

    if (results.length === 0) {
      Logger.log('fetchCouchbaseMetadata (N1QL): No keyspaces (buckets/scopes/collections) found.');
      return { buckets: [], scopesCollections: {} };
    }

    Logger.log('fetchCouchbaseMetadata (N1QL): Processing %s items from query.', results.length);
    
    results.forEach(item => {
      const bucket = item.bucket;
      const scope = item.scope;
      const collection = item.collection;

      if (!bucket || !scope || !collection) {
        Logger.log('fetchCouchbaseMetadata (N1QL): Skipping item with missing bucket, scope, or collection: %s', JSON.stringify(item));
        return; // continue to next item
      }

      if (!scopesCollections[bucket]) {
        scopesCollections[bucket] = {};
        bucketNames.push(bucket); // Add to unique bucket names list
      }
      if (!scopesCollections[bucket][scope]) {
        scopesCollections[bucket][scope] = [];
      }
      scopesCollections[bucket][scope].push(collection);
      // Logger.log('fetchCouchbaseMetadata (N1QL): Added: %s.%s.%s', bucket, scope, collection); // Can be verbose
    });
    
    Logger.log('fetchCouchbaseMetadata (N1QL): Final structure: %s', JSON.stringify(scopesCollections));

    return {
      buckets: bucketNames, // Primarily for consistency, scopesCollections is the main structure used by getConfig
      scopesCollections: scopesCollections
    };
    
  } catch (e) {
    Logger.log('Error in fetchCouchbaseMetadata (N1QL): %s. Stack: %s', e.toString(), e.stack);
    return { buckets: [], scopesCollections: {} }; // Fallback on any exception
  }
}

/**
 * Returns the user configurable options for the connector.
 */
function getConfig(request) {
  const cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  try {
    // Determine if this is the first request (no params yet)
    const isFirstRequest = (request.configParams === undefined);
    const configParams = request.configParams || {};

    // Set the config to be dynamic based on the official stepped config guide
    let isStepped = true; // Assume config is ongoing unless proven otherwise

    config
      .newInfo()
      .setId('instructions')
      .setText('Choose a configuration mode: query by selecting a collection, or enter a custom N1QL query.');

    const modeSelector = config.newSelectSingle()
      .setId('configMode')
      .setName('Configuration Mode')
      .setHelpText('Select how you want to define the data source.')
      .setAllowOverride(true)
      .setIsDynamic(true); // Changing mode should trigger refresh

    modeSelector.addOption(config.newOptionBuilder().setLabel('Query by Collection').setValue('collection'));
    modeSelector.addOption(config.newOptionBuilder().setLabel('Use Custom Query').setValue('customQuery'));

    const currentMode = configParams.configMode ? configParams.configMode : 'collection';
    Logger.log('getConfig: Current mode: %s', currentMode);

    if (currentMode === 'collection') {
      config.newInfo()
        .setId('collection_info')
        .setText('Select a collection to query data from.');

      // Fetch buckets, scopes, and collections
      const metadata = fetchCouchbaseMetadata();
      Logger.log('getConfig: Metadata fetch returned buckets: %s', JSON.stringify(metadata.buckets));
      
      // Use Single Select for the collection, as only the first is used by getSchema/getData
      const collectionSelect = config
        .newSelectSingle()
        .setId('collection')
        .setName('Couchbase Collection')
        .setHelpText('Select the collection to query data from.')
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

      // Check if the collection has been selected - if so, configuration is complete
      const selectedCollection = configParams.collection ? configParams.collection : null;
      if (selectedCollection) {
        isStepped = false; // Config is complete for collection mode if collection is selected
      }
      
      // Only add maxRows if config is complete for this mode
      if (!isStepped) {
        config
          .newTextInput()
          .setId('maxRows')
          .setName('Maximum Rows')
          .setHelpText('Maximum number of rows to return (default: 100)')
          .setPlaceholder('100')
          .setAllowOverride(true);
      }
    } else if (currentMode === 'customQuery') {
      config.newInfo()
        .setId('custom_query_info')
        .setText('Enter your custom N1QL query below.');
      config
        .newTextArea()
        .setId('query')
        .setName('Custom N1QL Query')
        .setHelpText('Enter a valid N1QL query. Ensure you include a LIMIT clause if needed.')
        .setPlaceholder('SELECT * FROM `travel-sample`.`inventory`.`airline` WHERE country = "France" LIMIT 100')
        .setAllowOverride(true);
      
      isStepped = false; // Config is complete once the custom query text area is shown
    }

    // Set the stepped config status for the response
    config.setIsSteppedConfig(isStepped);
    Logger.log('getConfig: Setting setIsSteppedConfig to: %s', isStepped);

    return config.build();

  } catch (e) {
    Logger.log('ERROR in getConfig: %s. Stack: %s', e.message, e.stack);
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText('An unexpected error occurred while building the configuration. Please check the Apps Script logs for details. Error: ' + e.message)
      .setDebugText('getConfig failed: ' + e.stack)
      .throwException();
  }
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
  
  if (!configParams.configMode) {
    throwUserError('Configuration mode not specified. Please select a mode.');
  }

  // Create a validated config object with defaults
  const validatedConfig = {
    path: path,
    username: username,
    password: password,
    configMode: configParams.configMode
  };
  
  if (configParams.configMode === 'collection') {
    if (!configParams.collection || configParams.collection.trim() === '') {
      throwUserError('Collection must be specified in "Query by Collection" mode.');
    }
    validatedConfig.collection = configParams.collection.trim();
    validatedConfig.maxRows = configParams.maxRows && parseInt(configParams.maxRows) > 0 ? 
             parseInt(configParams.maxRows) : 100;
  } else if (configParams.configMode === 'customQuery') {
    if (!configParams.query || configParams.query.trim() === '') {
      throwUserError('Custom query must be specified in "Use Custom Query" mode.');
    }
    validatedConfig.query = configParams.query.trim();
  } else {
    throwUserError('Invalid configuration mode selected.');
  }
  
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
           fieldTypeEnum = cc.FieldType.NUMBER;
           break;
         case 'BOOLEAN':
           fieldTypeEnum = cc.FieldType.BOOLEAN;
           break;
         case 'URL':
           fieldTypeEnum = cc.FieldType.URL;
           break;
         case 'STRING': // Fallthrough for STRING and any other unhandled types
         case 'TEXT':
         case 'DATE':
         case 'DATETIME':
         case 'GEO':
         default:
           fieldTypeEnum = cc.FieldType.TEXT; // Default to TEXT
           break;
       }
       
       if (fieldDefinition.semantics.conceptType === 'METRIC') {
         requestedFields.newMetric()
           .setId(fieldDefinition.name)
           .setName(fieldDefinition.name) 
           .setType(fieldTypeEnum);
       } else { // DIMENSION
         requestedFields.newDimension()
           .setId(fieldDefinition.name)
           .setName(fieldDefinition.name)
           .setType(fieldTypeEnum);
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
    const apiUrl = constructApiUrl(path);
    let documentForSchemaInference;

    if (configParams.configMode === 'customQuery') {
      if (!configParams.query || configParams.query.trim() === '') {
        throwUserError('Custom query must be specified in "Use Custom Query" mode.');
      }
      
      const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);
      const queryServiceUrl = `${apiUrl}/_p/query/query/service`;
      
      // For schema inference, we'll run the custom query with LIMIT 1 to get sample data
      let userQuery = configParams.query.trim();
      
      // If the query already contains LIMIT, don't add another one
      if (!userQuery.toLowerCase().includes('limit')) {
        userQuery += ' LIMIT 1';
      }
      
      Logger.log('getSchema: Running custom query for schema inference: %s', userQuery);
      
      const fetchOptions = {
        method: 'post',
        contentType: 'application/json',
        headers: { 'Authorization': authHeader },
        payload: JSON.stringify({ statement: userQuery }),
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      
      const response = UrlFetchApp.fetch(queryServiceUrl, fetchOptions);
      if (response.getResponseCode() !== 200) {
        throwUserError(`Couchbase Query API error (${response.getResponseCode()}): ${response.getContentText()}`);
      }
      
      const queryResult = JSON.parse(response.getContentText());
      if (!queryResult.results || queryResult.results.length === 0) {
        Logger.log('getSchema: Custom query returned no results for schema inference.');
        return { schema: [{ name: 'empty_result', label: 'Empty Result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
      }
      
      documentForSchemaInference = queryResult.results[0];
      Logger.log('getSchema: Successfully retrieved sample document via custom query.');
    } else if (configParams.configMode === 'collection') {
      if (!configParams.collection || configParams.collection.trim() === '') {
        throwUserError('Collection must be specified in "Query by Collection" mode.');
      }
      
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length !== 3) {
        throwUserError('Invalid collection path. Format: bucket.scope.collection');
      }
      const [bucket, scope, collection] = collectionParts;
      const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);
      
      const statement = "SELECT RAW " + collection + " FROM `" + bucket + "`.`" + scope + "`.`" + collection + "` LIMIT 1";
      
      Logger.log('getSchema: Retrieving sample document via Query Service using executeN1qlQuery.');
      Logger.log('getSchema: Statement: %s', statement);

      const queryResults = executeN1qlQuery(apiUrl, authHeader, statement);

      if (queryResults === null) {
        // executeN1qlQuery already logs details, so we can throw a more specific error here.
        throwUserError('Failed to retrieve sample document for schema. Check logs for query error details.');
      }
      if (queryResults.length === 0) {
        Logger.log('No documents returned from query for schema inference.');
        // Return a schema with a placeholder if the collection is empty
        return { schema: [{ name: 'empty_collection', label: 'Collection is Empty or No Documents Found', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
      }
      documentForSchemaInference = queryResults[0]; // executeN1qlQuery returns the array of results
      Logger.log('getSchema: Successfully retrieved sample document via Query Service.');
    } else {
      throwUserError('Invalid configuration mode for schema inference.');
    }
    
    // Function to recursively process document fields
    function processFields(obj, prefix = '') {
      const fields = [];
      if (!obj || typeof obj !== 'object') return fields;
      Object.keys(obj).forEach(key => {
        const fieldName = prefix ? `${prefix}.${key}` : key;
        const value = obj[key];
        let dataType = 'STRING';
        let conceptType = 'DIMENSION';
        if (value === null || value === undefined) {
          dataType = 'STRING';
        } else if (typeof value === 'number') {
          dataType = 'NUMBER';
          conceptType = 'METRIC';
        } else if (typeof value === 'boolean') {
          dataType = 'BOOLEAN';
        } else if (typeof value === 'string') {
          if (value.startsWith('http://') || value.startsWith('https://')) {
            dataType = 'URL';
          }
        } else if (Array.isArray(value)) {
          // Represent arrays as STRING for simplicity in Looker Studio
          dataType = 'STRING'; 
        } else if (typeof value === 'object') {
          // For nested objects, recursively add their fields
          fields.push(...processFields(value, fieldName));
          return; // Skip adding the parent object itself as a field
        }
        fields.push({
          name: fieldName,
          label: fieldName,
          dataType: dataType,
          semantics: { conceptType: conceptType }
        });
      });
      return fields;
    }

    const schemaFields = processFields(documentForSchemaInference);
    if (schemaFields.length === 0) {
      Logger.log('Warning: Schema inference resulted in zero fields.');
      return { schema: [{ name: 'empty_result', label: 'Empty Result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
    }
    Logger.log('getSchema: Final inferred schema: %s', JSON.stringify(schemaFields));
    return { schema: schemaFields };

  } catch (e) {
    Logger.log('Error in getSchema: %s. Stack: %s', e.message, e.stack);
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
    const userProperties = PropertiesService.getUserProperties();
    const path = userProperties.getProperty('dscc.path');
    const username = userProperties.getProperty('dscc.username');
    const password = userProperties.getProperty('dscc.password');
    if (!path || !username || !password) {
      throwUserError('Authentication credentials missing.');
    }

    const configParams = request.configParams || {};
    const apiUrl = constructApiUrl(path);
    const requestedFieldsObject = getRequestedFields(request);
    const requestedFieldsArray = requestedFieldsObject.asArray();
    let documents = [];

    if (configParams.configMode === 'customQuery') {
      if (!configParams.query || configParams.query.trim() === '') {
        throwUserError('Custom query must be specified in "Use Custom Query" mode.');
      }
      
      const queryServiceUrl = `${apiUrl}/_p/query/query/service`;
      let userQuery = configParams.query.trim();
      
      Logger.log('getData: Executing custom query: %s', userQuery);
      
      const fetchOptions = {
        method: 'post',
        contentType: 'application/json',
        headers: { 'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password) },
        payload: JSON.stringify({ statement: userQuery }),
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      
      const response = UrlFetchApp.fetch(queryServiceUrl, fetchOptions);
      const responseCode = response.getResponseCode();
      const responseText = response.getContentText();

      if (responseCode !== 200) {
        throwUserError(`Couchbase Query API error (${responseCode}): ${responseText}`);
      }
      
      const queryResult = JSON.parse(responseText);
      if (queryResult.results) {
        documents = queryResult.results;
      }
      
      Logger.log('getData: Successfully retrieved %s documents via custom query.', documents.length);
    } else if (configParams.configMode === 'collection') {
      if (!configParams.collection || configParams.collection.trim() === '') {
        throwUserError('Collection must be specified in "Query by Collection" mode.');
      }
      
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length !== 3) {
        throwUserError('Invalid collection path. Format: bucket.scope.collection');
      }
      const [bucket, scope, collection] = collectionParts;
      const queryServiceUrl = `${apiUrl}/_p/query/query/service`;
      const maxRows = parseInt(configParams.maxRows, 10) || 100;
      
      const statement = "SELECT RAW " + collection + " FROM `" + bucket + "`.`" + scope + "`.`" + collection + "` LIMIT " + maxRows;
      
      Logger.log('getData: Retrieving documents via Query Service: %s', queryServiceUrl);
      Logger.log('getData: Statement: %s', statement);

      const fetchOptions = {
        method: 'post',
        contentType: 'application/json',
        headers: { 'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password) },
        payload: JSON.stringify({ statement: statement }),
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      const response = UrlFetchApp.fetch(queryServiceUrl, fetchOptions);
      const responseCode = response.getResponseCode();
      const responseText = response.getContentText();

      if (responseCode !== 200) {
        throwUserError(`Couchbase Query API error (${responseCode}): ${responseText}`);
      }
      const queryResult = JSON.parse(responseText);
      if (queryResult.results) {
        documents = queryResult.results; // SELECT RAW returns an array of documents
      }
      Logger.log('getData: Successfully retrieved %s documents via Query Service.', documents.length);
      
    } else {
      throwUserError('Invalid configuration mode specified.');
    }

    // Helper function to get nested values
    function getNestedValue(obj, path) {
      const parts = path.replace(/[(\d+)]/g, '.$1').split('.');
      let current = obj;
      for (let i = 0; i < parts.length; i++) {
        if (current === null || current === undefined) return null;
        const key = parts[i];
        if (!isNaN(key) && Array.isArray(current)) {
          current = parseInt(key, 10) < current.length ? current[parseInt(key, 10)] : null;
        } else {
          current = current[key];
        }
      }
      return current;
    }

    const rows = documents.map(doc => {
      const values = [];
      requestedFieldsArray.forEach(field => {
        const fieldName = field.getId();
        const fieldType = field.getType(); // From schema
        let value = getNestedValue(doc, fieldName);
        let formattedValue = null;
        if (value !== null && value !== undefined) {
          switch (fieldType) {
            case DataStudioApp.createCommunityConnector().FieldType.NUMBER:
              formattedValue = Number(value);
              if (isNaN(formattedValue)) formattedValue = null;
              break;
            case DataStudioApp.createCommunityConnector().FieldType.BOOLEAN:
              if (typeof value === 'string') {
                const lower = value.toLowerCase();
                formattedValue = lower === 'true' ? true : (lower === 'false' ? false : null);
              } else {
                formattedValue = Boolean(value);
              }
              break;
            default: // STRING, URL, TEXT etc.
              formattedValue = (typeof value === 'object') ? JSON.stringify(value) : String(value);
              break;
          }
        } else {
          formattedValue = ''; // Default for null/undefined as per original logic
        }
        values.push(formattedValue);
      });
      return { values };
    });

    Logger.log('getData: Final rows sample (first %s): %s', Math.min(3, rows.length), JSON.stringify(rows.slice(0, 3)));
    return {
      schema: requestedFieldsObject.build(),
      rows: rows
    };

  } catch (e) {
    Logger.log('Error in getData: %s. Stack: %s', e.message, e.stack);
    throwUserError(`Error retrieving data: ${e.message}`);
  }
}

// ==========================================================================
// ===                            UTILITIES                               ===
// ==========================================================================

/**
 * Constructs a full API URL from a user-provided path.
 */
function constructApiUrl(path) {
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
  
  // The path provided by the user should now contain the host and optionally the port.
  // We no longer append a default port. If the service is not on 443,
  // the user must specify it in the path, e.g., "mycouchbase.local:18095".
  // For Capella/sandbox URLs, they operate on 443 by default.
  Logger.log('constructApiUrl: Using host and port as provided (or default 443 if no port specified): %s', hostAndPort);
  
  return 'https://' + hostAndPort;
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
