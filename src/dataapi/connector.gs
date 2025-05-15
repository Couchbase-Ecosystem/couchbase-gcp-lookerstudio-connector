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
        Logger.log('getConfig (collection mode): Collection is selected (%s), setting isStepped = false.', selectedCollection);
      } else {
        Logger.log('getConfig (collection mode): Collection NOT selected, isStepped = true.');
      }
      
      // Only add maxRows if config is complete for this mode
      if (!isStepped) { // This means if isStepped is false
        config
          .newTextInput()
          .setId('maxRows')
          .setName('Maximum Rows')
          .setHelpText('Maximum number of rows to return (default: 100)')
          .setPlaceholder('100')
          .setAllowOverride(true);
        Logger.log('getConfig (collection mode): isStepped is false, adding maxRows input.');
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
      Logger.log('getConfig (customQuery mode): Setting isStepped = false.');
    }

    // Set the stepped config status for the response
    config.setIsSteppedConfig(isStepped);
    Logger.log('getConfig: Final setIsSteppedConfig to: %s', isStepped);

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
 * Gets the requested fields from the request fields provided by Looker Studio,
 * using a master schema definition for type information.
 *
 * @param {Array} requestFields The request.fields array from Looker Studio's getData request.
 * @param {Array} masterSchema The complete schema definition from getSchema().
 * @return {Fields} The Looker Studio Fields object for the response.
 */
function getRequestedFields(requestFields, masterSchema) { 
  const cc = DataStudioApp.createCommunityConnector();
  const requestedFieldsObject = cc.getFields();

  Logger.log('getRequestedFields: Called with masterSchema. Processing request.fields: %s', JSON.stringify(requestFields));

  if (!requestFields || requestFields.length === 0) {
    Logger.log('getRequestedFields: No specific fields in requestFields. Building response fields from masterSchema as fallback.');
    if (masterSchema && masterSchema.length > 0) {
        masterSchema.forEach(fieldDef => {
            let fieldTypeEnum = cc.FieldType.TEXT;
            if (fieldDef.dataType === 'NUMBER') fieldTypeEnum = cc.FieldType.NUMBER;
            else if (fieldDef.dataType === 'BOOLEAN') fieldTypeEnum = cc.FieldType.BOOLEAN;
            else if (fieldDef.dataType === 'URL') fieldTypeEnum = cc.FieldType.URL;

            if (fieldDef.semantics.conceptType === 'METRIC') {
                requestedFieldsObject.newMetric().setId(fieldDef.name).setName(fieldDef.name).setType(fieldTypeEnum);
            } else {
                requestedFieldsObject.newDimension().setId(fieldDef.name).setName(fieldDef.name).setType(fieldTypeEnum);
            }
        });
    }
    return requestedFieldsObject; // Return possibly populated object if masterSchema was used
  }

  requestFields.forEach(requestedFieldInfo => { // This is an item from request.fields array
    const fieldName = requestedFieldInfo.name;
    const fieldDefinition = masterSchema.find(f => f.name === fieldName);

    let fieldTypeEnum = cc.FieldType.TEXT;
    let conceptType = 'DIMENSION';

    if (fieldDefinition) {
      conceptType = fieldDefinition.semantics.conceptType;
      switch (fieldDefinition.dataType) {
        case 'NUMBER': fieldTypeEnum = cc.FieldType.NUMBER; break;
        case 'BOOLEAN': fieldTypeEnum = cc.FieldType.BOOLEAN; break;
        case 'URL': fieldTypeEnum = cc.FieldType.URL; break;
        case 'STRING': 
        default: fieldTypeEnum = cc.FieldType.TEXT; break;
      }
      Logger.log('getRequestedFields: Mapped %s to LookerType: %s, Concept: %s', fieldName, fieldDefinition.dataType, conceptType);
    } else {
      Logger.log('getRequestedFields: WARNING - Requested field %s not found in masterSchema. Defaulting to TEXT/DIMENSION.', fieldName);
    }

    if (conceptType === 'METRIC') {
      requestedFieldsObject.newMetric().setId(fieldName).setName(fieldName).setType(fieldTypeEnum);
    } else {
      requestedFieldsObject.newDimension().setId(fieldName).setName(fieldName).setType(fieldTypeEnum);
    }
  });

  Logger.log('getRequestedFields: Constructed Fields object for getData response: %s', JSON.stringify(requestedFieldsObject.asArray()));
  return requestedFieldsObject;
}

/**
 * Helper function to process the output of an INFER N1QL query.
 *
 * @param {Array} inferQueryResult The 'results' array from the INFER N1QL query response.
 * @return {Array} An array of Looker Studio field definitions.
 */
function processInferSchemaOutput(inferQueryResult) {
  Logger.log('processInferSchemaOutput: Received INFER results: %s', JSON.stringify(inferQueryResult));

  if (!inferQueryResult || inferQueryResult.length === 0 || !inferQueryResult[0] || inferQueryResult[0].length === 0) {
    Logger.log('processInferSchemaOutput: INFER query returned no flavors or empty result.');
    return [{ name: 'empty_infer_result', label: 'INFER result is empty', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}];
  }

  const firstFlavor = inferQueryResult[0][0];

  if (!firstFlavor || !firstFlavor.properties) {
    Logger.log('processInferSchemaOutput: First flavor has no properties.');
    return [{ name: 'no_properties_in_flavor', label: 'No properties in INFER result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}];
  }

  const schemaFields = [];

  function extractFieldsFromProperties(properties, prefix = '') {
    Object.keys(properties).forEach(key => {
      const fieldDef = properties[key];
      const fieldName = prefix ? `${prefix}.${key}` : key;
      let dataType = 'STRING'; 
      let conceptType = 'DIMENSION'; 

      const inferTypes = Array.isArray(fieldDef.type) ? fieldDef.type : [fieldDef.type];

      if (inferTypes.includes('number') || inferTypes.includes('integer')) {
        dataType = 'NUMBER';
        conceptType = 'METRIC';
      } else if (inferTypes.includes('boolean')) {
        dataType = 'BOOLEAN';
      } else if (inferTypes.includes('string')) {
        // Check for URL, but be cautious with empty string samples
        let isPotentiallyUrl = false;
        let hasNonEmptyUrlSample = false;
        let hasEmptyStringSample = false;

        if (fieldDef.samples && fieldDef.samples.length > 0) {
          fieldDef.samples.forEach(sample => {
            if (typeof sample === 'string') {
              if (sample.startsWith('http://') || sample.startsWith('https://')) {
                isPotentiallyUrl = true;
                hasNonEmptyUrlSample = true;
              } else if (sample === '') {
                hasEmptyStringSample = true;
              }
            }
          });
        }
        
        // If it looks like a URL field but contains empty strings, treat as STRING to be safe.
        // Only treat as URL if at least one sample is a valid-looking URL and no empty strings are present,
        // or if all string samples are valid URLs.
        // More robust: if it contains http(s):// AND empty strings, it's safer to make it STRING.
        // If it contains http(s):// and NO empty strings, it can be URL.
        if (isPotentiallyUrl && hasNonEmptyUrlSample) {
            if (hasEmptyStringSample) {
                Logger.log('processInferSchemaOutput: Field [%s] has URL-like samples and empty strings. Defaulting to STRING.', fieldName);
                dataType = 'STRING';
            } else {
                dataType = 'URL';
            }
        } else {
            dataType = 'STRING';
        }

      } else if (inferTypes.includes('object') && fieldDef.properties) {
        extractFieldsFromProperties(fieldDef.properties, fieldName);
        return; 
      } else if (inferTypes.includes('array')) {
        dataType = 'STRING';
      }
      
      schemaFields.push({
        name: fieldName,
        label: fieldName, 
        dataType: dataType,
        semantics: { conceptType: conceptType }
      });
    });
  }

  extractFieldsFromProperties(firstFlavor.properties);
  
  if (schemaFields.length === 0) {
      Logger.log('processInferSchemaOutput: Warning: Schema inference from INFER resulted in zero fields.');
      return [{ name: 'empty_infer_properties', label: 'INFER properties empty', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}];
  }

  Logger.log('processInferSchemaOutput: Final schema fields from INFER: %s', JSON.stringify(schemaFields));
  return schemaFields;
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
    const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);
    let inferResults;

    if (configParams.configMode === 'customQuery') {
      if (!configParams.query || configParams.query.trim() === '') {
        throwUserError('Custom query must be specified in "Use Custom Query" mode.');
      }
      
      // For custom queries, we still fetch one document to infer schema.
      // Using INFER on an arbitrary N1QL query result is not directly supported by INFER.
      // INFER works on keyspaces. So, the old method of SELECT LIMIT 1 is more appropriate here.
      // Or, we could require the user to also specify a keyspace if they want INFER on custom query.
      // For now, let's keep the existing logic for customQuery for schema inference.
      
      let userQuery = configParams.query.trim();
      if (!userQuery.toLowerCase().includes('limit')) {
        userQuery += ' LIMIT 1';
      }
      Logger.log('getSchema (customQuery): Running custom query for schema inference: %s', userQuery);
      
      const queryServiceUrl = `${apiUrl}/_p/query/query/service`;
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
        throwUserError(`Couchbase Query API error for custom query schema (${response.getResponseCode()}): ${response.getContentText()}`);
      }
      
      const queryResult = JSON.parse(response.getContentText());
      if (!queryResult.results || queryResult.results.length === 0) {
        Logger.log('getSchema (customQuery): Custom query returned no results for schema inference.');
        return { schema: [{ name: 'empty_custom_query_result', label: 'Empty Custom Query Result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
      }
      
      // Manually process the single document for schema (old way for custom queries)
      const documentForSchemaInference = queryResult.results[0];
      Logger.log('getSchema (customQuery): Successfully retrieved sample document via custom query.');
      
      // --- Reusing the old processFields for custom query mode ---
      function processFieldsForCustomQuery(obj, prefix = '') { // Renamed to avoid conflict
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
            dataType = 'STRING'; 
          } else if (typeof value === 'object') {
            fields.push(...processFieldsForCustomQuery(value, fieldName));
            return; 
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
      const schemaFields = processFieldsForCustomQuery(documentForSchemaInference);
      // --- End of old processFields for custom query mode ---

      if (schemaFields.length === 0) {
        Logger.log('Warning: Schema inference for custom query resulted in zero fields.');
        return { schema: [{ name: 'empty_custom_query_schema', label: 'Empty Custom Query Schema', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
      }
      Logger.log('getSchema (customQuery): Final inferred schema: %s', JSON.stringify(schemaFields));
      return { schema: schemaFields };

    } else if (configParams.configMode === 'collection') {
      if (!configParams.collection || configParams.collection.trim() === '') {
        throwUserError('Collection must be specified in "Query by Collection" mode.');
      }
      
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length !== 3) {
        throwUserError('Invalid collection path. Format: bucket.scope.collection');
      }
      // These are the raw parts, e.g., "travel-sample", "inventory", "landmark"
      const rawBucket = collectionParts[0];
      const rawScope = collectionParts[1];
      const rawCollection = collectionParts[2];
      
      // For INFER statement, keyspace path needs backticks
      const keyspacePathForInfer = `\`${rawBucket}\`.\`${rawScope}\`.\`${rawCollection}\``;
      const inferWithOptions = `WITH {\"sample_size\": 100, \"num_sample_values\": 3, \"similarity_metric\": 0.6}`;
      const actualInferStatement = `INFER ${keyspacePathForInfer} ${inferWithOptions}`;
      
      Logger.log('getSchema (collectionMode): Retrieving schema via INFER statement.');
      Logger.log('getSchema (collectionMode): Statement (intended): %s', actualInferStatement);

      // Ensure inferResults is assigned the result of executing the *actualInferStatement*
      inferResults = executeN1qlQuery(apiUrl, authHeader, actualInferStatement);

      if (inferResults === null) {
        throwUserError('Failed to execute INFER query. Check logs for N1QL error details.');
      }
      // executeN1qlQuery returns the 'results' array from the JSON response.
      // For INFER, this 'results' array itself contains the schema structure.
      // Specifically, results: [ [ flavor1, flavor2, ... ] ]

      const schemaFields = processInferSchemaOutput(inferResults);
      Logger.log('getSchema (collectionMode): Final schema from INFER: %s', JSON.stringify(schemaFields));
      return { schema: schemaFields };

    } else {
      throwUserError('Invalid configuration mode for schema inference.');
    }
    
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

    // Call getSchema ONCE to get the master schema definition
    const masterSchema = getSchema(request).schema; 
    if (!masterSchema || masterSchema.length === 0) {
        throwUserError('Failed to obtain a valid master schema for getData.');
    }
    Logger.log('getData: Obtained masterSchema with %s fields.', masterSchema.length);

    // Get the Fields object for the fields Looker Studio is actually requesting for this getData call
    const requestedFieldsObject = getRequestedFields(request.fields, masterSchema); 
    // build() returns the array of fields {name, label, dataType, semantics} for the response schema
    const schemaForResponse = requestedFieldsObject.build(); 

    let documents = [];
    const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);

    if (configParams.configMode === 'collection') {
        if (!configParams.collection || configParams.collection.trim() === '') {
            throwUserError('Collection must be specified in "Query by Collection" mode.');
        }
        const collectionParts = configParams.collection.split('.');
        if (collectionParts.length !== 3) {
            throwUserError('Invalid collection path. Format: bucket.scope.collection');
        }
        
        // Define variables for each part of the keyspace path, with backticks
        const bucketName = `\`${collectionParts[0]}\``;     // e.g., `travel-sample`
        const scopeName = `\`${collectionParts[1]}\``;        // e.g., `inventory`
        const collectionName = `\`${collectionParts[2]}\``; // e.g., `hotel` or `landmark`
        
        const maxRows = parseInt(configParams.maxRows, 10) || 100;
              
        const statement = `SELECT RAW ${collectionName} FROM ${bucketName}.${scopeName}.${collectionName} LIMIT ${maxRows}`;
        
        Logger.log('getData (collectionMode): Retrieving documents. Statement: %s', statement);
        const queryResults = executeN1qlQuery(apiUrl, authHeader, statement);

        if (queryResults === null) {
            throwUserError('Failed to retrieve documents for getData. Check logs for query error details.');
        }
        documents = queryResults; 
        Logger.log('getData (collectionMode): Successfully retrieved %s documents.', documents.length);

    } else if (configParams.configMode === 'customQuery') {
        if (!configParams.query || configParams.query.trim() === '') {
            throwUserError('Custom query must be specified in "Use Custom Query" mode.');
        }
        let userQuery = configParams.query.trim(); 
        // Note: LIMIT for custom queries should ideally be part of the query itself for predictability.
        // Appending a default LIMIT here if not present might alter user's intended query.
        // Consider advising users to include LIMIT in custom queries.
        Logger.log('getData (customQueryMode): Executing custom query: %s', userQuery);
        const queryResults = executeN1qlQuery(apiUrl, authHeader, userQuery);
        if (queryResults === null) {
            throwUserError('Failed to retrieve documents for custom query. Check logs for N1QL error details.');
        }
        documents = queryResults;
        Logger.log('getData (customQueryMode): Successfully retrieved %s documents.', documents.length);
    } else {
      throwUserError('Invalid configuration mode specified for getData.');
    }

    function getNestedValue(obj, pathString) { 
      const parts = pathString.replace(/[\[(\d+)\]]/g, '.$1').split('.');
      let current = obj;
      for (let i = 0; i < parts.length; i++) {
        if (current === null || current === undefined) return null;
        const key = parts[i];
        if (!isNaN(key) && Array.isArray(current)) { 
          current = parseInt(key, 10) < current.length ? current[parseInt(key, 10)] : null;
        } else if (typeof current === 'object' && current !== null) { // Ensure current is an object and not null
          current = current[key];
        } else {
          return null; 
        }
      }
      return current;
    }

    const rows = documents.map(doc => {
      const values = [];
      // Iterate over schemaForResponse, which is the array of plain field definition objects
      // from requestedFieldsObject.build()
      schemaForResponse.forEach(fieldDefinition => { 
        const fieldName = fieldDefinition.name; // Get the name from the plain object
        
        // Find the field's definition from our masterSchema to know its true dataType
        // (masterFieldDefinition was already used to build schemaForResponse, 
        //  so fieldDefinition itself contains the necessary dataType and semantics)
        const lookerDataType = fieldDefinition.dataType; // Use dataType from our built schemaForResponse

        let value = getNestedValue(doc, fieldName);
        let formattedValue = null;

        if (value !== null && value !== undefined) {
          switch (lookerDataType) { 
            case 'NUMBER':
              formattedValue = Number(value);
              if (isNaN(formattedValue)) formattedValue = null;
              break;
            case 'BOOLEAN':
              if (typeof value === 'string') {
                const lower = value.toLowerCase();
                formattedValue = lower === 'true' ? true : (lower === 'false' ? false : null);
              } else {
                formattedValue = Boolean(value);
              }
              break;
            case 'URL': 
                 formattedValue = String(value);
                 break;
            case 'STRING': 
            default:
              formattedValue = (typeof value === 'object') ? JSON.stringify(value) : String(value);
              break;
          }
        } else {
          formattedValue = null; 
        }
        values.push(formattedValue);
      });
      return { values };
    });

    Logger.log('getData: Final rows sample (first %s): %s', Math.min(3, rows.length), JSON.stringify(rows.slice(0, 3)));
    
    return {
      schema: schemaForResponse, 
      rows: rows
    };

  } catch (e) {
    Logger.log('Error in getData: %s. Stack: %s', e.message, e.stack);
    const errorMessage = typeof e.getText === 'function' ? e.getText() : e.message;
    throwUserError(`Error retrieving data: ${errorMessage}`);
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
