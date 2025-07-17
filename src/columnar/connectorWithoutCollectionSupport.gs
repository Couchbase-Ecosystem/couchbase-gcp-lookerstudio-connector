/**
 * Couchbase Columnar Connector for Google Looker Studio (Views and Custom Query Only)
 * This connector allows users to connect to a Couchbase database and run Columnar queries.
 * Supports only Views and Custom Queries - Collection mode has been removed.
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
 * Fetches available databases, scopes, and views from Couchbase.
 * Used to populate dropdowns in the config UI.
 * Note: Collection support has been removed from this version.
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
      databases: [],
      scopesViews: {}
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
  
  // Initialize empty result structure - only views, no collections
  let databaseNames = [];
  const scopesViews = {};
  
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
      
      // Get databases (databases)
      const databaseQueryPayload = {
        statement: "SELECT DISTINCT DatabaseName FROM System.Metadata.`Dataset`",
        timeout: "10000ms"
      };
      
      options.payload = JSON.stringify(databaseQueryPayload);
      
      const databaseResponse = UrlFetchApp.fetch(queryUrl, options);
      
      if (databaseResponse.getResponseCode() === 200) {
        const databaseData = JSON.parse(databaseResponse.getContentText());
        
        if (databaseData.results && Array.isArray(databaseData.results)) {
          databaseNames = databaseData.results
            .filter(item => item.DatabaseName && item.DatabaseName !== 'System') // Filter out System database
            .map(item => item.DatabaseName);
          
          Logger.log('fetchCouchbaseMetadata: Found databases: %s', databaseNames.join(', '));
        }
      }
      
      // Get only views (no collections)
      const viewsQueryPayload = {
        statement: "SELECT DatabaseName, DataverseName, DatasetName FROM System.Metadata.`Dataset` WHERE DatabaseName != 'System' AND DatasetType = 'VIEW'",
        timeout: "10000ms"
      };
      
      options.payload = JSON.stringify(viewsQueryPayload);
      
      const viewsResponse = UrlFetchApp.fetch(queryUrl, options);
      
      if (viewsResponse.getResponseCode() === 200) {
        const viewsData = JSON.parse(viewsResponse.getContentText());
        
        if (viewsData.results && Array.isArray(viewsData.results)) {
          Logger.log('fetchCouchbaseMetadata: Found %s views in metadata', viewsData.results.length);
          
          // Initialize database structures
          databaseNames.forEach(database => {
            scopesViews[database] = {};
          });
          
          // Process views data only
          viewsData.results.forEach(item => {
            if (item.DatabaseName && item.DataverseName && item.DatasetName) {
              const database = item.DatabaseName;
              const scope = item.DataverseName;
              const viewName = item.DatasetName;
              
              // Skip non-matching databases
              if (!databaseNames.includes(database)) {
                return;
              }
              
              // Process as view
              if (!scopesViews[database][scope]) {
                scopesViews[database][scope] = [];
              }
              
              if (!scopesViews[database][scope].includes(viewName)) {
                scopesViews[database][scope].push(viewName);
                Logger.log('fetchCouchbaseMetadata: Added view: %s.%s.%s', 
                          database, scope, viewName);
              }
            }
          });
        }
      }
    } else {
      // Fall back to legacy approach - but only look for views
      Logger.log('fetchCouchbaseMetadata: System.Metadata is not accessible, using legacy approach (views only)');
      
      // For legacy systems, we can't easily distinguish views from collections
      // So we'll just get databases and let users specify manually
      const databaseQueryPayload = {
        statement: "SELECT DISTINCT SPLIT_PART(keyspace_id, ':', 1) AS database FROM system:keyspaces WHERE SPLIT_PART(keyspace_id, ':', 1) != 'system';",
        timeout: "10000ms"
      };
      
      // First get all databases
      options.payload = JSON.stringify(databaseQueryPayload);
      Logger.log('fetchCouchbaseMetadata: Querying for databases (legacy)');
      
      const databaseResponse = UrlFetchApp.fetch(queryUrl, options);
      
      if (databaseResponse.getResponseCode() === 200) {
        const databaseData = JSON.parse(databaseResponse.getContentText());
        
        if (databaseData.results && Array.isArray(databaseData.results)) {
          databaseNames = databaseData.results
            .filter(item => item.database) // Filter out any null or undefined
            .map(item => item.database);
          
          Logger.log('fetchCouchbaseMetadata: Found databases: %s', databaseNames.join(', '));
        } else {
          Logger.log('fetchCouchbaseMetadata: Database query result format unexpected or empty.');
        }
      } else {
        Logger.log('Error fetching databases. Code: %s, Response: %s', 
                  databaseResponse.getResponseCode(), databaseResponse.getContentText());
      }
      
      // Initialize empty view structures since we can't distinguish views in legacy mode
      databaseNames.forEach(databaseName => {
        scopesViews[databaseName] = {};
      });
    }

    return {
      databases: databaseNames,
      scopesViews: scopesViews
    };
    
  } catch (e) {
    Logger.log('Error in fetchCouchbaseMetadata: %s', e.toString());
    Logger.log('Exception details: %s', e.stack);
    return {
      databases: [],
      scopesViews: {}
    };
  }
}

/**
 * Returns the user configurable options for the connector.
 * Note: Collection support has been removed - only supports Views and Custom Queries.
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
      .setText('Choose a configuration mode: query by selecting a view (recommended) or enter a custom Columnar query.');

    const modeSelector = config.newSelectSingle()
      .setId('configMode')
      .setName('Configuration Mode')
      .setHelpText('Select how you want to define the data source.')
      .setAllowOverride(true)
      .setIsDynamic(true); // Changing mode should trigger refresh

    // Only view and custom query modes - collection support removed
    modeSelector.addOption(config.newOptionBuilder().setLabel('By View').setValue('view'));
    modeSelector.addOption(config.newOptionBuilder().setLabel('Use Custom Query').setValue('customQuery'));

    const currentMode = configParams.configMode ? configParams.configMode : 'view';
    Logger.log('getConfig: Current mode: %s', currentMode);

    if (currentMode === 'view') {
      // Add view-specific info message
      config.newInfo()
        .setId('view_info')
        .setText('Select the Database, Scope, and View to query. Views provide a stable, optimized interface for BI tools.');

      const metadata = fetchCouchbaseMetadata();
      Logger.log('getConfig (view mode): Metadata: %s', JSON.stringify(metadata));

      const databaseSelect = config.newSelectSingle()
        .setId('database')
        .setName('Couchbase Database')
        .setHelpText('Select the Couchbase Database.')
        .setAllowOverride(true)
        .setIsDynamic(true); // Changing database should trigger scope refresh
      
      if (metadata && metadata.databases && Array.isArray(metadata.databases)) {
        metadata.databases.forEach(databaseName => {
          if (databaseName) {
            databaseSelect.addOption(config.newOptionBuilder().setLabel(databaseName).setValue(databaseName));
          }
        });
      } else {
        Logger.log('getConfig: No databases found or metadata.databases is not an array.');
      }

      const selectedDatabase = configParams.database ? configParams.database : null;
      if (selectedDatabase && metadata && metadata.scopesViews && metadata.scopesViews[selectedDatabase]) {
        const scopeSelect = config.newSelectSingle()
          .setId('scope')
          .setName('Couchbase Scope')
          .setHelpText('Select the Couchbase Scope within the selected Database.')
          .setAllowOverride(true)
          .setIsDynamic(true); // Changing scope should trigger view refresh
        
        Object.keys(metadata.scopesViews[selectedDatabase]).forEach(scopeName => {
          if (scopeName) {
            scopeSelect.addOption(config.newOptionBuilder().setLabel(scopeName).setValue(scopeName));
          }
        });

        const selectedScope = configParams.scope ? configParams.scope : null;
        if (selectedScope && metadata.scopesViews[selectedDatabase][selectedScope] && Array.isArray(metadata.scopesViews[selectedDatabase][selectedScope])) {
          const viewSelect = config.newSelectSingle()
            .setId('viewName')
            .setName('Couchbase View')
            .setHelpText('Select the Couchbase View within the selected Scope.')
            .setAllowOverride(true); // View selection doesn't trigger further steps
          
          metadata.scopesViews[selectedDatabase][selectedScope].forEach(viewName => {
            if (viewName) {
              viewSelect.addOption(config.newOptionBuilder().setLabel(viewName).setValue(viewName));
            }
          });

          // If we have reached the point of showing the view dropdown,
          // check if a view has actually been selected to finalize the stepped config.
          const selectedView = configParams.viewName ? configParams.viewName : null;
          if (selectedView) { 
            isStepped = false; // Config is complete for view mode if view is selected
          } 
        }
      }

      // Only add maxRows if config is complete for view mode
      if (!isStepped) {
        config
          .newTextInput()
          .setId('maxRows')
          .setName('Maximum Rows')
          .setHelpText('Maximum number of rows to return (default: 1000).')
          .setPlaceholder('1000')
          .setAllowOverride(true);
      }

    } else if (currentMode === 'customQuery') {
      config.newInfo()
        .setId('custom_query_info')
        .setText('Enter your custom Columnar query below.');
      config
        .newTextArea()
        .setId('query')
        .setName('Custom Columnar Query')
        .setHelpText('Enter a valid Columnar query. Ensure you include a LIMIT clause if needed.')
        .setPlaceholder('SELECT airline.name, airline.iata, airline.country FROM `travel-sample`.`inventory`.`airline` AS airline WHERE airline.country = \"France\" LIMIT 100')
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
 * Note: Collection mode validation has been removed.
 *
 * @param {Object} configParams The user configuration parameters.
 * @return {Object} The validated configuration object.
 */
function validateConfig(configParams) {
  Logger.log('Validating config parameters: %s', JSON.stringify(configParams));
  
  if (!configParams || !configParams.configMode) {
    throwUserError('No configuration mode provided. Please select a mode.');
  }
  
  // Only allow view and customQuery modes
  if (configParams.configMode !== 'view' && configParams.configMode !== 'customQuery') {
    throwUserError('Invalid configuration mode selected. Only "view" and "customQuery" modes are supported.');
  }
  
  // Get credentials from user properties
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  if (!path || !username || !password) {
    throwUserError('Authentication credentials missing. Please reauthenticate.');
  }

  const validatedConfig = {
    path: path,
    username: username,
    password: password,
    configMode: configParams.configMode
  };
  
  if (configParams.configMode === 'view') {
    if (!configParams.database || configParams.database.trim() === '') {
      throwUserError('Database must be specified in "By View" mode.');
    }
    if (!configParams.scope || configParams.scope.trim() === '') {
      throwUserError('Scope must be specified in "By View" mode.');
    }
    if (!configParams.viewName || configParams.viewName.trim() === '') {
      throwUserError('View must be specified in "By View" mode.');
    }
    
    validatedConfig.database = configParams.database.trim();
    validatedConfig.scope = configParams.scope.trim();
    validatedConfig.viewName = configParams.viewName.trim();
    validatedConfig.maxRows = configParams.maxRows && parseInt(configParams.maxRows) > 0 ? 
                             parseInt(configParams.maxRows) : 1000;
  } else if (configParams.configMode === 'customQuery') {
    if (!configParams.query || configParams.query.trim() === '') {
      throwUserError('Custom query must be specified in "Use Custom Query" mode.');
    }
    validatedConfig.query = configParams.query.trim();
  }
  
  Logger.log('Config validation successful: %s', JSON.stringify(validatedConfig));
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
 * Note: Collection mode has been removed - only supports Views and Custom Queries.
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
    if (configParams.schemaFields && configParams.schemaFields.trim() !== '') {
      try {
        const schemaFields = JSON.parse(configParams.schemaFields);
        Logger.log('Using provided schema fields: %s', configParams.schemaFields);
        return { schema: schemaFields };
      } catch (e) {
        Logger.log('Error parsing schema fields: %s', e.message);
        // Continue to infer schema if provided schema is invalid
      }
    }

    // --- Schema Processing Helper Functions ---
    function mapInferredTypeToLookerType(inferredType) {
      if (Array.isArray(inferredType)) {
        if (inferredType.includes('string')) return 'STRING';
        if (inferredType.includes('number')) return 'NUMBER';
        if (inferredType.includes('boolean')) return 'BOOLEAN';
        return 'STRING'; // Default fallback for mixed or unknown arrays
      }
      // Handle single types
      switch (inferredType) {
        case 'number':
          return 'NUMBER';
        case 'boolean':
          return 'BOOLEAN';
        case 'string':
          return 'STRING';
        case 'object':
        case 'array':
        case 'null':
        default:
          return 'STRING'; // Treat objects, arrays, nulls, and unknowns as STRING
      }
    }

    function getConceptTypeFromLookerType(lookerType) {
      return lookerType === 'NUMBER' ? 'METRIC' : 'DIMENSION';
    }

    function processInferredProperties(properties, prefix = '') {
      const fields = [];
      if (!properties || typeof properties !== 'object') {
        return fields;
      }
      Object.keys(properties).forEach(key => {
        const fieldInfo = properties[key];
        const fieldName = prefix ? `${prefix}.${key}` : key;
        if (fieldInfo.type === 'object' && fieldInfo.properties) {
          // Recursively process nested objects
          fields.push(...processInferredProperties(fieldInfo.properties, fieldName));
        } else if (fieldInfo.type === 'array') {
           // Represent the whole array as a single STRING field
           fields.push({
             name: fieldName,
             label: fieldName,
             dataType: 'STRING', 
             semantics: { conceptType: 'DIMENSION' }
           });
           Logger.log('processInferredProperties: Added field for array (as STRING): %s', fieldName);
        } else {
          // Handle primitive types (string, number, boolean, null, or mixed)
          const lookerType = mapInferredTypeToLookerType(fieldInfo.type);
          const conceptType = getConceptTypeFromLookerType(lookerType);
          fields.push({
            name: fieldName,
            label: fieldName,
            dataType: lookerType,
            semantics: { conceptType: conceptType }
          });
          Logger.log('processInferredProperties: Added field: %s (Type: %s)', fieldName, lookerType);
        }
      });
      return fields;
    }
    // --- End of Schema Processing Helper Functions ---

    // --- Construct the array_infer_schema query ---
    let inferSchemaQuery = '';
    let targetCollectionPath = ''; 

    if (configParams.configMode === 'view') {
      if (!configParams.database || !configParams.scope || !configParams.viewName) {
        throwUserError('Database, Scope, and View must be selected to infer schema in "By View" mode.');
      }
      targetCollectionPath = `\`${configParams.database}\`.\`${configParams.scope}\`.\`${configParams.viewName}\``;
      inferSchemaQuery = `SELECT array_infer_schema((SELECT VALUE t FROM ${targetCollectionPath} AS t LIMIT 1000)) AS inferred_schema;`;
      Logger.log('getSchema: Inferring schema from view: %s', targetCollectionPath);
    } else if (configParams.configMode === 'customQuery') {
       if (!configParams.query || configParams.query.trim() === '') {
         throwUserError('Custom query must be specified to infer schema in "Use Custom Query" mode.');
       }
       let userQuery = configParams.query.trim().replace(/;$/, '');
       if (!userQuery.toLowerCase().includes('limit')) {
         userQuery += ' LIMIT 1000'; 
       }
       inferSchemaQuery = `SELECT array_infer_schema((${userQuery})) AS inferred_schema;`;
       Logger.log('getSchema: Inferring schema from custom query results.');
    } else {
      throwUserError('Invalid configuration mode. Only "view" and "customQuery" modes are supported.');
    }

    Logger.log('Schema inference query: %s', inferSchemaQuery);
    const columnarUrl = constructApiUrl(path, 18095);
    const apiUrl = columnarUrl + '/api/v1/request';

    const payload = {
      statement: inferSchemaQuery,
      timeout: '60s'
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

    const response = UrlFetchApp.fetch(apiUrl, options);
    const responseCode = response.getResponseCode();
    const responseBody = response.getContentText();

    Logger.log('getSchema: Raw API response (code %s): %s', responseCode, responseBody);

    if (responseCode !== 200) {
      Logger.log('API error in getSchema: %s, Error: %s', responseCode, responseBody);
      throwUserError(`Couchbase API error during schema inference (${responseCode}): ${responseBody}`);
    }

    let parsedResponse;
    try {
      parsedResponse = JSON.parse(responseBody);
    } catch (e) {
       Logger.log('Error parsing schema inference response: %s', e.message);
       throwUserError('Invalid response from Couchbase API during schema inference: ' + e.message);
    }

    if (!parsedResponse.results || parsedResponse.results.length === 0) {
      Logger.log('Schema inference query did not return results. Response: %s', responseBody);
      throwUserError('Schema inference failed: No results returned from the query.');
    }

    // Check if inferred_schema property exists but is null (empty collection case)
    if (!parsedResponse.results[0].hasOwnProperty('inferred_schema')) {
      Logger.log('Schema inference query did not return the expected structure. Response: %s', responseBody);
      throwUserError('Schema inference failed: Could not find inferred_schema in the response.');
    }

    const inferredSchemaArray = parsedResponse.results[0].inferred_schema;
    
    // Handle the case where inferred_schema is null (empty view/query result)
    if (inferredSchemaArray === null) {
      const entityType = configParams.configMode === 'view' ? 'view' : 'query result';
      const entityPath = configParams.configMode === 'customQuery' ? 'custom query' : 
                        `${configParams.database}.${configParams.scope}.${configParams.viewName}`;
      
      Logger.log('Schema inference returned null - empty %s: %s', entityType, entityPath);
      throwUserError(
        `The ${entityType} "${entityPath}" appears to be empty or does not exist. ` +
        `Please verify:\n` +
        `1. The ${entityType} exists and contains data\n` +
        `2. Your credentials have permission to access this ${entityType}\n` +
        `3. The database, scope, and ${entityType} names are correct`
      );
    }
    
    if (!Array.isArray(inferredSchemaArray) || inferredSchemaArray.length === 0) {
       Logger.log('Inferred schema array is empty or not an array.');
       throwUserError('Schema inference failed: Invalid inferred_schema structure.');
    }

    // Process ALL flavors from array_infer_schema, not just the first one
    // Each flavor represents a different document structure variant
    let allFields = [];
    const processedFieldNames = new Set();
    
    Logger.log('getSchema: Processing %d schema flavors from array_infer_schema', inferredSchemaArray.length);
    
    inferredSchemaArray.forEach((schemaDefinition, flavorIndex) => {
      if (!schemaDefinition || schemaDefinition.type !== 'object' || !schemaDefinition.properties) {
        Logger.log('getSchema: Flavor %d does not contain valid object properties, skipping', flavorIndex);
        return;
      }
      
      Logger.log('getSchema: Processing flavor %d with %d documents (%.1f%% of total)', 
                flavorIndex, schemaDefinition['#docs'], schemaDefinition['%docs']);
      
      const flavorFields = processInferredProperties(schemaDefinition.properties);
      
      // Add fields from this flavor that haven't been seen before
      flavorFields.forEach(field => {
        if (!processedFieldNames.has(field.name)) {
          allFields.push(field);
          processedFieldNames.add(field.name);
          Logger.log('getSchema: Added field from flavor %d: %s (Type: %s)', 
                    flavorIndex, field.name, field.dataType);
        } else {
          Logger.log('getSchema: Field %s already exists from previous flavor, skipping', field.name);
        }
      });
    });
    
    if (allFields.length === 0) {
       Logger.log('getSchema: No valid schema flavors found with properties.');
       throwUserError('Schema inference failed: Could not find properties in any schema flavor.');
    }

    let fields = allFields;

    if (fields.length === 0) {
       Logger.log('Warning: Schema inference resulted in zero fields. Check view/query and data.');
       return { schema: [{ name: 'empty_result', label: 'Empty Result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
    }

    Logger.log('getSchema: Final inferred schema: %s', JSON.stringify(fields));
    return { schema: fields };

  } catch (e) {
    Logger.log('Error in getSchema: %s', e.message);
    Logger.log('getSchema Error Stack: %s', e.stack);
    
    // Check if this is a user error that should be displayed to the user
    if (e.isUserError || e.name === 'UserError') {
      // Convert to Apps Script user error for proper display
      DataStudioApp.createCommunityConnector()
        .newUserError()
        .setText(e.message)
        .throwException();
    }
    
    // For genuine system errors, wrap with context
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText(`Error inferring schema: ${e.message || e.toString()}`)
      .throwException();
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
    } else if (typeof value === 'string') {
      // Check for URL first
      if (value.startsWith('http://') || value.startsWith('https://')) {
        potentialDataType = 'URL';
        potentialSemantics = { conceptType: 'DIMENSION' }; 
      } else {
        // Keep other strings as STRING/DIMENSION (including potential dates)
        potentialDataType = 'STRING';
        potentialSemantics = { conceptType: 'DIMENSION' };
      }
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
 * Returns the data for the given request.
 * Note: Collection mode has been removed - only supports Views and Custom Queries.
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
    
    if (!configParams.configMode) {
      throwUserError('Configuration mode not specified.');
    }
    
    // Only allow view and customQuery modes
    if (configParams.configMode !== 'view' && configParams.configMode !== 'customQuery') {
      throwUserError('Invalid configuration mode. Only "view" and "customQuery" modes are supported.');
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
    
    if (configParams.configMode === 'view') {
      // Construct query based on view and requested fields
      if (!configParams.database || !configParams.scope || !configParams.viewName) {
        throwUserError('Database, Scope, and View must be selected in "By View" mode.');
      }
      const viewPath = '`' + configParams.database + '`.`' + configParams.scope + '`.`' + configParams.viewName + '`';
      
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
      query = 'SELECT ' + selectClause + ' FROM ' + viewPath + ' LIMIT ' + (configParams.maxRows || 1000); // Use maxRows from config or default
    } else if (configParams.configMode === 'customQuery') {
      // Use custom query
      if (!configParams.query || configParams.query.trim() === '') {
        throwUserError('Custom query is missing in "Use Custom Query" mode.');
      }
      query = configParams.query.trim();
      Logger.log('getData: Using custom query as provided.');
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
    
    // Check if this is a user error that should be displayed to the user
    if (e.isUserError || e.name === 'UserError') {
      // Convert to Apps Script user error for proper display
      DataStudioApp.createCommunityConnector()
        .newUserError()
        .setText(e.message)
        .throwException();
    }
    
    // For genuine system errors, wrap with context
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText(`Error retrieving data: ${errorMessage}`)
      .throwException();
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
  // Create a custom error that preserves the message when caught
  const customError = new Error(message);
  customError.name = 'UserError';
  customError.isUserError = true;
  throw customError;
}

/**
 * Returns whether the current user is an admin user (currently unused).
 */
function isAdminUser() {
  return false;
} 