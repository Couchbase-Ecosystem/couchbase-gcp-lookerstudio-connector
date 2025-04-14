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
  Logger.log('Attempting to validate credentials for path: %s, username: %s', path, username);
  if (!path || !username || !password) {
    Logger.log('Validation failed: Missing path, username, or password.');
    return false; 
  }

  let apiBaseUrl = path;
  // Force HTTPS and remove common query/analytics ports to target standard 443
  if (apiBaseUrl.startsWith('couchbases://')) {
    apiBaseUrl = 'https://' + apiBaseUrl.substring('couchbases://'.length);
  } else if (apiBaseUrl.startsWith('couchbase://')) {
    apiBaseUrl = 'https://' + apiBaseUrl.substring('couchbase://'.length); // Force HTTPS
  } else if (!apiBaseUrl.startsWith('https://')) {
    apiBaseUrl = 'https://' + apiBaseUrl;
  }
  apiBaseUrl = apiBaseUrl.replace(/:(8091|8093|8095|18091|18093|18095)$/, '');
  
  const queryUrl = apiBaseUrl.replace(/\/$/, '') + '/query/service';
  Logger.log('Validation query URL (forced port 443): %s', queryUrl);

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
  
  Logger.log('isAuthValid: Found credentials. Attempting validation...');
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

  Logger.log('Received path: %s, username: %s, password: %s', path, username, '********');

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
 * Returns the user configurable options for the connector.
 */
function getConfig(request) {
  const cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText('Enter your Couchbase connection details and N1QL query. Use a publicly accessible URL (e.g., Capella or a public IP/domain), not localhost. The query should return a consistent schema.');

  config
    .newTextInput()
    .setId('bucket')
    .setName('Bucket Name')
    .setHelpText('The name of the Couchbase bucket to query')
    .setPlaceholder('default')
    .setAllowOverride(true);

  config
    .newTextInput()
    .setId('scope')
    .setName('Scope (Optional)')
    .setHelpText('The name of the scope within the bucket (e.g., inventory). Defaults to _default if blank.')
    .setPlaceholder('_default')
    .setAllowOverride(true);

  config
    .newTextInput()
    .setId('collection')
    .setName('Collection (Optional)')
    .setHelpText('The name of the collection within the scope (e.g., airport). Defaults to _default if blank.')
    .setPlaceholder('_default')
    .setAllowOverride(true);

  config
    .newTextArea()
    .setId('query')
    .setName('N1QL Query')
    .setHelpText('Enter a valid N1QL query (e.g., SELECT * FROM `travel-sample`.inventory.airport LIMIT 100). The query should return a consistent schema.')
    .setPlaceholder('SELECT * FROM `travel-sample`.inventory.airport LIMIT 100')
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
  
  // Validate required connector-specific config parameters
  if (!configParams.bucket) {
    throwUserError('Bucket name is required.');
  }
  if (!configParams.query) {
    throwUserError('N1QL query is required.');
  }
  
  // Set default values for scope and collection if not provided
  if (!configParams.scope) {
    configParams.scope = '_default';
  }
  if (!configParams.collection) {
    configParams.collection = '_default';
  }
  
  // Optional: Check Capella URL format
  if (path.includes('cloud.couchbase.com') && !path.startsWith('couchbases://') && !path.startsWith('https://')) {
      Logger.log('validateConfig Warning: Capella URL found without secure prefix: %s', path);
  }
  
  Logger.log('validateConfig successful for bucket: %s', configParams.bucket);
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
    const result = fetchData(request.configParams);
    const schema = buildSchema(result);
    return { schema: schema };
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
  const firstRow = result.results[0];

  let dataObject = firstRow;
  const keys = Object.keys(firstRow);
  if (keys.length === 1 && typeof firstRow[keys[0]] === 'object' && firstRow[keys[0]] !== null) {
    dataObject = firstRow[keys[0]];
    Logger.log('buildSchema: Data appears nested under key: %s', keys[0]);
  } else {
    Logger.log('buildSchema: Data appears to be at the top level.');
  }

  Object.keys(dataObject).forEach(function(key) {
    const value = dataObject[key];
    const type = typeof value;
    let dataType = 'STRING'; 
    let semantics = { conceptType: 'DIMENSION' };

    if (type === 'number') {
      dataType = 'NUMBER';
      semantics = { conceptType: 'METRIC', isReaggregatable: true };
    } else if (type === 'boolean') {
      dataType = 'BOOLEAN';
      semantics = { conceptType: 'DIMENSION' };
    } else if (value instanceof Date || (typeof value === 'string' && value.length > 10 && !isNaN(Date.parse(value)))) {
      dataType = 'YEAR_MONTH_DAY_HOUR';
      semantics = { conceptType: 'DIMENSION', semanticGroup: 'DATETIME' };
    }

    schema.push({
      name: key,
      label: key,
      dataType: dataType,
      semantics: semantics
    });
  });

  if (schema.length === 0) {
    throw new Error('Could not determine any fields from the first row of results. Check query and data structure.');
  }
  
  Logger.log('buildSchema successful. Detected fields: %s', schema.map(f => f.name).join(', '));
  return schema;
}

/**
 * Returns the tabular data for the given request.
 */
function getData(request) {
  request.configParams = validateConfig(request.configParams);
  
  try {
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
  const username = configParams.username;
  const password = configParams.password;
  const baseUrl = configParams.baseUrl;

  if (!username || !password || !baseUrl) {
     Logger.log('fetchData Error: Missing baseUrl, username, or password in configParams.');
     throw new Error('Configuration error: Connection details missing.');
  }

  const bucket = configParams.bucket;
  const scope = configParams.scope || '_default';
  const query = configParams.query;
  const timeout = 30000; 

  let apiBaseUrl = baseUrl;
  // Force HTTPS and remove common query/analytics ports
  if (apiBaseUrl.startsWith('couchbases://')) {
    apiBaseUrl = 'https://' + apiBaseUrl.substring('couchbases://'.length);
  } else if (apiBaseUrl.startsWith('couchbase://')) {
    apiBaseUrl = 'https://' + apiBaseUrl.substring('couchbase://'.length); // Force HTTPS
  } else if (!apiBaseUrl.startsWith('https://')) {
    apiBaseUrl = 'https://' + apiBaseUrl;
  }
  apiBaseUrl = apiBaseUrl.replace(/:(8091|8093|8095|18091|18093|18095)$/, '');
  
  const queryUrl = apiBaseUrl.replace(/\/$/, '') + '/query/service';
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
    // Fallback if getSchema fails here
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
