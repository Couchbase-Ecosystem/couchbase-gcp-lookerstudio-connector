/**
 * Couchbase Connector for Google Looker Studio
 * This connector allows users to connect to a Couchbase database and run N1QL queries.
 */

// Hardcoded credentials (replace with your actual Capella details for testing)
const HARDCODED_URL = "xyz"; // Include port if needed
const HARDCODED_USERNAME = "xyz";
const HARDCODED_PASSWORD = "xyz";

/**
 * Returns the authentication method required by the connector.
 * Setting to NONE for hardcoded testing.
 */
function getAuthType() {
  const cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.NONE)
    .build();
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
    .setText('TESTING: Using hardcoded credentials. Enter Bucket, Scope, Collection, and Query.'); // Updated instructions

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
 * Validates config and adds hardcoded credentials.
 */
function validateConfig(configParams) {
  configParams = configParams || {};
  
  // Add hardcoded credentials for use in fetchData
  configParams.baseUrl = HARDCODED_URL;
  configParams.username = HARDCODED_USERNAME;
  configParams.password = HARDCODED_PASSWORD;
  
  // Validate required config parameters (bucket and query still needed)
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
  
  // Removed credential checks and Capella URL check as URL is hardcoded
  
  return configParams;
}

/**
 * Returns the schema for the given request.
 */
function getSchema(request) {
  // Pass the user config directly to validateConfig which adds hardcoded creds
  request.configParams = validateConfig(request.configParams);
  
  try {
    const result = fetchData(request.configParams); // Pass the modified configParams
    const schema = buildSchema(result);
    return { schema: schema };
  } catch (e) {
    throwUserError(e);
  }
}

/**
 * Builds schema from the query results.
 *
 * @param {Object} result Query result.
 * @returns {Array} Schema fields.
 */
function buildSchema(result) {
  if (!result?.results?.length) {
    throwUserError('Query returned no results. Cannot build schema.');
  }
  
  const schema = [];
  const firstRow = result.results[0];
  
  Object.keys(firstRow).forEach(function(key) {
    const value = firstRow[key];
    const type = typeof value;
    
    if (type === 'number') {
      schema.push({
        name: key,
        label: key,
        dataType: 'NUMBER',
        semantics: {
          conceptType: 'METRIC',
          isReaggregatable: true
        }
      });
    } else if (type === 'boolean') {
      schema.push({
        name: key,
        label: key,
        dataType: 'BOOLEAN',
        semantics: {
          conceptType: 'DIMENSION'
        }
      });
    } else if (value instanceof Date || (typeof value === 'string' && !isNaN(Date.parse(value)))) {
      schema.push({
        name: key,
        label: key,
        dataType: 'YEAR_MONTH_DAY_HOUR',
        semantics: {
          conceptType: 'DIMENSION',
          semanticGroup: 'DATETIME'
        }
      });
    } else {
      schema.push({
        name: key,
        label: key,
        dataType: 'STRING',
        semantics: {
          conceptType: 'DIMENSION'
        }
      });
    }
  });
  
  return schema;
}

/**
 * Returns the tabular data for the given request.
 */
function getData(request) {
  // Pass the user config directly to validateConfig which adds hardcoded creds
  request.configParams = validateConfig(request.configParams);
  
  try {
    const result = fetchData(request.configParams); // Pass the modified configParams
    
    if (!result?.results?.length) {
      return {
        schema: [],
        rows: []
      };
    }
    
    const schema = buildSchema(result);
    const requestedFieldIds = request.fields.map(field => field.name);
    const requestedFields = schema.filter(field => requestedFieldIds.indexOf(field.name) > -1);
    
    const rows = result.results.map(row => {
      const values = requestedFieldIds.map(fieldId => row[fieldId] !== undefined ? row[fieldId] : null);
      return { values };
    });
    
    return {
      schema: requestedFields,
      rows: rows
    };
  } catch (e) {
    throwUserError(e);
  }
}

/**
 * Fetches data from Couchbase using the provided configuration (with hardcoded credentials).
 */
function fetchData(configParams) {
  // Use hardcoded credentials directly
  const username = HARDCODED_USERNAME;
  const password = HARDCODED_PASSWORD;
  const baseUrl = HARDCODED_URL;

  // Get other config params provided by the user
  const bucket = configParams.bucket;
  const scope = configParams.scope || '_default';
  const collection = configParams.collection || '_default';
  const query = configParams.query;
  const timeout = 30000; // Default timeout of 30 seconds
  
  // Convert Couchbase connection string to HTTP endpoint for query service
  let apiBaseUrl = baseUrl;
  if (apiBaseUrl.startsWith('couchbases://')) {
    apiBaseUrl = 'https://' + apiBaseUrl.substring('couchbases://'.length);
  } else if (apiBaseUrl.startsWith('couchbase://')) {
    apiBaseUrl = 'http://' + apiBaseUrl.substring('couchbase://'.length);
  }
  if (!apiBaseUrl.startsWith('http://') && !apiBaseUrl.startsWith('https://')) {
     // Simplified assumption: if it contains cloud.couchbase, use https, otherwise http
    if (apiBaseUrl.includes('cloud.couchbase.com')) {
      apiBaseUrl = 'https://' + apiBaseUrl;
    } else {
      apiBaseUrl = 'http://' + apiBaseUrl;
    }
  }
  
  const queryUrl = apiBaseUrl.replace(/\/$/, '') + '/query/service';
  
  // Build the query context: default:`bucket`.`scope`
  // Note: Backticks are important if names contain special characters.
  let queryContext = `default:\`${bucket}\``; // Start with namespace and bucket
  if (scope && scope !== '_default') { // Add scope if it's provided and not the default
    queryContext += `.\`${scope}\``;
  }
  // Collection is typically specified within the N1QL statement (FROM clause),
  // not usually needed in the query_context parameter itself.
  
  // Prepare the query payload
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
    // Keep validateHttpsCertificates: false for now based on previous testing
    validateHttpsCertificates: false 
  };
  
  try {
    const response = UrlFetchApp.fetch(queryUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();
    
    if (responseCode !== 200) {
      // Log detailed error
      Logger.log('Error querying Couchbase (No-Auth Test). URL: %s, Code: %s, Response: %s', queryUrl, responseCode, responseText);
      throwUserError('Error querying Couchbase (No-Auth Test): [Code: ' + responseCode + '] ' + responseText);
    }
    
    Logger.log('fetchData successful (No-Auth Test) for URL: %s', queryUrl);
    return JSON.parse(responseText);
  } catch (e) {
    Logger.log('Error connecting to Couchbase during fetchData (No-Auth Test). URL: %s, Exception: %s', queryUrl, e.toString());
    Logger.log('fetchData (No-Auth Test) Exception details: %s', e.stack);
    throwUserError('Error connecting to Couchbase (No-Auth Test): ' + e.toString());
  }
}

/**
 * Throws a user-friendly error message.
 *
 * @param {string} message Error message.
 */
function throwUserError(message) {
  // Ensure cc is defined if it wasn't globally
  const cc = DataStudioApp.createCommunityConnector(); 
  cc.newUserError()
    .setText(message)
    .throwException();
}

/**
 * Returns whether the current user is an admin user.
 *
 * @returns {boolean} Whether the current user is an admin user.
 */
function isAdminUser() {
  return false;
} 