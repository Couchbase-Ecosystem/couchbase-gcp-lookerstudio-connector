/**
 * Couchbase Connector for Google Looker Studio
 * This connector allows users to connect to a Couchbase database and run N1QL queries.
 */

var connector = {};
connector.usernameKey = 'dscc.username';
connector.passwordKey = 'dscc.password';
connector.baseUrlKey = 'couchbase.baseUrl';
connector.bucketKey = 'couchbase.bucket';
connector.queryKey = 'couchbase.query';

/**
 * Returns the authentication method required by the connector to authorize the
 * third-party service.
 *
 * @returns {Object} AuthType
 */
function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.USER_PASS)
    .build();
}

/**
 * Returns the user configurable options for the connector.
 *
 * @param {Object} request Config request parameters.
 * @returns {Object} Connector configuration to be displayed to the user.
 */
function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText(
      'Enter your Couchbase server information and N1QL query. The query should return a consistent schema.'
    );

  config
    .newTextInput()
    .setId('baseUrl')
    .setName('Couchbase Server URL')
    .setHelpText('e.g., https://localhost:8091 or https://cb.<your-endpoint>.cloud.couchbase.com for Capella')
    .setPlaceholder('https://localhost:8091')
    .setAllowOverride(true);

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
    .setHelpText('The name of the scope within the bucket (e.g., inventory)')
    .setPlaceholder('_default')
    .setAllowOverride(true);
    
  config
    .newTextInput()
    .setId('collection')
    .setName('Collection (Optional)')
    .setHelpText('The name of the collection within the scope (e.g., airport)')
    .setPlaceholder('_default')
    .setAllowOverride(true);

  config
    .newTextArea()
    .setId('query')
    .setName('N1QL Query')
    .setHelpText('Enter a valid N1QL query. The query should return a consistent schema.')
    .setPlaceholder('SELECT * FROM `default` LIMIT 100')
    .setAllowOverride(true);
    
  // Advanced options section
  config
    .newInfo()
    .setId('advancedOptions')
    .setText('Advanced Options (Optional)');
    
  config
    .newSelectSingle()
    .setId('scanConsistency')
    .setName('Scan Consistency')
    .setHelpText('Controls the consistency of the query. NotBounded is fastest but may not include recent mutations.')
    .setAllowOverride(true)
    .addOption(
      config.newOptionBuilder()
        .setLabel('Not Bounded (Default)')
        .setValue('not_bounded')
    )
    .addOption(
      config.newOptionBuilder()
        .setLabel('Request Plus')
        .setValue('request_plus')
    );
    
  config
    .newTextInput()
    .setId('timeout')
    .setName('Query Timeout (ms)')
    .setHelpText('Maximum time to wait for the query to complete in milliseconds')
    .setPlaceholder('30000')
    .setAllowOverride(true);
    
  // Vector search options for Couchbase 7.6+
  config
    .newInfo()
    .setId('vectorSearchInfo')
    .setText('Vector Search Options (Couchbase Server 7.6+)');
    
  config
    .newCheckbox()
    .setId('enableVectorSearch')
    .setName('Enable Vector Search')
    .setHelpText('Enable vector search capabilities (requires Couchbase Server 7.6+)')
    .setAllowOverride(true);
    
  config
    .newTextInput()
    .setId('vectorField')
    .setName('Vector Field')
    .setHelpText('The field containing vector embeddings')
    .setPlaceholder('vector_field')
    .setAllowOverride(true);

  return config.build();
}

/**
 * Validates config and throws errors if necessary.
 *
 * @param {Object} configParams Config parameters.
 * @returns {Object} Updated Config parameters.
 */
function validateConfig(configParams) {
  configParams = configParams || {};
  
  if (!configParams.baseUrl) {
    throwUserError('Couchbase Server URL is required.');
  }
  
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
  
  // Check if the URL is for Capella and ensure it's using https
  if (configParams.baseUrl.indexOf('cloud.couchbase.com') > -1) {
    if (!configParams.baseUrl.startsWith('https://')) {
      throwUserError('Couchbase Capella requires a secure connection. URL must start with https://');
    }
  }
  
  // Validate and convert timeout to number if provided
  if (configParams.timeout) {
    var timeout = parseInt(configParams.timeout, 10);
    if (isNaN(timeout) || timeout <= 0) {
      throwUserError('Timeout must be a positive number.');
    }
    configParams.timeout = timeout;
  } else {
    configParams.timeout = 30000; // Default timeout: 30 seconds
  }
  
  // Validate scan consistency
  if (configParams.scanConsistency && 
      configParams.scanConsistency !== 'not_bounded' && 
      configParams.scanConsistency !== 'request_plus') {
    throwUserError('Invalid scan consistency value. Must be "not_bounded" or "request_plus".');
  }
  
  // Handle vector search options
  if (configParams.enableVectorSearch === 'true') {
    if (!configParams.vectorField) {
      throwUserError('Vector field is required when vector search is enabled.');
    }
    
    // Set up vector search parameters
    configParams.vectorSearch = {
      field: configParams.vectorField
    };
  }
  
  return configParams;
}

/**
 * Returns the schema for the given request.
 *
 * @param {Object} request Schema request parameters.
 * @returns {Object} Schema for the given request.
 */
function getSchema(request) {
  request.configParams = validateConfig(request.configParams);
  
  try {
    var result = fetchData(request.configParams);
    var schema = buildSchema(result);
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
  if (!result || !result.results || result.results.length === 0) {
    throwUserError('Query returned no results. Cannot build schema.');
  }
  
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  
  var firstRow = result.results[0];
  
  Object.keys(firstRow).forEach(function(key) {
    var value = firstRow[key];
    var type = typeof value;
    
    if (type === 'number') {
      fields
        .newMetric()
        .setId(key)
        .setName(key)
        .setType(types.NUMBER);
    } else if (type === 'boolean') {
      fields
        .newDimension()
        .setId(key)
        .setName(key)
        .setType(types.BOOLEAN);
    } else if (value instanceof Date || (typeof value === 'string' && !isNaN(Date.parse(value)))) {
      fields
        .newDimension()
        .setId(key)
        .setName(key)
        .setType(types.YEAR_MONTH_DAY_HOUR);
    } else {
      fields
        .newDimension()
        .setId(key)
        .setName(key)
        .setType(types.TEXT);
    }
  });
  
  return fields.build();
}

/**
 * Returns the tabular data for the given request.
 *
 * @param {Object} request Data request parameters.
 * @returns {Object} Contains the schema and data for the given request.
 */
function getData(request) {
  request.configParams = validateConfig(request.configParams);
  
  try {
    var result = fetchData(request.configParams);
    
    if (!result || !result.results || result.results.length === 0) {
      return {
        schema: [],
        rows: []
      };
    }
    
    var schema = buildSchema(result);
    var requestedFieldIds = request.fields.map(function(field) {
      return field.name;
    });
    var requestedFields = schema.filter(function(field) {
      return requestedFieldIds.indexOf(field.name) > -1;
    });
    
    var rows = result.results.map(function(row) {
      var values = requestedFieldIds.map(function(fieldId) {
        return row[fieldId] !== undefined ? row[fieldId] : null;
      });
      
      return { values: values };
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
 * Fetches data from Couchbase using the provided configuration.
 *
 * @param {Object} configParams Config parameters.
 * @returns {Object} Query result.
 */
function fetchData(configParams) {
  var credentials = getCredentials();
  
  if (!credentials.username || !credentials.password) {
    throwUserError('Username and password are required.');
  }
  
  var baseUrl = configParams.baseUrl;
  var bucket = configParams.bucket;
  var scope = configParams.scope || '_default';
  var collection = configParams.collection || '_default';
  var query = configParams.query;
  var timeout = configParams.timeout || 30000; // Default timeout of 30 seconds
  
  // Construct the Couchbase query API URL
  var queryUrl = baseUrl.replace(/\/$/, '') + '/query/service';
  
  // For Couchbase Capella, use secure connection with configProfile
  var isCapella = baseUrl.indexOf('cloud.couchbase.com') > -1;
  
  // Build the query context based on bucket, scope, and collection
  // Format: bucket.scope.collection for proper scoping
  var queryContext = bucket;
  if (scope !== '_default') {
    queryContext += '.' + scope;
  }
  if (collection !== '_default') {
    queryContext += '.' + collection;
  }
  
  // Prepare the query payload
  var queryPayload = {
    statement: query,
    query_context: queryContext,
    timeout: timeout + "ms" // Add timeout in milliseconds
  };
  
  // Add scan consistency options if provided
  if (configParams.scanConsistency) {
    queryPayload.scan_consistency = configParams.scanConsistency;
  }
  
  // Add vector search parameters if provided
  if (configParams.vectorSearch) {
    queryPayload.vector_search = configParams.vectorSearch;
  }
  
  var options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(queryPayload),
    headers: {
      Authorization: 'Basic ' + Utilities.base64Encode(credentials.username + ':' + credentials.password)
    },
    muteHttpExceptions: true,
    timeout: timeout // Set the HTTP request timeout
  };
  
  try {
    var response = UrlFetchApp.fetch(queryUrl, options);
    var responseCode = response.getResponseCode();
    var responseText = response.getContentText();
    
    if (responseCode !== 200) {
      throwUserError('Error querying Couchbase: ' + responseText);
    }
    
    return JSON.parse(responseText);
  } catch (e) {
    throwUserError('Error connecting to Couchbase: ' + e.toString());
  }
}

/**
 * Returns true if the auth service has access.
 *
 * @returns {boolean} True if the auth service has access.
 */
function isAuthValid() {
  var credentials = getCredentials();
  return credentials && credentials.username && credentials.password;
}

/**
 * Returns the user's credentials.
 *
 * @returns {Object} User credentials.
 */
function getCredentials() {
  var userProperties = PropertiesService.getUserProperties();
  var username = userProperties.getProperty(connector.usernameKey);
  var password = userProperties.getProperty(connector.passwordKey);
  
  return {
    username: username,
    password: password
  };
}

/**
 * Sets the credentials.
 *
 * @param {Object} request The set credentials request.
 * @returns {Object} An object with an errorCode.
 */
function setCredentials(request) {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty(connector.usernameKey, request.userPass.username);
  userProperties.setProperty(connector.passwordKey, request.userPass.password);
  return { errorCode: 'NONE' };
}

/**
 * Resets the auth service.
 */
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty(connector.usernameKey);
  userProperties.deleteProperty(connector.passwordKey);
}

/**
 * Throws a user-friendly error message.
 *
 * @param {string} message Error message.
 */
function throwUserError(message) {
  DataStudioApp.createCommunityConnector()
    .newUserError()
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
