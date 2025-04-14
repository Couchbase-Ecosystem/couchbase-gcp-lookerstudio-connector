/**
 * Couchbase Connector for Google Looker Studio
 * This connector allows users to connect to a Couchbase database and run N1QL queries.
 */

/**
 * Returns the authentication method required by the connector to authorize the
 * third-party service.
 *
 * @returns {Object} AuthType
 */
function getAuthType() {
  const cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.PATH_USER_PASS)
    .setHelpUrl('https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html')
    .build();
}

/**
 * Returns true if the auth service has access.
 * 
 * @returns {boolean} True if the auth service has access.
 */
function isAuthValid() {
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  return path && username && password;
}

/**
 * Sets the credentials.
 * 
 * @param {Request} request The set credentials request.
 * @returns {Object} An object with an errorCode.
 */
function setCredentials(request) {
  const creds = request.pathUserPass;
  const path = creds.path;
  const username = creds.username;
  const password = creds.password;
  
  const userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.path', path);
  userProperties.setProperty('dscc.username', username);
  userProperties.setProperty('dscc.password', password);
  
  return {
    errorCode: 'NONE'
  };
}

/**
 * Resets the auth service.
 */
function resetAuth() {
  const userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.path');
  userProperties.deleteProperty('dscc.username');
  userProperties.deleteProperty('dscc.password');
}

/**
 * Returns the user configurable options for the connector.
 *
 * @param {Object} request Config request parameters.
 * @returns {Object} Connector configuration to be displayed to the user.
 */
function getConfig(request) {
  const config = {
    configParams: [
      {
        type: 'INFO',
        name: 'instructions',
        text: 'Enter your Couchbase connection details and N1QL query. Use a publicly accessible URL (e.g., Capella or a public IP/domain), not localhost. The query should return a consistent schema.'
      },
      {
        type: 'TEXTINPUT',
        name: 'bucket',
        displayName: 'Bucket Name',
        helpText: 'The name of the Couchbase bucket to query',
        placeholder: 'default'
      },
      {
        type: 'TEXTINPUT',
        name: 'scope',
        displayName: 'Scope (Optional)',
        helpText: 'The name of the scope within the bucket (e.g., inventory)',
        placeholder: '_default'
      },
      {
        type: 'TEXTINPUT',
        name: 'collection',
        displayName: 'Collection (Optional)',
        helpText: 'The name of the collection within the scope (e.g., airport)',
        placeholder: '_default'
      },
      {
        type: 'TEXTAREA',
        name: 'query',
        displayName: 'N1QL Query',
        helpText: 'Enter a valid N1QL query (e.g., SELECT * FROM `travel-sample`.inventory.airport LIMIT 100). The query should return a consistent schema.',
        placeholder: 'SELECT * FROM `travel-sample`.inventory.airport LIMIT 100'
      }
    ]
  };
  return config;
}

/**
 * Validates config and throws errors if necessary.
 *
 * @param {Object} configParams Config parameters.
 * @returns {Object} Updated Config parameters.
 */
function validateConfig(configParams) {
  configParams = configParams || {};
  
  // Get stored credentials
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  // Validate stored credentials
  if (!path || !username || !password) {
    throwUserError('Authentication credentials are missing. Please reconnect to Couchbase.');
  }
  
  // Add credentials to configParams for use in fetchData
  configParams.baseUrl = path;
  configParams.username = username;
  configParams.password = password;
  
  // Validate required config parameters
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
  
  // Check if the URL is for Capella and ensure it's using a secure connection
  if (path.indexOf('cloud.couchbase.com') > -1) {
    if (!path.startsWith('https://') && !path.startsWith('couchbases://')) {
      throwUserError('Couchbase Capella requires a secure connection. URL must start with https:// or couchbases://');
    }
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
    const result = fetchData(request.configParams);
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
 *
 * @param {Object} request Data request parameters.
 * @returns {Object} Contains the schema and data for the given request.
 */
function getData(request) {
  request.configParams = validateConfig(request.configParams);
  
  try {
    const result = fetchData(request.configParams);
    
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
 * Fetches data from Couchbase using the provided configuration.
 *
 * @param {Object} configParams Config parameters.
 * @returns {Object} Query result.
 */
function fetchData(configParams) {
  const username = configParams.username;
  const password = configParams.password;
  
  if (!username || !password) {
    throwUserError('Username and password are required.');
  }
  
  const baseUrl = configParams.baseUrl;
  const bucket = configParams.bucket;
  const scope = configParams.scope || '_default';
  const collection = configParams.collection || '_default';
  const query = configParams.query;
  const timeout = 30000; // Default timeout of 30 seconds
  
  // Note: Google Apps Script doesn't support importing npm packages like the Couchbase SDK.
  // In a normal Node.js environment, we would use code like:
  //
  // const cluster = await couchbase.connect(clusterConnStr, {
  //   username: username,
  //   password: password,
  //   configProfile: 'wanDevelopment',
  // })
  // const bucket = cluster.bucket(bucketName)
  // const collection = bucket.scope(scope).collection(collection)
  // const result = await cluster.query(query)
  //
  // Instead, we're simulating this connection by making HTTP requests to the Query Service endpoint.
  
  // Convert Couchbase connection string to HTTP endpoint for query service
  let apiBaseUrl = baseUrl;
  
  // Handle different URL formats (similar to how SDK would handle connection strings)
  if (apiBaseUrl.startsWith('couchbases://')) {
    // Convert couchbases:// to https:// for API calls
    apiBaseUrl = 'https://' + apiBaseUrl.substring('couchbases://'.length);
  } else if (apiBaseUrl.startsWith('couchbase://')) {
    // Convert couchbase:// to http:// for API calls (assuming non-secure if couchbase://)
    // Or force https:// if preferred: 'https://' + apiBaseUrl.substring('couchbase://'.length);
    apiBaseUrl = 'http://' + apiBaseUrl.substring('couchbase://'.length);
  }
  
  // Ensure the URL has a scheme (default to http if missing, unless it looks like Capella)
  if (!apiBaseUrl.startsWith('http://') && !apiBaseUrl.startsWith('https://')) {
    if (apiBaseUrl.includes('cloud.couchbase.com')) {
      // Default Capella to https
      apiBaseUrl = 'https://' + apiBaseUrl;
    } else {
       // Default other connections to http (adjust if https is standard for your non-Capella setups)
      apiBaseUrl = 'http://' + apiBaseUrl;
    }
  }
  
  // Construct the Couchbase query API URL (equivalent to cluster.query() in SDK)
  // Default port for Query service is 8093 (HTTP) or 18093 (HTTPS)
  // We need to ensure the correct port is present or added if common ports (80, 443) were omitted.
  // This part requires careful handling based on expected user input.
  // For simplicity here, we assume the user includes the port if it's not standard http/https.
  const queryUrl = apiBaseUrl.replace(/\/$/, '') + '/query/service';
  
  // Build the query context based on bucket, scope, and collection
  // Format: bucket.scope.collection for proper scoping
  let queryContext = bucket;
  if (scope !== '_default') {
    queryContext += '.' + scope;
  }
  if (collection !== '_default') {
    queryContext += '.' + collection;
  }
  
  // Prepare the query payload
  const queryPayload = {
    statement: query,
    query_context: queryContext,
    timeout: timeout + "ms" // Add timeout in milliseconds
  };
  
  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(queryPayload),
    headers: {
      Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
    },
    muteHttpExceptions: true,
    timeout: timeout // Set the HTTP request timeout
  };
  
  try {
    const response = UrlFetchApp.fetch(queryUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();
    
    if (responseCode !== 200) {
      throwUserError('Error querying Couchbase: ' + responseText);
    }
    
    return JSON.parse(responseText);
  } catch (e) {
    throwUserError('Error connecting to Couchbase: ' + e.toString());
  }
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
