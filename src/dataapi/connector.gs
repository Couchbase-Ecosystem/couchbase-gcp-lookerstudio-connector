/**
 * Couchbase Data API Connector for Google Looker Studio.
 * This connector allows users to connect to the Couchbase Data API, enabling them to
 * visualize data from their Couchbase clusters within Looker Studio.
 * It supports authentication, dynamic configuration for selecting data (either by
 * specific collection or a custom N1QL query), schema inference, and data retrieval.
 */

// ==========================================================================
// ===                      CORE UTILITY FUNCTIONS                        ===
// ==========================================================================

/**
 * @private
 * Constructs the full base URL for the Couchbase Data API from a user-provided path.
 * It standardizes the URL to use HTTPS and removes any trailing slashes.
 * The user is expected to include the port in the path if it's not the default HTTPS port (443).
 *
 * @param {string} path The user-provided path, e.g., "my.server.com", "my.server.com:18095", "http://my.server.com", "couchbases://my.server.com".
 * @return {string} The standardized HTTPS base URL for the Couchbase Data API, e.g., "https://my.server.com", "https://my.server.com:18095".
 */
function _constructApiUrl(path) {
  let hostAndPort = path;

  // Standardize scheme by removing common prefixes to isolate host and port.
  if (hostAndPort.startsWith('couchbases://')) {
    hostAndPort = hostAndPort.substring('couchbases://'.length); // Remove "couchbases://" prefix
  } else if (hostAndPort.startsWith('couchbase://')) {
    hostAndPort = hostAndPort.substring('couchbase://'.length); // Remove "couchbase://" prefix
  } else if (hostAndPort.startsWith('https://')) {
    hostAndPort = hostAndPort.substring('https://'.length); // Remove "https://" prefix
  } else if (hostAndPort.startsWith('http://')) {
    hostAndPort = hostAndPort.substring('http://'.length); // Remove "http://" prefix
  }

  // Remove trailing slash if present to ensure URL consistency.
  hostAndPort = hostAndPort.replace(/\/$/, '');
  // The path provided by the user should now contain the host and optionally the port.
  // We no longer append a default port. If the service is not on 443,
  // the user must specify it in the path, e.g., "mycouchbase.local:18095".
  // For Capella/sandbox URLs, they operate on 443 by default.
  Logger.log('_constructApiUrl: Using host and port as provided (or default 443 if no port specified): %s', hostAndPort);

  // Prepend "https://" to ensure the URL uses HTTPS.
  return 'https://' + hostAndPort;
}

/**
 * @private
 * Throws a user-friendly error in Looker Studio.
 * This function should be used to communicate errors originating from the connector
 * back to the Looker Studio user interface.
 *
 * @param {string} message The error message to display to the user.
 */
function _throwUserError(message) {
  DataStudioApp.createCommunityConnector()
    .newUserError()
    .setText(message)
    .throwException(); // Actually throws the error to Looker Studio
}

/**
 * @private
 * Executes a given N1QL query against the Couchbase Query Service.
 * It handles the HTTP POST request, authorization, and basic error checking.
 *
 * @param {string} apiUrl The base API URL for the Couchbase cluster, constructed by `_constructApiUrl`.
 * @param {string} authHeader The Basic authentication header string (e.g., "Basic dXNlcjpwYXNz").
 * @param {string} statement The N1QL query statement to execute.
 * @return {Array|null} An array of result objects if the query is successful and returns results,
 * an empty array if the query is successful but returns no results,
 * or null if an error occurs or the response format is unexpected.
 */
function _executeN1qlQuery(apiUrl, authHeader, statement) {
  const queryServiceUrl = apiUrl + '/_p/query/query/service'; // Endpoint for N1QL queries
  Logger.log('_executeN1qlQuery: URL: %s, Statement: %s', queryServiceUrl, statement);

  const options = {
    method: 'post', // N1QL queries are typically sent via POST
    contentType: 'application/json',
    headers: { Authorization: authHeader },
    payload: JSON.stringify({ statement: statement }), // The N1QL statement is sent in the payload
    muteHttpExceptions: true, // Allows handling HTTP errors manually
    validateHttpsCertificates: false // Consistent with other fetch calls in this connector
  };

  try {
    const response = UrlFetchApp.fetch(queryServiceUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    if (responseCode === 200) { // HTTP 200 OK indicates success
      const queryResult = JSON.parse(responseText);
      if (queryResult.results) { // Standard case: query returns a list of results
        Logger.log('_executeN1qlQuery: Success, %s results.', queryResult.results.length);
        return queryResult.results; // This is an array of result objects
      } else if (queryResult.status === 'success' && queryResult.results === undefined) {
        // Some queries (e.g., DDL or successful queries with no matching documents)
        // might return success status without a "results" field.
        Logger.log('_executeN1qlQuery: Success but no "results" field, assuming empty. Response: %s', responseText);
        return []; // Treat as an empty set of results
      } else {
        // Handle cases where the query was "successful" (200 OK) but the response format is not as expected.
        Logger.log('_executeN1qlQuery: Query successful but response format unexpected. Code: %s, Response: %s', responseCode, responseText);
        return null; // Indicate an issue or unexpected format
      }
    } else {
      // Handle non-200 HTTP responses (e.g., 400, 401, 500).
      Logger.log('_executeN1qlQuery: Error. Code: %s, Response: %s', responseCode, responseText);
      return null; // Indicate error
    }
  } catch (e) {
    // Handle exceptions during the fetch operation (e.g., network issues).
    Logger.log('_executeN1qlQuery: Exception during fetch: %s. Statement: %s', e.toString(), statement);
    return null; // Indicate error
  }
}

// ==========================================================================
// ===                       AUTHENTICATION FLOW                          ===
// ==========================================================================

/**
 * Returns the authentication method required by the connector.
 * This connector uses username and password authentication, including a path for the Couchbase host.
 *
 * @return {Object} The AuthType response object for Looker Studio.
 */
function getAuthType() {
  const cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.PATH_USER_PASS) // Specifies PATH_USER_PASS authentication
    .setHelpUrl('https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html') // Provides a help URL for users
    .build();
}

/**
 * @private
 * Attempts to validate user credentials by making a minimal request to the Couchbase Data API.
 * This function is called internally by `isAuthValid` to perform a live check against the
 * `/v1/callerIdentity` endpoint, which requires valid credentials.
 *
 * @param {string} path The base path (host and optional port) for the Couchbase Data API.
 * @param {string} username The username for authentication.
 * @param {string} password The password for authentication.
 * @return {boolean} True if credentials are valid and the API responds with 200 OK, false otherwise.
 */
function _validateCredentials(path, username, password) {
  Logger.log('_validateCredentials received path: %s', path);
  Logger.log('Attempting to validate credentials against Data API for path: %s, username: %s', path, username); // Username logged for debugging, password is not.

  // Ensure all necessary credential components are present.
  if (!path || !username || !password) {
    Logger.log('Validation failed: Missing path, username, or password.');
    return false;
  }

  // Construct API URL for Data API.
  const apiUrl = _constructApiUrl(path);
  // Test endpoint - /v1/callerIdentity requires valid credentials and is a lightweight check.
  const validationUrl = apiUrl + '/v1/callerIdentity';
  Logger.log('_validateCredentials constructed Data API URL for validation: %s', validationUrl);

  const options = {
    method: 'get', // GET request for this validation endpoint.
    contentType: 'application/json',
    headers: {
      // Encode username and password for Basic Authentication.
      Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
    },
    muteHttpExceptions: true, // Allows us to handle HTTP errors (like 401 Unauthorized) gracefully.
    validateHttpsCertificates: false // As per original script, HTTPS certificates are not validated.
  };

  try {
    Logger.log('Sending validation request...');
    const response = UrlFetchApp.fetch(validationUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();
    Logger.log('Validation response code: %s', responseCode);

    if (responseCode === 200) { // HTTP 200 OK indicates successful authentication.
      Logger.log('Credential validation successful.');
      return true;
    } else {
      // Log failure details for debugging.
      Logger.log('Credential validation failed. Code: %s, Response: %s', responseCode, responseText);
      return false;
    }
  } catch (e) {
    // Log exceptions that occur during the fetch operation (e.g., network error).
    Logger.log('Credential validation failed with exception: %s', e.toString());
    Logger.log('Exception details: %s', e.stack);
    return false;
  }
}

/**
 * Checks if the currently stored authentication credentials are valid.
 * It retrieves credentials from `PropertiesService` and uses `_validateCredentials`
 * to perform a live check against the Couchbase Data API.
 *
 * @return {boolean} True if credentials are stored and valid, false otherwise.
 */
function isAuthValid() {
  Logger.log('isAuthValid called.');
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password'); // Retrieve stored password

  // Log retrieved credentials (masking password for security).
  Logger.log('isAuthValid: Path: %s, Username: %s, Password: %s', path, username, '********'); // Password masked in log

  // If any part of the credentials is not found in storage, authentication is invalid.
  if (!path || !username || !password) {
    Logger.log('isAuthValid: Credentials not found in storage.');
    return false;
  }

  Logger.log('isAuthValid: Found credentials. Performing live validation test.');
  const isValid = _validateCredentials(path, username, password); // Perform live validation
  Logger.log('isAuthValid: Validation result: %s', isValid);
  return isValid;
}

/**
 * Sets (stores) the user-provided credentials (path, username, password)
 * in `PropertiesService` for the current user.
 *
 * @param {Object} request The request object from Looker Studio containing credentials.
 * `request.pathUserPass` holds the path, username, and password.
 * @return {Object} An object indicating the success or failure of storing credentials.
 * Returns `{errorCode: 'NONE'}` on success.
 */
function setCredentials(request) {
  Logger.log('setCredentials called.');
  const creds = request.pathUserPass;
  const path = creds.path;
  const username = creds.username;
  const password = creds.password;

  // Log received credentials, masking the password.
  Logger.log('Received path: %s, username: %s, password: %s', path, username, '*'.repeat(password.length)); // Password length logged, not the password itself

  try {
    const userProperties = PropertiesService.getUserProperties();
    userProperties.setProperty('dscc.path', path);
    userProperties.setProperty('dscc.username', username);
    userProperties.setProperty('dscc.password', password);
    Logger.log('Credentials stored successfully.');
  } catch (e) {
    Logger.log('Error storing credentials: %s', e.toString());
    // Return a system error if storing credentials fails.
    return {
      errorCode: 'SystemError',
      errorText: 'Failed to store credentials: ' + e.toString() // Provide error details
    };
  }

  Logger.log('setCredentials finished successfully.');
  // Return 'NONE' error code to indicate success.
  return {
    errorCode: 'NONE'
  };
}

/**
 * Resets (clears) the stored authentication credentials for the current user
 * from `PropertiesService`.
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
    // Log any errors during the deletion process.
    Logger.log('Error during resetAuth: %s', e.toString());
  }
}

// ==========================================================================
// ===                      CONFIGURATION FLOW                           ===
// ==========================================================================

/**
 * @private
 * Fetches Couchbase metadata (buckets, scopes, and collections) using N1QL queries
 * against system catalogs. This metadata is used to populate dropdowns in the
 * connector's configuration UI, allowing users to select a specific collection.
 *
 * @return {Object} An object containing `buckets` (an array of bucket names) and
 * `scopesCollections` (an object mapping bucket -> scope -> collections).
 * Returns empty structures if credentials are missing or an error occurs.
 */
function _fetchCouchbaseMetadata() {
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path'); // Get stored Couchbase path
  const username = userProperties.getProperty('dscc.username'); // Get stored username
  const password = userProperties.getProperty('dscc.password'); // Get stored password
  Logger.log('_fetchCouchbaseMetadata (N1QL): Starting fetch with path: %s, username: %s', path, username);

  // If authentication credentials are not available, metadata cannot be fetched.
  if (!path || !username || !password) {
    Logger.log('_fetchCouchbaseMetadata (N1QL): Auth credentials missing.');
    return { buckets: [], scopesCollections: {} }; // Return empty metadata
  }

  const apiUrl = _constructApiUrl(path); // Construct the API URL
  const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password); // Prepare auth header

  const scopesCollections = {}; // Structure: { bucketName: { scopeName: [collectionName1, ...] } }
  let bucketNames = []; // To keep track of unique bucket names

  try {
    // N1QL query to select bucket, scope, and collection names from system catalogs.
    // This provides a comprehensive list of all available keyspaces.
    const n1qlQuery = 'SELECT b.name AS `bucket`, s.name AS `scope`, k.name AS `collection` ' +
                      'FROM system:buckets AS b ' +
                      'JOIN system:all_scopes AS s ON s.`bucket` = b.name ' + // Join buckets with scopes
                      'JOIN system:keyspaces AS k ON k.`bucket` = b.name AND k.`scope` = s.name ' + // Join with keyspaces (collections)
                      'ORDER BY b.name, s.name, k.name;'; // Order for consistent presentation
    const results = _executeN1qlQuery(apiUrl, authHeader, n1qlQuery);

    // If the query fails or system catalogs are not accessible.
    if (results === null) {
      Logger.log('_fetchCouchbaseMetadata (N1QL): Failed to fetch keyspace information or system catalogs not accessible.');
      return { buckets: [], scopesCollections: {} }; // Return empty metadata
    }

    // If the query returns no results (e.g., an empty Couchbase cluster).
    if (results.length === 0) {
      Logger.log('_fetchCouchbaseMetadata (N1QL): No keyspaces (buckets/scopes/collections) found.');
      return { buckets: [], scopesCollections: {} }; // Return empty metadata
    }

    Logger.log('_fetchCouchbaseMetadata (N1QL): Processing %s items from query.', results.length);
    // Iterate over query results to populate the scopesCollections structure.
    results.forEach(item => {
      const bucket = item.bucket;
      const scope = item.scope;
      const collection = item.collection;

      // Skip any item that might be missing essential fields (robustness).
      if (!bucket || !scope || !collection) {
        Logger.log('_fetchCouchbaseMetadata (N1QL): Skipping item with missing bucket, scope, or collection: %s', JSON.stringify(item));
        return; // Continue to the next item in forEach.
      }

      // Initialize bucket entry if it doesn't exist.
      if (!scopesCollections[bucket]) {
        scopesCollections[bucket] = {};
        bucketNames.push(bucket); // Add to unique bucket names list
      }
      // Initialize scope entry within the bucket if it doesn't exist.
      if (!scopesCollections[bucket][scope]) {
        scopesCollections[bucket][scope] = [];
      }
      // Add collection to the scope.
      scopesCollections[bucket][scope].push(collection);
    });
    Logger.log('_fetchCouchbaseMetadata (N1QL): Final structure: %s', JSON.stringify(scopesCollections));

    // Return the fetched metadata.
    return {
      buckets: bucketNames, // List of unique bucket names
      scopesCollections: scopesCollections // Nested structure of buckets, scopes, and collections
    };
  } catch (e) {
    // Log any unexpected errors during metadata fetching.
    Logger.log('Error in _fetchCouchbaseMetadata (N1QL): %s. Stack: %s', e.toString(), e.stack);
    return { buckets: [], scopesCollections: {} }; // Fallback to empty metadata on any exception
  }
}

/**
 * Returns the user-configurable options for the connector.
 * This function defines the configuration UI that users see when setting up
 * the connector in Looker Studio. It allows users to choose between querying
 * a specific collection or using a custom N1QL query.
 *
 * @param {Object} request The request object from Looker Studio, which may contain
 * existing configuration parameters (`request.configParams`).
 * @return {Object} The configuration object built by `CommunityConnector.getConfig()`.
 */
function getConfig(request) {
  const cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig(); // Start building a new configuration object.

  try {
    // Determine if this is the first request (no params yet) or a subsequent one.
    const isFirstRequest = (request.configParams === undefined);
    const configParams = request.configParams || {}; // Use existing params or an empty object.

    // The configuration is dynamic and stepped, guiding the user through choices.
    let isStepped = true; // Assume config is ongoing unless all required inputs for a mode are provided.

    // Informational text for the user.
    config
      .newInfo()
      .setId('instructions')
      .setText('Choose a configuration mode: query by selecting a collection, or enter a custom N1QL query.');

    // Mode selector: 'Query by Collection' or 'Use Custom Query'.
    const modeSelector = config.newSelectSingle()
      .setId('configMode')
      .setName('Configuration Mode')
      .setHelpText('Select how you want to define the data source.')
      .setAllowOverride(true) // Allow users to change this later.
      .setIsDynamic(true); // Changing this selector will refresh the config.

    modeSelector.addOption(config.newOptionBuilder().setLabel('Query by Collection').setValue('collection'));
    modeSelector.addOption(config.newOptionBuilder().setLabel('Use Custom Query').setValue('customQuery'));

    // Determine the current mode, defaulting to 'collection'.
    const currentMode = configParams.configMode ? configParams.configMode : 'collection';
    Logger.log('getConfig: Current mode: %s', currentMode);

    if (currentMode === 'collection') {
      config.newInfo()
        .setId('collection_info')
        .setText('Select a collection to query data from.');

      // Fetch buckets, scopes, and collections to populate the dropdown.
      const metadata = _fetchCouchbaseMetadata();
      Logger.log('getConfig: Metadata fetch returned buckets: %s', JSON.stringify(metadata.buckets));

      // Dropdown for selecting a collection.
      const collectionSelect = config
        .newSelectSingle()
        .setId('collection')
        .setName('Couchbase Collection')
        .setHelpText('Select the collection to query data from.')
        .setAllowOverride(true);

      // Build a list of fully qualified collection paths (bucket.scope.collection).
      const collectionPaths = [];
      Object.keys(metadata.scopesCollections).forEach(bucket => { // Iterate through buckets
        Object.keys(metadata.scopesCollections[bucket]).forEach(scope => { // Iterate through scopes
          metadata.scopesCollections[bucket][scope].forEach(collection => { // Iterate through collections
            const path = `${bucket}.${scope}.${collection}`; // e.g., travel-sample.inventory.airline
            const label = `${bucket} > ${scope} > ${collection}`; // User-friendly label
            collectionPaths.push({ path: path, label: label });
            Logger.log('getConfig: Added collection path: %s', path);
          });
        });
      });

      // Sort collection paths alphabetically for better UX.
      collectionPaths.sort((a, b) => a.label.localeCompare(b.label));

      // Add each collection path as an option to the dropdown.
      collectionPaths.forEach(item => {
        collectionSelect.addOption(
          config.newOptionBuilder().setLabel(item.label).setValue(item.path)
        );
      });

      // Check if a collection has been selected.
      const selectedCollection = configParams.collection ? configParams.collection : null;
      if (selectedCollection) {
        isStepped = false; // Configuration is complete for 'collection' mode if a collection is selected.
        Logger.log('getConfig (collection mode): Collection is selected (%s), setting isStepped = false.', selectedCollection);
      } else {
        Logger.log('getConfig (collection mode): Collection NOT selected, isStepped = true.');
      }

      // Only add 'maxRows' input if the collection has been selected (config is complete for this step).
      if (!isStepped) {
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

      // Text area for users to input their custom N1QL query.
      config
        .newTextArea()
        .setId('query')
        .setName('Custom N1QL Query')
        .setHelpText('Enter a valid N1QL query. Ensure you include a LIMIT clause if needed for performance or sampling (e.g., for schema inference).')
        .setPlaceholder('SELECT * FROM `travel-sample`.`inventory`.`airline` WHERE country = "France" LIMIT 100')
        .setAllowOverride(true);

      isStepped = false; // Configuration is complete once the custom query text area is shown.
      Logger.log('getConfig (customQuery mode): Setting isStepped = false.');
    }

    // Set whether the configuration process is stepped (requires more input) or complete.
    config.setIsSteppedConfig(isStepped);
    Logger.log('getConfig: Final setIsSteppedConfig to: %s', isStepped);

    return config.build(); // Return the built configuration object.

  } catch (e) {
    // Handle any unexpected errors during configuration building.
    Logger.log('ERROR in getConfig: %s. Stack: %s', e.message, e.stack);
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText('An unexpected error occurred while building the configuration. Please check the Apps Script logs for details. Error: ' + e.message)
      .setDebugText('getConfig failed: ' + e.stack)
      .throwException(); // Throw a user-facing error.
  }
}

/**
 * Validates the user-provided configuration parameters.
 * This function is called by Looker Studio to ensure the configuration is complete
 * and valid before proceeding to `getSchema` or `getData`.
 *
 * @param {Object} configParams The user configuration parameters from `request.configParams`.
 * @return {Object} A validated configuration object containing all necessary parameters
 * and their default values if applicable.
 * @throws {UserError} If the configuration is invalid or incomplete.
 */
function validateConfig(configParams) {
  Logger.log('Validating config parameters: %s', JSON.stringify(configParams));

  if (!configParams) {
    _throwUserError('No configuration provided');
  }

  // Retrieve stored authentication credentials.
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password'); // implies password is also retrieved

  // Ensure authentication credentials are present.
  if (!path || !username || !password) {
    _throwUserError('Authentication credentials missing. Please reauthenticate.');
  }

  // Ensure configuration mode is specified.
  if (!configParams.configMode) {
    _throwUserError('Configuration mode not specified. Please select a mode.');
  }

  // Create a base validated config object with credentials and mode.
  const validatedConfig = {
    path: path,
    username: username,
    password: password, // Password should be part of the validated config for internal use
    configMode: configParams.configMode
  };

  // Validate based on the selected configuration mode. (Order switched: collection first)
  if (configParams.configMode === 'collection') {
    if (!configParams.collection || configParams.collection.trim() === '') {
      _throwUserError('Collection must be specified in "Query by Collection" mode.');
    }
    validatedConfig.collection = configParams.collection.trim(); // Store trimmed collection path
    // Set maxRows, defaulting to 100 if not specified or invalid.
    validatedConfig.maxRows = configParams.maxRows && parseInt(configParams.maxRows) > 0 ?
             parseInt(configParams.maxRows) : 100;
  } else if (configParams.configMode === 'customQuery') {
    if (!configParams.query || configParams.query.trim() === '') {
      _throwUserError('Custom query must be specified in "Use Custom Query" mode.');
    }
    validatedConfig.query = configParams.query.trim(); // Store trimmed custom query
  } else {
    // Handle unknown configuration mode.
    _throwUserError('Invalid configuration mode selected.');
  }

  Logger.log('Config validation successful');
  return validatedConfig; // Return the fully validated configuration.
}


// ==========================================================================
// ===                        SCHEMA & DATA FLOW                          ===
// ==========================================================================

/**
 * @private
 * Processes the output of a Couchbase `INFER` N1QL query to generate a schema
 * definition suitable for Looker Studio. It extracts field names, infers data types
 * (NUMBER, BOOLEAN, STRING, URL), and semantic types (METRIC, DIMENSION).
 *
 * @param {Array} inferQueryResult The 'results' array from the `INFER` N1QL query response.
 * This is typically an array containing one or more "flavors"
 * of schema, where each flavor describes the properties of documents.
 * @return {Array<Object>} An array of Looker Studio field definitions. Each object includes
 * `name`, `label`, `dataType`, and `semantics` (with `conceptType`).
 * Returns a default placeholder field if inference fails or yields no fields.
 */
function _processInferSchemaOutput(inferQueryResult) {
  Logger.log('_processInferSchemaOutput: Received INFER results: %s', JSON.stringify(inferQueryResult));

  // Check if INFER query returned any results or if the result structure is empty.
  if (!inferQueryResult || inferQueryResult.length === 0 || !inferQueryResult[0] || inferQueryResult[0].length === 0) {
    Logger.log('_processInferSchemaOutput: INFER query returned no flavors or empty result.');
    // Return a placeholder field indicating an empty inference result.
    return [{ name: 'empty_infer_result', label: 'INFER result is empty', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}];
  }

  // Use the first "flavor" from the INFER results for schema generation.
  const firstFlavor = inferQueryResult[0][0];

  // Check if the first flavor contains properties.
  if (!firstFlavor || !firstFlavor.properties) {
    Logger.log('_processInferSchemaOutput: First flavor has no properties.');
    // Return a placeholder field indicating no properties found.
    return [{ name: 'no_properties_in_flavor', label: 'No properties in INFER result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}];
  }

  const schemaFields = []; // Array to hold the generated field definitions.

  /**
   * Recursively extracts fields from nested properties object.
   * @param {Object} properties The properties object from the INFER result.
   * @param {string} [prefix=''] A prefix for nested field names (e.g., "address.").
   */
  function extractFieldsFromProperties(properties, prefix = '') {
    Object.keys(properties).forEach(key => {
      const fieldDef = properties[key]; // Definition of the current field from INFER output.
      const fieldName = prefix ? `${prefix}.${key}` : key; // Construct full field name (e.g., "user.name").
      let dataType = 'STRING'; // Default Looker Studio data type.
      let conceptType = 'DIMENSION'; // Default Looker Studio semantic type.

      // INFER can return a single type or an array of possible types. Standardize to an array.
      const inferTypes = Array.isArray(fieldDef.type) ? fieldDef.type : [fieldDef.type];

      // Determine Looker Studio dataType and conceptType based on inferred Couchbase types.
      if (inferTypes.includes('number') || inferTypes.includes('integer')) {
        dataType = 'NUMBER';
        conceptType = 'METRIC'; // Numbers are often treated as metrics.
      } else if (inferTypes.includes('boolean')) {
        dataType = 'BOOLEAN'; // Booleans are dimensions.
      } else if (inferTypes.includes('string')) {
        // Special handling for strings to detect URLs.
        let isPotentiallyUrl = false;
        let hasNonEmptyUrlSample = false;
        let hasEmptyStringSample = false;

        if (fieldDef.samples && fieldDef.samples.length > 0) {
          fieldDef.samples.forEach(sample => { // Iterate through samples to check for URL patterns.
            if (typeof sample === 'string') {
              if (sample.startsWith('http://') || sample.startsWith('https://')) {
                isPotentiallyUrl = true; // Found a sample that looks like a URL.
                hasNonEmptyUrlSample = true; // Confirmed at least one non-empty URL-like sample.
              } else if (sample === '') {
                hasEmptyStringSample = true; // Found an empty string sample.
              }
            }
          });
        }
        
        // Logic to decide if a string field should be typed as URL:
        // If it has URL-like samples AND empty strings, default to STRING for safety.
        // If it has URL-like samples AND NO empty strings, it can be typed as URL.
        if (isPotentiallyUrl && hasNonEmptyUrlSample) {
            if (hasEmptyStringSample) {
                Logger.log('_processInferSchemaOutput: Field [%s] has URL-like samples and empty strings. Defaulting to STRING.', fieldName);
                dataType = 'STRING';
            } else {
                dataType = 'URL';
            }
        } else {
            dataType = 'STRING'; // Default to STRING if not clearly a URL.
        }

      } else if (inferTypes.includes('object') && fieldDef.properties) {
        // Recursively process nested objects.
        extractFieldsFromProperties(fieldDef.properties, fieldName);
        return; // Return early as fields are added in the recursive call.
      } else if (inferTypes.includes('array')) {
        // Arrays are typically represented as STRING (e.g., JSON stringified).
        dataType = 'STRING';
      }
      // Else, it remains STRING/DIMENSION by default.

      // Add the processed field to the schema.
      schemaFields.push({
        name: fieldName,
        label: fieldName, // Use field name as label by default.
        dataType: dataType,
        semantics: { conceptType: conceptType }
      });
    });
  }

  extractFieldsFromProperties(firstFlavor.properties); // Start extraction from top-level properties.

  // If, after processing, no fields were added, return a placeholder.
  // Also check if the only field is the placeholder 'empty_infer_result' etc.
  if (schemaFields.length === 0 || 
      (schemaFields.length === 1 && 
       (schemaFields[0].name === 'empty_infer_result' || 
        schemaFields[0].name === 'no_properties_in_flavor'))) {
      Logger.log('_processInferSchemaOutput: Warning: Schema inference from INFER resulted in zero or placeholder fields.');
      // Return empty array to signify to caller that INFER didn't yield a good schema.
      return []; 
  }

  Logger.log('_processInferSchemaOutput: Final schema fields from INFER: %s', JSON.stringify(schemaFields));
  return schemaFields; // Return the array of generated field definitions.
}


/**
 * @private
 * Constructs a Looker Studio `Fields` object based on the fields requested by Looker Studio
 * and a master schema definition. This ensures that `getData` responses only include
 * the fields, types, and semantic types that Looker Studio expects for a given query.
 *
 * @param {Array<Object>} requestFields The `request.fields` array from Looker Studio's `getData` request.
 * Each object in this array represents a field Looker Studio expects.
 * @param {Array<Object>} masterSchema The complete schema definition for the data source,
 * typically generated by `getSchema()`. Each object
 * should define `name`, `dataType`, and `semantics`.
 * @return {Fields} A Looker Studio `Fields` object populated with the requested fields,
 * typed according to the `masterSchema`.
 */
function _getRequestedFields(requestFields, masterSchema) {
  const cc = DataStudioApp.createCommunityConnector();
  const requestedFieldsObject = cc.getFields(); // Initialize a new Fields object.
  Logger.log('_getRequestedFields: Called with masterSchema. Processing request.fields: %s', JSON.stringify(requestFields));

  // If Looker Studio doesn't request specific fields, fallback to using the entire masterSchema.
  // This can happen in some contexts or if `request.fields` is empty.
  if (!requestFields || requestFields.length === 0) {
    Logger.log('_getRequestedFields: No specific fields in requestFields. Building response fields from masterSchema as fallback.');
    if (masterSchema && masterSchema.length > 0) {
        masterSchema.forEach(fieldDef => { // Iterate over each field in the master schema.
            let fieldTypeEnum = cc.FieldType.TEXT; // Default to TEXT type.
            // Map masterSchema dataType to Looker Studio FieldType.
            if (fieldDef.dataType === 'NUMBER') fieldTypeEnum = cc.FieldType.NUMBER;
            else if (fieldDef.dataType === 'BOOLEAN') fieldTypeEnum = cc.FieldType.BOOLEAN;
            else if (fieldDef.dataType === 'URL') fieldTypeEnum = cc.FieldType.URL;

            // Determine if the field is a METRIC or DIMENSION.
            if (fieldDef.semantics.conceptType === 'METRIC') {
                requestedFieldsObject.newMetric().setId(fieldDef.name).setName(fieldDef.name).setType(fieldTypeEnum);
            } else {
                requestedFieldsObject.newDimension().setId(fieldDef.name).setName(fieldDef.name).setType(fieldTypeEnum);
            }
        });
    }
    return requestedFieldsObject; // Return the Fields object, possibly populated from masterSchema.
  }

  // Process each field explicitly requested by Looker Studio.
  requestFields.forEach(requestedFieldInfo => { // `requestedFieldInfo` is an item from Looker Studio's `request.fields` array.
    const fieldName = requestedFieldInfo.name; // Name of the field requested by Looker Studio.
    // Find this field in our masterSchema to get its correct dataType and semantics.
    const fieldDefinition = masterSchema.find(f => f.name === fieldName);

    let fieldTypeEnum = cc.FieldType.TEXT; // Default Looker Studio type.
    let conceptType = 'DIMENSION'; // Default Looker Studio semantic concept.

    if (fieldDefinition) {
      // If found in masterSchema, use its defined type and semantics.
      conceptType = fieldDefinition.semantics.conceptType;
      switch (fieldDefinition.dataType) {
        case 'NUMBER': fieldTypeEnum = cc.FieldType.NUMBER; break;
        case 'BOOLEAN': fieldTypeEnum = cc.FieldType.BOOLEAN; break;
        case 'URL': fieldTypeEnum = cc.FieldType.URL; break;
        case 'STRING': // Falls through to default
        default: fieldTypeEnum = cc.FieldType.TEXT; break; // Handles STRING and any other unmapped types.
      }
      Logger.log('_getRequestedFields: Mapped %s to LookerType: %s (from %s), Concept: %s', fieldName, fieldTypeEnum, fieldDefinition.dataType, conceptType);
    } else {
      // If a requested field is NOT in masterSchema, it's an anomaly.
      // Default to TEXT/DIMENSION to avoid errors, but log a warning.
      Logger.log('_getRequestedFields: WARNING - Requested field %s not found in masterSchema. Defaulting to TEXT/DIMENSION.', fieldName);
    }

    // Add the field to the Fields object as either a Metric or Dimension.
    if (conceptType === 'METRIC') {
      requestedFieldsObject.newMetric().setId(fieldName).setName(fieldName).setType(fieldTypeEnum);
    } else { // Default to Dimension if not Metric.
      requestedFieldsObject.newDimension().setId(fieldName).setName(fieldName).setType(fieldTypeEnum);
    }
  });

  Logger.log('_getRequestedFields: Constructed Fields object for getData response: %s', JSON.stringify(requestedFieldsObject.asArray())); // Log the structure of fields being sent back.
  return requestedFieldsObject; // Return the fully populated Fields object.
}

/**
 * Returns the schema for the given request. The schema defines the fields (columns)
 * that will be available from this data source, including their names, data types,
 * and semantic types (Dimension or Metric).
 *
 * If `configMode` is 'collection', it uses an `INFER` N1QL query on the specified collection
 * to dynamically determine the schema.
 * If `configMode` is 'customQuery', it first attempts to use `INFER (subquery) WITH ...`.
 * If that fails, it falls back to executing the user's query with `LIMIT 1` and
 * infers the schema from the single result document.
 *
 * @param {Object} request The request object from Looker Studio, containing `configParams`.
 * @return {Object} A schema response object `{ schema: [...] }` where `[...]` is an
 * array of field definitions.
 * @throws {UserError} If credentials are missing, configuration is invalid, or schema
 * inference fails.
 */
function getSchema(request) {
  Logger.log('getSchema request: %s', JSON.stringify(request));
  try {
    // Retrieve stored authentication credentials.
    const userProperties = PropertiesService.getUserProperties();
    const path = userProperties.getProperty('dscc.path');
    const username = userProperties.getProperty('dscc.username');
    const password = userProperties.getProperty('dscc.password');

    // Ensure authentication credentials are present.
    if (!path || !username || !password) {
      Logger.log('getSchema: Missing credentials');
      _throwUserError('Authentication credentials missing. Please reauthenticate.');
    }

    const configParams = request.configParams || {}; // Use config from request or default to empty.
    const apiUrl = _constructApiUrl(path); // Construct base API URL.
    const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password); // Prepare auth header.
    let schemaFields; // To store the array of field definitions.

    // Schema inference logic depends on the configuration mode. (Order switched: collection first)
    if (configParams.configMode === 'collection') {
      // Validate that a collection is specified in collection mode.
      if (!configParams.collection || configParams.collection.trim() === '') {
        _throwUserError('Collection must be specified in "Query by Collection" mode.');
      }

      const collectionParts = configParams.collection.split('.'); // e.g., "bucket.scope.collection"
      if (collectionParts.length !== 3) {
        _throwUserError('Invalid collection path. Format: bucket.scope.collection');
      }
      // Extract raw bucket, scope, and collection names.
      const rawBucket = collectionParts[0];
      const rawScope = collectionParts[1];
      const rawCollection = collectionParts[2];

      // Construct the keyspace path for the INFER statement, ensuring names are backticked.
      const keyspacePathForInfer = `\`${rawBucket}\`.\`${rawScope}\`.\`${rawCollection}\``;
      // Options for the INFER statement to control sampling.
      const inferWithOptions = `WITH {"sample_size": 100, "num_sample_values": 3, "similarity_metric": 0.6}`;
      const actualInferStatement = `INFER ${keyspacePathForInfer} ${inferWithOptions}`;

      Logger.log('getSchema (collectionMode): Retrieving schema via INFER statement.');
      Logger.log('getSchema (collectionMode): Statement (intended): %s', actualInferStatement);

      // Execute the INFER N1QL query.
      const inferResults = _executeN1qlQuery(apiUrl, authHeader, actualInferStatement);
      if (inferResults === null) {
        // `_executeN1qlQuery` returns null on error.
        _throwUserError('Failed to execute INFER query for collection. Check logs for N1QL error details.');
      }
      
      schemaFields = _processInferSchemaOutput(inferResults);
      if (!schemaFields || schemaFields.length === 0 || (schemaFields.length === 1 && schemaFields[0].name.startsWith('empty_'))) {
         Logger.log('getSchema (collectionMode): INFER results processed but yielded no valid fields. Returning placeholder.');
         return { schema: [{ name: 'empty_collection_infer_schema', label: 'INFER on collection failed or yielded empty schema', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
      }

      Logger.log('getSchema (collectionMode): Final schema from INFER: %s', JSON.stringify(schemaFields));
      return { schema: schemaFields }; // Return the schema derived from INFER.

    } else if (configParams.configMode === 'customQuery') {
      // Validate that a query is provided in custom query mode.
      if (!configParams.query || configParams.query.trim() === '') {
        _throwUserError('Custom query must be specified in "Use Custom Query" mode.');
      }
      
      const originalUserQuery = configParams.query.trim();
      let queryForInfer = originalUserQuery;
      // Append `LIMIT 100` for INFER subquery if no LIMIT clause exists, for schema diversity.
      if (!queryForInfer.toLowerCase().includes('limit')) {
        queryForInfer += ' LIMIT 100';
      }
      
      // Options for INFER on subquery (custom query).
      const inferCustomQueryWithOptions = `WITH {"sample_size": 10000, "num_sample_values": 2, "similarity_metric": 0.1}`;
      const inferSubQueryStatement = `INFER (${queryForInfer}) ${inferCustomQueryWithOptions}`;
      
      Logger.log('getSchema (customQuery): Attempting schema inference via INFER (subquery): %s', inferSubQueryStatement);
      const inferResults = _executeN1qlQuery(apiUrl, authHeader, inferSubQueryStatement);
      
      let inferSuccessful = false;
      if (inferResults !== null) {
        schemaFields = _processInferSchemaOutput(inferResults);
        // Check if _processInferSchemaOutput returned a valid schema (not empty or placeholder)
        if (schemaFields && schemaFields.length > 0 && !(schemaFields.length === 1 && schemaFields[0].name.startsWith('empty_'))) {
          inferSuccessful = true;
          Logger.log('getSchema (customQuery): Successfully inferred schema via INFER (subquery). Schema: %s', JSON.stringify(schemaFields));
        } else {
          Logger.log('getSchema (customQuery): INFER (subquery) executed but processed schema is empty or placeholder. Results: %s', JSON.stringify(inferResults));
        }
      } else {
        Logger.log('getSchema (customQuery): INFER (subquery) execution failed.');
      }

      if (inferSuccessful) {
        return { schema: schemaFields };
      } else {
        Logger.log('getSchema (customQuery): INFER (subquery) failed or yielded unusable schema. Falling back to single document fetch method.');
        
        let queryForFallback = originalUserQuery;
        // Append `LIMIT 1` for fallback if no LIMIT clause exists.
        if (!queryForFallback.toLowerCase().includes('limit')) {
          queryForFallback += ' LIMIT 1';
        }
        Logger.log('getSchema (customQuery Fallback): Running query for single document schema inference: %s', queryForFallback);

        const queryServiceUrl = `${apiUrl}/_p/query/query/service`; // N1QL query endpoint.
        const fetchOptions = {
          method: 'post',
          contentType: 'application/json',
          headers: { 'Authorization': authHeader },
          payload: JSON.stringify({ statement: queryForFallback }),
          muteHttpExceptions: true,
          validateHttpsCertificates: false
        };

        const response = UrlFetchApp.fetch(queryServiceUrl, fetchOptions);
        if (response.getResponseCode() !== 200) {
          _throwUserError(`Couchbase Query API error for custom query schema (fallback) (${response.getResponseCode()}): ${response.getContentText()}`);
        }

        const queryResult = JSON.parse(response.getContentText());
        if (!queryResult.results || queryResult.results.length === 0) {
          Logger.log('getSchema (customQuery Fallback): Custom query returned no results for schema inference.');
          return { schema: [{ name: 'empty_custom_query_result', label: 'Empty Custom Query Result (Fallback)', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
        }

        const documentForSchemaInference = queryResult.results[0];
        Logger.log('getSchema (customQuery Fallback): Successfully retrieved sample document.');

        /**
         * @private
         * Processes a single document (object) to infer schema fields for custom queries (fallback).
         */
        function processFieldsForCustomQuery(obj, prefix = '') {
          // (Implementation of this helper remains the same as previous version)
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
              name: fieldName, label: fieldName, dataType: dataType, semantics: { conceptType: conceptType }
            });
          });
          return fields;
        }
        schemaFields = processFieldsForCustomQuery(documentForSchemaInference);

        if (schemaFields.length === 0) {
          Logger.log('Warning: Schema inference for custom query (fallback) resulted in zero fields.');
          return { schema: [{ name: 'empty_custom_query_schema_fallback', label: 'Empty Custom Query Schema (Fallback)', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
        }
        Logger.log('getSchema (customQuery Fallback): Final inferred schema: %s', JSON.stringify(schemaFields));
        return { schema: schemaFields };
      }
    } else {
      // Handle invalid or unspecified configuration mode.
      _throwUserError('Invalid configuration mode for schema inference.');
    }

  } catch (e) {
    // Catch-all for errors during schema retrieval.
    Logger.log('Error in getSchema: %s. Stack: %s', e.message, e.stack);
    _throwUserError(`Error inferring schema: ${e.message}`); // Throw a user-friendly error.
  }
}

/**
 * @private
 * Retrieves a value from a nested object or array using a dot-separated path string.
 * Handles paths that may include array indices (though the current implementation
 * in the source treats numeric path parts as keys if the current context is an array).
 *
 * @param {Object|Array} obj The object or array from which to retrieve the value.
 * @param {string} pathString The dot-separated path to the desired value (e.g., "user.address.street", "items.0.name").
 * @return {*} The value at the specified path, or `null` if the path is invalid or the value is not found.
 */
function _getNestedValue(obj, pathString) {
  // Normalize path for array indexing (e.g., "items[0]" to "items.0") although source uses direct numeric keys for arrays.
  const parts = pathString.replace(/[\[(\d+)\]]/g, '.$1').split('.');
  let current = obj; // Start with the root object.

  for (let i = 0; i < parts.length; i++) {
    if (current === null || current === undefined) return null; // Path is invalid if current becomes null/undefined.

    const key = parts[i];
    // Check if the key is a number and current context is an array (basic array indexing attempt).
    if (!isNaN(key) && Array.isArray(current)) {
      const index = parseInt(key, 10);
      current = index < current.length ? current[index] : null; // Access array element or null if out of bounds.
    } else if (typeof current === 'object' && current !== null) { // If current is an object, access property by key.
      current = current[key]; // Access property using the key.
    } else {
      // If current is not an object/array or key doesn't fit, path is invalid.
      return null;
    }
  }
  return current; // Return the final value found at the end of the path.
}


/**
 * Returns the data for the given request. This function is called by Looker Studio
 * to fetch the actual data rows based on the user's configuration and the
 * fields requested in the report.
 *
 * It first obtains the master schema (by calling `getSchema` internally, though the
 * provided code implies `getSchema` is called by Looker Studio and its result is available).
 * Then, it determines which fields are requested by Looker Studio for this specific
 * `getData` call using `_getRequestedFields`.
 *
 * Based on `configMode`:
 * - 'collection': Constructs a `SELECT RAW collectionName ... LIMIT maxRows` query.
 * - 'customQuery': Executes the user-provided N1QL query.
 *
 * Finally, it transforms the retrieved documents into the row format expected by Looker Studio.
 *
 * @param {Object} request The request object from Looker Studio, containing `configParams`
 * and `fields` (the fields requested for this data fetch).
 * @return {Object} A data response object with `schema` (the schema for the requested fields)
 * and `rows` (an array of data rows).
 * @throws {UserError} If credentials are missing, configuration is invalid, or data
 * retrieval fails.
 */
function getData(request) {
  Logger.log('getData request: %s', JSON.stringify(request));
  try {
    // Retrieve stored authentication credentials.
    const userProperties = PropertiesService.getUserProperties();
    const path = userProperties.getProperty('dscc.path');
    const username = userProperties.getProperty('dscc.username');
    const password = userProperties.getProperty('dscc.password');

    if (!path || !username || !password) {
      _throwUserError('Authentication credentials missing.');
    }

    const configParams = request.configParams || {}; // User configuration.
    const apiUrl = _constructApiUrl(path); // Base API URL.

    // Obtain the master schema definition. This defines all possible fields.
    const masterSchema = getSchema(request).schema;
    if (!masterSchema || masterSchema.length === 0) {
        _throwUserError('Failed to obtain a valid master schema for getData.');
    }
    Logger.log('getData: Obtained masterSchema with %s fields.', masterSchema.length);

    const requestedFieldsObject = _getRequestedFields(request.fields, masterSchema);
    const schemaForResponse = requestedFieldsObject.build();

    let documents = []; // Array to store documents fetched from Couchbase.
    const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password); // Auth header.

    // Fetch documents based on configuration mode. (Order switched: collection first)
    if (configParams.configMode === 'collection') {
        if (!configParams.collection || configParams.collection.trim() === '') {
            _throwUserError('Collection must be specified in "Query by Collection" mode.');
        }
        const collectionParts = configParams.collection.split('.');
        if (collectionParts.length !== 3) {
            _throwUserError('Invalid collection path. Format: bucket.scope.collection');
        }

        const bucketName = `\`${collectionParts[0]}\``;
        const scopeName = `\`${collectionParts[1]}\``;
        const collectionName = `\`${collectionParts[2]}\``;

        const maxRows = parseInt(configParams.maxRows, 10) || 100;

        const statement = `SELECT RAW ${collectionName} FROM ${bucketName}.${scopeName}.${collectionName} LIMIT ${maxRows}`;

        Logger.log('getData (collectionMode): Retrieving documents. Statement: %s', statement);
        const queryResults = _executeN1qlQuery(apiUrl, authHeader, statement);

        if (queryResults === null) {
            _throwUserError('Failed to retrieve documents for getData (collection mode). Check logs for query error details.');
        }
        documents = queryResults;
        Logger.log('getData (collectionMode): Successfully retrieved %s documents.', documents.length);

    } else if (configParams.configMode === 'customQuery') {
        if (!configParams.query || configParams.query.trim() === '') {
            _throwUserError('Custom query must be specified in "Use Custom Query" mode.');
        }
        let userQuery = configParams.query.trim();
        Logger.log('getData (customQueryMode): Executing custom query: %s', userQuery);
        const queryResults = _executeN1qlQuery(apiUrl, authHeader, userQuery);

        if (queryResults === null) {
            _throwUserError('Failed to retrieve documents for custom query. Check logs for N1QL error details.');
        }
        documents = queryResults;
        Logger.log('getData (customQueryMode): Successfully retrieved %s documents.', documents.length);
    } else {
      _throwUserError('Invalid configuration mode specified for getData.');
    }

    // Transform retrieved documents into Looker Studio row format.
    const rows = documents.map(doc => {
      const values = [];
      schemaForResponse.forEach(fieldDefinition => {
        const fieldName = fieldDefinition.name;
        const lookerDataType = fieldDefinition.dataType;

        let value = _getNestedValue(doc, fieldName);
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
    _throwUserError(`Error retrieving data: ${errorMessage}`);
  }
}


// ==========================================================================
// ===                       ADMIN USER FUNCTION                          ===
// ==========================================================================

/**
 * Returns whether the current user is an admin user.
 * This function is part of the standard Looker Studio connector interface but is
 * marked as currently unused in this specific connector's logic.
 *
 * @return {boolean} Always returns false in this implementation.
 */
function isAdminUser() {
  // This connector does not currently implement different behavior for admin users.
  return false;
}