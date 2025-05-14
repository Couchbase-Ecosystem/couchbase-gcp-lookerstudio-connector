/**
 * Couchbase Data API Connector for Google Looker Studio
 * This connector allows users to connect to a Couchbase database and run N1QL queries.
 */

// ==========================================================================\n// ===                       AUTHENTICATION FLOW                          ===\n// ==========================================================================

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
 * Helper function to construct the API URL.
 * Handles various path formats (hostname, hostname:port, http(s)://hostname, http(s)://hostname:port).
 */
function constructApiUrl(path, defaultPort) {
  Logger.log('constructApiUrl received path: %s, defaultPort: %s', path, defaultPort);
  if (!path) {
    Logger.log('constructApiUrl: Path is null or empty. Returning null.');
    return null;
  }

  let protocol = '';
  let hostAndPort = path;

  if (path.startsWith('http://')) {
    protocol = 'http://';
    hostAndPort = path.substring(7);
  } else if (path.startsWith('https://')) {
    protocol = 'https://';
    hostAndPort = path.substring(8);
  } else {
    // Default to http if no protocol is specified.
    // For Capella, users must provide https:// explicitly.
    // For self-managed, if they provide only hostname, http is a safe default.
    // The user can override by specifying hostname:port or full https://host:port.
    protocol = 'http://';
  }

  // Check if port is already included in hostAndPort
  const parts = hostAndPort.split(':');
  let finalUrl;
  if (parts.length > 1 && !isNaN(parts[parts.length -1])) { // Port is specified
    finalUrl = protocol + hostAndPort;
  } else { // Port is not specified, use defaultPort
    finalUrl = protocol + hostAndPort + ':' + defaultPort;
  }
  Logger.log('constructApiUrl constructed: %s', finalUrl);
  return finalUrl;
}


/**
 * Attempts to validate credentials by making a minimal query to Couchbase Data API.
 * Called by isAuthValid.
 */
function validateCredentials(path, username, password) {
  Logger.log('validateCredentials (Data API) received path: %s', path);
  Logger.log('Attempting to validate credentials against Data API for path: %s, username: %s', path, username);

  if (!path || !username || !password) {
    Logger.log('Validation failed: Missing path, username, or password.');
    return false;
  }

  // Use constructApiUrl for consistent URL handling. Port 18093 is common for N1QL over HTTP.
  // For HTTPS, it's often 18094. If user provides full URL with port, that will be used.
  const dataApiBaseUrl = constructApiUrl(path, 18093); 
  if (!dataApiBaseUrl) {
    Logger.log('validateCredentials (Data API): Could not construct base URL from path: %s', path);
    return false;
  }
  const queryUrl = dataApiBaseUrl + '/query/service'; // Standard N1QL endpoint
  Logger.log('validateCredentials (Data API) constructed N1QL queryUrl: %s', queryUrl);

  const queryPayload = {
    statement: 'SELECT "ok" AS status;', // Simple N1QL query for validation
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
    validateHttpsCertificates: false // Consider making this configurable or stricter for production
  };

  try {
    Logger.log('Sending validation request to Data API...');
    const response = UrlFetchApp.fetch(queryUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();
    Logger.log('Data API Validation response code: %s', responseCode);

    if (responseCode === 200) {
      const responseData = JSON.parse(responseText);
      if (responseData.status === 'success' && responseData.results && responseData.results[0] && responseData.results[0].status === 'ok') {
        Logger.log('Data API Credential validation successful.');
        return true;
      } else {
        Logger.log('Data API Credential validation failed. Response OK, but content unexpected: %s', responseText);
        return false;
      }
    } else {
      Logger.log('Data API Credential validation failed. Code: %s, Response: %s', responseCode, responseText);
      return false;
    }
  } catch (e) {
    Logger.log('Data API Credential validation failed with exception: %s', e.toString());
    Logger.log('Exception details: %s', e.stack);
    return false;
  }
}

/**
 * Returns true if the auth service has access (credentials are stored and valid).
 */
function isAuthValid() {
  Logger.log('isAuthValid (Data API) called.');
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');

  // Log retrieved properties (mask password)
  Logger.log('isAuthValid (Data API): Path from props: %s, Username from props: %s', path, username);

  if (!path || !username || !password) {
     Logger.log('isAuthValid (Data API): Credentials not found in storage.');
     return false;
  }

  Logger.log('isAuthValid (Data API): Found credentials. Performing live validation test.');
  const isValid = validateCredentials(path, username, password);
  Logger.log('isAuthValid (Data API): Validation result: %s', isValid);
  return isValid;
}

/**
 * Sets the credentials entered by the user.
 */
function setCredentials(request) {
  Logger.log('setCredentials (Data API) called.');
  const creds = request.pathUserPass;
  const path = creds.path;
  const username = creds.username;
  const password = creds.password;

  Logger.log('Received path: %s, username: %s, password: %s', path, username, '********');

  // Basic validation of input before storing
  if (!path || typeof path !== 'string' || path.trim() === '' ||
      !username || typeof username !== 'string' || username.trim() === '' ||
      !password || typeof password !== 'string' ) {
    Logger.log('setCredentials (Data API) error: Invalid credentials received - path, username, or password missing or invalid type.');
    return {
      errorCode: 'INVALID_CREDENTIALS',
      errorText: 'Path, username, and password must be provided and valid.'
    };
  }
  
  // Attempt to validate before storing
  const isValid = validateCredentials(path, username, password);
  if (!isValid) {
    Logger.log('setCredentials (Data API) error: Provided credentials failed validation.');
    return {
      errorCode: 'INVALID_CREDENTIALS',
      errorText: 'The provided credentials could not be validated. Please check the path, username, and password.'
    };
  }

  try {
    const userProperties = PropertiesService.getUserProperties();
    userProperties.setProperty('dscc.path', path);
    userProperties.setProperty('dscc.username', username);
    userProperties.setProperty('dscc.password', password);
    Logger.log('Data API Credentials stored successfully.');
  } catch (e) {
    Logger.log('Error storing Data API credentials: %s', e.toString());
    return {
      errorCode: 'SystemError',
      errorText: 'Failed to store credentials: ' + e.toString()
    };
  }

  Logger.log('setCredentials (Data API) finished successfully.');
  return {
    errorCode: 'NONE'
  };
}

/**
 * Resets the auth service (clears stored credentials).
 */
function resetAuth() {
  Logger.log('resetAuth (Data API) called.');
  try {
    const userProperties = PropertiesService.getUserProperties();
    userProperties.deleteProperty('dscc.path');
    userProperties.deleteProperty('dscc.username');
    userProperties.deleteProperty('dscc.password');
    Logger.log('Data API Auth properties deleted.');
  } catch (e) {
    Logger.log('Error during Data API resetAuth: %s', e.toString());
    // Optionally, return an error if this function is expected to provide feedback
  }
}

// ==========================================================================\n// ===                      CONFIGURATION FLOW                           ===\n// ==========================================================================

/**
 * Returns the connector configuration.
 */
function getConfig(request) {
  Logger.log('getConfig (Data API) called. Request: %s', JSON.stringify(request));
  const cc = DataStudioApp.createCommunityConnector();
  const config = cc.getConfig();

  // Add a general N1QL query text area
  config.newInfo()
    .setId('instructions')
    .setText('Enter your N1QL query. For example, SELECT * FROM \`travel-sample\`.inventory.hotel LIMIT 100.');

  config.newTextArea()
    .setId('n1qlQuery')
    .setName('N1QL Query')
    .setHelpText('Enter the N1QL query to execute (e.g., SELECT শহর FROM \`travel-sample\`.inventory.landmark WHERE country = "United States" AND STRPOS(city, "San") = 0 LIMIT 10)')
    .setPlaceholder('SELECT * FROM \`your-bucket\`.\`your-scope\`.\`your-collection\` LIMIT 10')
    .setAllowOverride(true);
  
  // Add a date range (optional, but good practice)
  config.setDateRangeRequired(false); // N1QL queries might not always use date ranges

  // TODO: Add dynamic fetching of buckets, scopes, collections for user-friendly selection
  // This would involve:
  // 1. Calling a helper function like `fetchDataApiMetadata()`
  // 2. `fetchDataApiMetadata` would use stored credentials to query system catalogs
  //    (e.g., `SELECT RAW name FROM system:datastores WHERE state = 'online'` for buckets)
  //    (e.g., `SELECT RAW name FROM system:namespaces WHERE datastore_id = $BUCKET_NAME` for scopes)
  //    (e.g., `SELECT RAW name FROM system:keyspaces WHERE namespace_id = $SCOPE_NAME` for collections)
  // 3. Populating selectSingles or other config elements with the results.
  // For now, users must type the full path in the N1QL query.

  Logger.log('getConfig (Data API) response: %s', JSON.stringify(config.build()));
  return config.build();
}

// ==========================================================================\n// ===                      SCHEMA & DATA FLOW                            ===\n// ==========================================================================

/**
 * Returns the schema for the given request.
 */
function getSchema(request) {
  Logger.log('getSchema (Data API) called. Request: %s', JSON.stringify(request));
  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');
  
  const n1qlQuery = request.configParams && request.configParams.n1qlQuery ? request.configParams.n1qlQuery : null;

  if (!n1qlQuery) {
    Logger.log('getSchema (Data API) - N1QL query is missing.');
    return DataStudioApp.createCommunityConnector()
      .newGetSchemaResponse()
      .setError('USER', 'N1QL Query is missing. Please configure the connector with a valid N1QL query.')
      .build();
  }

  // TODO: Implement actual schema inference for Data API (N1QL)
  // 1. Execute a modified version of the user's N1QL query (e.g., with LIMIT 1 or using INFER)
  //    to get a sample of the data structure.
  // 2. Parse the sample result to determine field names and types.
  // 3. Remember: "array infer doesn't work unless it's columnar. we need to use the regular infer command for data api."
  //    This implies using N1QL's INFER command:
  //    `INFER \`bucket\`.\`scope\`.\`collection\` WITH {"sample_size": 1000, "num_sample_values": 5, "similarity_metric": 0.5}`
  //    However, INFER works on a collection, not directly on a query result.
  //    Alternatively, execute the query with LIMIT 1 and derive schema from the result.
  // For now, returning a placeholder schema.

  // Placeholder: If the query is simple like 'SELECT "ok" AS status', provide that schema
  if (n1qlQuery.toLowerCase().trim() === 'select "ok" as status;') {
    return DataStudioApp.createCommunityConnector().newGetSchemaResponse()
      .newFields()
      .newDimension()
      .setId('status')
      .setName('Status')
      .setType(DataStudioApp.createCommunityConnector().FieldType.TEXT)
      .build()
      .build();
  }
  
  // For any other query, we need a more robust inference.
  // This is a very basic example assuming a single field 'data'.
  // Replace with actual inference logic.
  
  // Try to infer from a sample document (LIMIT 1)
  const dataApiBaseUrl = constructApiUrl(path, 18093);
  if (!dataApiBaseUrl) {
    Logger.log('getSchema (Data API): Could not construct base URL from path: %s', path);
    return DataStudioApp.createCommunityConnector().newGetSchemaResponse().setError('SYSTEM_ERROR', 'Invalid server path configuration.').build();
  }
  const queryUrl = dataApiBaseUrl + '/query/service';

  // Modify user's query to get just one result for schema inference.
  // This is a simplistic approach; be careful with complex queries (e.g., with GROUP BY, UNION).
  // A more robust method might involve parsing the query to add LIMIT 1 safely.
  let inferSchemaQuery = n1qlQuery;
  if (!n1qlQuery.toUpperCase().includes('LIMIT ')) {
    inferSchemaQuery = n1qlQuery + ' LIMIT 1';
  } else {
    // If LIMIT is already there, try to make it LIMIT 1. This is tricky.
    // For simplicity, we'll just use the existing LIMIT or add one if not present.
    // A more robust solution would parse and replace the LIMIT clause.
    Logger.log("Query already contains LIMIT. Using it as is for schema inference sample or adding LIMIT 1 if it doesn't exist.");
  }


  const queryPayload = {
    statement: inferSchemaQuery,
    timeout: '10s' // Longer timeout for schema inference if needed
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
    Logger.log('Fetching sample data for schema inference from Data API. Query: %s', inferSchemaQuery);
    const response = UrlFetchApp.fetch(queryUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    if (responseCode === 200) {
      const responseData = JSON.parse(responseText);
      if (responseData.status === 'success' && responseData.results && responseData.results.length > 0) {
        const sampleDoc = responseData.results[0];
        const fields = DataStudioApp.createCommunityConnector().newFields();
        
        // Infer schema from the first document's top-level keys
        // This is a basic inference. Needs to handle nested objects, arrays, and diverse types.
        for (const key in sampleDoc) {
          if (Object.prototype.hasOwnProperty.call(sampleDoc, key)) {
            const value = sampleDoc[key];
            let fieldType = DataStudioApp.createCommunityConnector().FieldType.TEXT; // Default
            
            if (typeof value === 'number') {
              fieldType = DataStudioApp.createCommunityConnector().FieldType.NUMBER;
            } else if (typeof value === 'boolean') {
              fieldType = DataStudioApp.createCommunityConnector().FieldType.BOOLEAN;
            } else if (typeof value === 'string') {
               // Could add date/datetime detection here
               if (/^\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})?)?$/.test(value)) {
                 fieldType = DataStudioApp.createCommunityConnector().FieldType.YEAR_MONTH_DAY_HOUR; // Or other date types
               } else {
                 fieldType = DataStudioApp.createCommunityConnector().FieldType.TEXT;
               }
            }
            // Note: Arrays and nested objects are not fully handled here. Looker Studio may flatten them.
            // For arrays, one might pick the first element's type or create multiple fields.
            // For nested objects, one might flatten (e.g., parent.child) or allow Looker to handle.

            // Use the key as both ID and Name. Sanitize if necessary.
            const fieldId = key.replace(/[^a-zA-Z0-9_]/g, '_'); // Basic sanitization
            fields.newDimension() // Default to dimension, can be changed to metric based on type/semantics
              .setId(fieldId)
              .setName(key)
              .setType(fieldType);
          }
        }
        if (fields.build().length === 0) {
             Logger.log('getSchema (Data API) - No fields inferred from sample document. Query: %s, Sample: %s', inferSchemaQuery, JSON.stringify(sampleDoc));
             return DataStudioApp.createCommunityConnector().newGetSchemaResponse().setError('USER', 'Could not infer schema. The query returned no fields or an empty result.').build();
        }
        Logger.log('getSchema (Data API) - Successfully inferred schema.');
        return fields.buildResponse();

      } else {
        Logger.log('getSchema (Data API) - Failed to fetch sample data or query returned no results. Code: %s, Response: %s', responseCode, responseText);
        return DataStudioApp.createCommunityConnector().newGetSchemaResponse().setError('USER', 'Failed to retrieve data for schema inference. Response: ' + responseText).build();
      }
    } else {
      Logger.log('getSchema (Data API) - Error fetching sample data for schema. Code: %s, Response: %s', responseCode, responseText);
      return DataStudioApp.createCommunityConnector().newGetSchemaResponse().setError('SYSTEM_ERROR', 'Error fetching data for schema. Code: ' + responseCode + ', Msg: ' + responseText).build();
    }

  } catch (e) {
    Logger.log('getSchema (Data API) - Exception during schema inference: %s', e.toString());
    Logger.log('Exception details: %s', e.stack);
    return DataStudioApp.createCommunityConnector().newGetSchemaResponse().setError('SYSTEM_ERROR', 'Exception during schema inference: ' + e.message).build();
  }
}

/**
 * Returns the tabular data for the given request.
 */
function getData(request) {
  Logger.log('getData (Data API) called. Request: %s', JSON.stringify(request));

  const userProperties = PropertiesService.getUserProperties();
  const path = userProperties.getProperty('dscc.path');
  const username = userProperties.getProperty('dscc.username');
  const password = userProperties.getProperty('dscc.password');

  const n1qlQuery = request.configParams && request.configParams.n1qlQuery ? request.configParams.n1qlQuery : null;

  if (!n1qlQuery) {
    Logger.log('getData (Data API) - N1QL query is missing.');
    return DataStudioApp.createCommunityConnector()
      .newGetDataResponse()
      .setError('USER', 'N1QL Query is missing. Please configure the connector with a valid N1QL query.')
      .build();
  }
  
  // Fetch the schema based on the request.fields
  // The request.fields object contains the fields requested by Looker Studio.
  // We should use these fields to structure our response.
  const requestedFieldIds = request.fields.map(field => field.name); // Using name as it was set from key in getSchema
  Logger.log('getData (Data API) - Requested fields: %s', JSON.stringify(requestedFieldIds));


  const dataApiBaseUrl = constructApiUrl(path, 18093); // Or your configured Data API port
  if (!dataApiBaseUrl) {
    Logger.log('getData (Data API): Could not construct base URL from path: %s', path);
    return DataStudioApp.createCommunityConnector().newGetDataResponse().setError('SYSTEM_ERROR', 'Invalid server path configuration.').build();
  }
  const queryUrl = dataApiBaseUrl + '/query/service';

  // TODO: Handle pagination if the API supports it and Looker Studio requests it.
  // TODO: Incorporate date range filters (request.dateRange) if applicable to the N1QL query.
  //       This would typically involve adding a WHERE clause to the N1QL query.
  //       Example: If dateRange.startDate and dateRange.endDate are present, and you have a date field 'event_timestamp':
  //       `WHERE event_timestamp >= "${request.dateRange.startDate}" AND event_timestamp <= "${request.dateRange.endDate}"`

  const queryPayload = {
    statement: n1qlQuery,
    // Consider adding a reasonable timeout from connector config or a default
    // timeout: request.configParams.timeout || '60s' 
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(queryPayload),
    headers: {
      Authorization: 'Basic ' + Utilities.base64Encode(username + ':' + password)
    },
    muteHttpExceptions: true,
    validateHttpsCertificates: false // As before, consider implications
  };

  try {
    Logger.log('Executing N1QL query for getData (Data API): %s', n1qlQuery);
    const response = UrlFetchApp.fetch(queryUrl, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    if (responseCode === 200) {
      const responseData = JSON.parse(responseText);
      if (responseData.status === 'success' && responseData.results) {
        Logger.log('getData (Data API) - Query successful. Processing %s results.', responseData.results.length);
        
        const dataResponse = DataStudioApp.createCommunityConnector().newGetDataResponse();
        const dataSchema = [];
        request.fields.forEach(field => {
            dataSchema.push({
                name: field.name, // This should match the ID/name from getSchema
                dataType: field.dataType // Get the type from the request fields
            });
        });
        dataResponse.setFields(request.fields);


        responseData.results.forEach(row => {
          const values = [];
          // Ensure values are pushed in the order of requestedFieldIds
          requestedFieldIds.forEach(requestedFieldName => {
            // Handle cases where a field might be missing in a specific document (NoSQL nature)
            let value = row[requestedFieldName];
            if (value === undefined || value === null) {
                 // Determine the type of the field from dataSchema to provide a typed null/default
                const fieldSchema = dataSchema.find(f => f.name === requestedFieldName);
                if (fieldSchema) {
                    switch (fieldSchema.dataType) {
                        case DataStudioApp.createCommunityConnector().FieldType.NUMBER:
                            value = null; // Or 0 if appropriate
                            break;
                        case DataStudioApp.createCommunityConnector().FieldType.BOOLEAN:
                            value = null; // Or false if appropriate
                            break;
                        default:
                            value = null; // Or "" for text
                    }
                } else {
                    value = null; // Default if type not found (should not happen if schema is consistent)
                }
            }
            // TODO: Add more robust type conversion/casting based on schema if needed
            values.push(value);
          });
          dataResponse.newRow().setValues(values);
        });
        
        return dataResponse.build();
      } else {
        Logger.log('getData (Data API) - Query failed or returned unexpected structure. Response: %s', responseText);
        return DataStudioApp.createCommunityConnector().newGetDataResponse().setError('USER', 'N1QL query execution failed or returned unexpected data. Details: ' + responseText).build();
      }
    } else {
      Logger.log('getData (Data API) - Error executing N1QL query. Code: %s, Response: %s', responseCode, responseText);
      return DataStudioApp.createCommunityConnector().newGetDataResponse().setError('SYSTEM_ERROR', 'Error executing N1QL query. Code: ' + responseCode + '. Response: ' + responseText).build();
    }

  } catch (e) {
    Logger.log('getData (Data API) - Exception during N1QL query execution: %s', e.toString());
    Logger.log('Exception details: %s', e.stack);
    return DataStudioApp.createCommunityConnector().newGetDataResponse().setError('SYSTEM_ERROR', 'Exception during N1QL query execution: ' + e.message).build();
  }
}

// ==========================================================================\n// ===                      ADMIN / UTILITY                               ===\n// ==========================================================================

// Required for Community Connectors.
function isAdminUser() {
  // Check if the effective user is an admin.
  // This implementation assumes all users are admins for simplicity.
  // For more complex scenarios, you might check against a list of admin email addresses.
  // const effectiveUserEmail = Session.getEffectiveUser().getEmail();
  // return ADMIN_USERS_LIST.includes(effectiveUserEmail); // Where ADMIN_USERS_LIST is predefined
  Logger.log('isAdminUser (Data API) called. Returning true by default.');
  return true; 
}
