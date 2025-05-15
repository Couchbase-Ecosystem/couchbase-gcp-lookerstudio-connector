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

  config
    .newInfo()
    .setId('instructions')
    .setText('Select a collection OR enter a document key path for direct access to a specific document.');

  // Fetch buckets, scopes, and collections
  const metadata = fetchCouchbaseMetadata();
  Logger.log('getConfig: Metadata fetch returned buckets: %s', JSON.stringify(metadata.buckets));
  
  // Use Single Select for the collection, as only the first is used by getSchema/getData
  const collectionSelect = config
    .newSelectSingle()
    .setId('collection')
    .setName('Couchbase Collection')
    .setHelpText('Select the collection to query data from (ignored if Document Key Path is entered).')
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

  // Add document key path option
  config
    .newTextInput()
    .setId('documentKeyPath')
    .setName('Document Key Path')
    .setHelpText('Enter a document key path in format "bucket/scope/collection/documentKey". If entered, this will be used instead of the collection selection above.')
    .setPlaceholder('travel-sample/inventory/airline/airline_10')
    .setAllowOverride(true);
  
  // Add max rows option
  config
    .newTextInput()
    .setId('maxRows')
    .setName('Maximum Rows')
    .setHelpText('Maximum number of rows to return (default: 100)')
    .setPlaceholder('100')
    .setAllowOverride(true);

  return config.build();
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
  
  // Check that either a collection or document key path is provided
  if ((!configParams.collection || configParams.collection.trim() === '') && 
      (!configParams.documentKeyPath || configParams.documentKeyPath.trim() === '')) {
    throwUserError('Either a collection or a document key path must be specified');
  }
  
  // Create a validated config object with defaults
  const validatedConfig = {
    path: path,
    username: username,
    password: password,
    collection: configParams.collection ? configParams.collection.trim() : '',
    documentKeyPath: configParams.documentKeyPath ? configParams.documentKeyPath.trim() : '',
    maxRows: configParams.maxRows && parseInt(configParams.maxRows) > 0 ? 
             parseInt(configParams.maxRows) : 100
  };
  
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

    if (configParams.documentKeyPath && configParams.documentKeyPath.trim() !== '') {
      const docPathParts = configParams.documentKeyPath.split('/');
      if (docPathParts.length !== 4) {
        throwUserError('Invalid document key path. Format should be "bucket/scope/collection/documentKey"');
      }
      const [bucket, scope, collection, documentKey] = docPathParts;
      const documentUrl = `${apiUrl}/v1/buckets/${bucket}/scopes/${scope}/collections/${collection}/documents/${documentKey}`;
      Logger.log('getSchema: Retrieving specific document for schema: %s', documentUrl);

      const fetchOptions = {
        method: 'get',
        contentType: 'application/json',
        headers: { 'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password) },
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      const response = UrlFetchApp.fetch(documentUrl, fetchOptions);
      if (response.getResponseCode() !== 200) {
        throwUserError(`Couchbase API error (${response.getResponseCode()}): ${response.getContentText()}`);
      }
      documentForSchemaInference = JSON.parse(response.getContentText());
      Logger.log('getSchema: Successfully retrieved specific document for schema.');

    } else if (configParams.collection && configParams.collection.trim() !== '') {
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length !== 3) {
        throwUserError('Invalid collection path. Format: bucket.scope.collection');
      }
      const [bucket, scope, collection] = collectionParts;
      const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);
      
      // Use the collection name as an alias for RAW projection.
      // Using direct dot notation for FROM clause as backticks were causing issues via API.
      const statement = `SELECT RAW ${collection} FROM ${bucket}.${scope}.${collection} LIMIT 1`;
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
      throwUserError('Either collection or document key path must be specified for schema inference.');
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
    const maxRows = parseInt(configParams.maxRows, 10) || 100;
    let documents = [];

    if (configParams.documentKeyPath && configParams.documentKeyPath.trim() !== '') {
      const docPathParts = configParams.documentKeyPath.split('/');
      if (docPathParts.length !== 4) {
        throwUserError('Invalid document key path. Format: bucket/scope/collection/documentKey');
      }
      const [bucket, scope, collection, documentKey] = docPathParts;
      const documentUrl = `${apiUrl}/v1/buckets/${bucket}/scopes/${scope}/collections/${collection}/documents/${documentKey}`;
      Logger.log('getData: Retrieving specific document: %s', documentUrl);

      const fetchOptions = {
        method: 'get',
        contentType: 'application/json',
        headers: { 'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password) },
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      const response = UrlFetchApp.fetch(documentUrl, fetchOptions);
      if (response.getResponseCode() !== 200) {
        throwUserError(`Couchbase API error (${response.getResponseCode()}): ${response.getContentText()}`);
      }
      documents.push(JSON.parse(response.getContentText()));
      Logger.log('getData: Successfully retrieved specific document.');

    } else if (configParams.collection && configParams.collection.trim() !== '') {
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length !== 3) {
        throwUserError('Invalid collection path. Format: bucket.scope.collection');
      }
      const [bucket, scope, collection] = collectionParts;
      const queryServiceUrl = `${apiUrl}/_p/query/query/service`;
      // Use the collection name as an alias for RAW projection
      const statement = `SELECT RAW ${collection} FROM \\\`${bucket}\\\`.\\\`${scope}\\\`.\\\`${collection}\\\` LIMIT ${maxRows}`;
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
      throwUserError('Either collection or document key path must be specified.');
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
