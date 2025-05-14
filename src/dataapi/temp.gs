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
  
  // Test endpoint - we'll try to get a list of buckets which requires valid credentials
  const bucketListUrl = apiUrl + '/v2/databases';
  Logger.log('validateCredentials constructed Data API URL: %s', bucketListUrl);

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
    const response = UrlFetchApp.fetch(bucketListUrl, options);
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
 * Fetches available buckets, scopes, and collections from Couchbase Data API.
 * Used to populate dropdowns in the config UI.
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
      buckets: [],
      scopesCollections: {}
    };
  }
  
  // Construct API URL for Data API
  const apiUrl = constructApiUrl(path);
  const bucketsUrl = apiUrl + '/v2/databases';
  Logger.log('fetchCouchbaseMetadata: Using Data API URL: %s', bucketsUrl);

  const authHeader = 'Basic ' + Utilities.base64Encode(username + ':' + password);
  const options = {
    method: 'get',
    contentType: 'application/json',
    headers: {
      Authorization: authHeader
    },
    muteHttpExceptions: true,
    validateHttpsCertificates: false
  };
  
  // Initialize empty result structure
  let bucketNames = [];
  const scopesCollections = {};
  
  try {
    // First get all buckets
    Logger.log('fetchCouchbaseMetadata: Querying for buckets');
    const bucketResponse = UrlFetchApp.fetch(bucketsUrl, options);
    
    if (bucketResponse.getResponseCode() === 200) {
      const bucketData = JSON.parse(bucketResponse.getContentText());
      
      if (bucketData && Array.isArray(bucketData)) {
        bucketNames = bucketData
          .filter(bucket => bucket.name) // Filter out any null or undefined
          .map(bucket => bucket.name);
        
        Logger.log('fetchCouchbaseMetadata: Found buckets: %s', bucketNames.join(', '));
      } else {
        Logger.log('fetchCouchbaseMetadata: Bucket query result format unexpected or empty.');
      }
    } else {
      Logger.log('Error fetching buckets. Code: %s, Response: %s', 
                bucketResponse.getResponseCode(), bucketResponse.getContentText());
    }
    
    // Now get scopes and collections for each bucket
    for (const bucket of bucketNames) {
      scopesCollections[bucket] = {};
      const scopesUrl = apiUrl + `/v2/databases/${bucket}/scopes`;
      
      Logger.log('fetchCouchbaseMetadata: Querying for scopes in bucket: %s', bucket);
      const scopesResponse = UrlFetchApp.fetch(scopesUrl, options);
      
      if (scopesResponse.getResponseCode() === 200) {
        const scopesData = JSON.parse(scopesResponse.getContentText());
        
        if (scopesData && Array.isArray(scopesData)) {
          Logger.log('fetchCouchbaseMetadata: Found %s scopes in bucket %s', scopesData.length, bucket);
          
          // Process each scope and get its collections
          for (const scope of scopesData) {
            if (!scope.name) continue;
            
            const scopeName = scope.name;
            scopesCollections[bucket][scopeName] = [];
            
            const collectionsUrl = apiUrl + `/v2/databases/${bucket}/scopes/${scopeName}/collections`;
            Logger.log('fetchCouchbaseMetadata: Querying for collections in bucket: %s, scope: %s', bucket, scopeName);
            
            const collectionsResponse = UrlFetchApp.fetch(collectionsUrl, options);
            
            if (collectionsResponse.getResponseCode() === 200) {
              const collectionsData = JSON.parse(collectionsResponse.getContentText());
              
              if (collectionsData && Array.isArray(collectionsData)) {
                Logger.log('fetchCouchbaseMetadata: Found %s collections in scope %s', collectionsData.length, scopeName);
                
                for (const collection of collectionsData) {
                  if (collection.name) {
                    scopesCollections[bucket][scopeName].push(collection.name);
                    Logger.log('fetchCouchbaseMetadata: Added: %s.%s.%s', 
                              bucket, scopeName, collection.name);
                  }
                }
              }
            } else {
              Logger.log('Error fetching collections. Code: %s, Response: %s', 
                        collectionsResponse.getResponseCode(), collectionsResponse.getContentText());
            }
          }
        }
      } else {
        Logger.log('Error fetching scopes. Code: %s, Response: %s', 
                  scopesResponse.getResponseCode(), scopesResponse.getContentText());
      }
    }
    
    // Add _default._default if no other collections were found for a bucket
    bucketNames.forEach(bucketName => {
      if (Object.keys(scopesCollections[bucketName]).length === 0) {
        scopesCollections[bucketName] = { '_default': ['_default'] };
        Logger.log('fetchCouchbaseMetadata: Added default keyspace for bucket %s', bucketName);
      }
    });

    return {
      buckets: bucketNames,
      scopesCollections: scopesCollections
    };
    
  } catch (e) {
    Logger.log('Error in fetchCouchbaseMetadata: %s', e.toString());
    Logger.log('Exception details: %s', e.stack);
    return {
      buckets: [],
      scopesCollections: {}
    };
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
    
    // Get configuration parameters
    const configParams = request.configParams || {};
    
    // Construct the API URL
    const apiUrl = constructApiUrl(path);
    
    // Determine if we're using document key path or collection
    let documentData;
    
    if (configParams.documentKeyPath && configParams.documentKeyPath.trim() !== '') {
      // Use document key path to get a specific document
      const docPathParts = configParams.documentKeyPath.split('/');
      if (docPathParts.length !== 4) {
        throwUserError('Invalid document key path. Format should be "bucket/scope/collection/documentKey"');
      }
      
      const [bucket, scope, collection, documentKey] = docPathParts;
      const documentUrl = `${apiUrl}/v1/buckets/${bucket}/scopes/${scope}/collections/${collection}/documents/${documentKey}`;
      
      Logger.log('getSchema: Retrieving specific document using URL: %s', documentUrl);
      
      const options = {
        method: 'get',
        contentType: 'application/json',
        headers: {
          'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password)
        },
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      
      const response = UrlFetchApp.fetch(documentUrl, options);
      const responseCode = response.getResponseCode();
      
      if (responseCode !== 200) {
        Logger.log('API error in getSchema for document: %s, Error: %s', responseCode, response.getContentText());
        throwUserError(`Couchbase API error (${responseCode}): ${response.getContentText()}`);
      }
      
      documentData = JSON.parse(response.getContentText());
      Logger.log('getSchema: Successfully retrieved document data');
      
    } else if (configParams.collection && configParams.collection.trim() !== '') {
      // Use collection path to get documents
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length !== 3) {
        throwUserError('Invalid collection path specified. Use format: bucket.scope.collection');
      }
      
      const [bucket, scope, collection] = collectionParts;
      const documentsUrl = `${apiUrl}/v1/buckets/${bucket}/scopes/${scope}/collections/${collection}/docs?limit=1`;
      
      Logger.log('getSchema: Retrieving sample document using URL: %s', documentsUrl);
      
      const options = {
        method: 'get',
        contentType: 'application/json',
        headers: {
          'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password)
        },
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      
      const response = UrlFetchApp.fetch(documentsUrl, options);
      const responseCode = response.getResponseCode();
      
      if (responseCode !== 200) {
        Logger.log('API error in getSchema for collection: %s, Error: %s', responseCode, response.getContentText());
        throwUserError(`Couchbase API error (${responseCode}): ${response.getContentText()}`);
      }
      
      const responseData = JSON.parse(response.getContentText());
      
      if (!responseData.results || responseData.results.length === 0) {
        Logger.log('No documents found in collection');
        // Return a minimal default schema
        return { schema: [{ name: 'empty_result', label: 'Empty Result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
      }
      
      // Use the first document for schema inference
      documentData = responseData.results[0].document;
      Logger.log('getSchema: Successfully retrieved sample document data');
      
    } else {
      throwUserError('Either collection or document key path must be specified to infer schema.');
    }
    
    // Function to recursively process document fields
    function processFields(obj, prefix = '') {
      const fields = [];
      
      if (!obj || typeof obj !== 'object') {
        return fields;
      }
      
      Object.keys(obj).forEach(key => {
        const fieldName = prefix ? `${prefix}.${key}` : key;
        const value = obj[key];
        
        if (value === null || value === undefined) {
          // For null/undefined values, default to STRING type
          fields.push({
            name: fieldName,
            label: fieldName,
            dataType: 'STRING',
            semantics: { conceptType: 'DIMENSION' }
          });
        } else if (typeof value === 'number') {
          fields.push({
            name: fieldName,
            label: fieldName,
            dataType: 'NUMBER',
            semantics: { conceptType: 'METRIC' }
          });
        } else if (typeof value === 'boolean') {
          fields.push({
            name: fieldName,
            label: fieldName,
            dataType: 'BOOLEAN',
            semantics: { conceptType: 'DIMENSION' }
          });
        } else if (typeof value === 'string') {
          // Check if it looks like a URL
          if (value.startsWith('http://') || value.startsWith('https://')) {
            fields.push({
              name: fieldName,
              label: fieldName,
              dataType: 'URL',
              semantics: { conceptType: 'DIMENSION' }
            });
          } else {
            fields.push({
              name: fieldName,
              label: fieldName,
              dataType: 'STRING',
              semantics: { conceptType: 'DIMENSION' }
            });
          }
        } else if (typeof value === 'object' && !Array.isArray(value)) {
          // Handle nested objects
          fields.push(...processFields(value, fieldName));
        } else if (Array.isArray(value)) {
          // For arrays, we'll represent the whole array as a single STRING field
          fields.push({
            name: fieldName,
            label: fieldName,
            dataType: 'STRING',
            semantics: { conceptType: 'DIMENSION' }
          });
          
          // If array contains objects, we could potentially process them recursively
          // This is simplified for now
        }
      });
      
      return fields;
    }
    
    // Process the document to extract schema
    const fields = processFields(documentData);
    
    if (fields.length === 0) {
      Logger.log('Warning: Schema inference resulted in zero fields. Check collection/document and data.');
      // Provide a minimal default schema
      return { schema: [{ name: 'empty_result', label: 'Empty Result', dataType: 'STRING', semantics: { conceptType: 'DIMENSION' }}] };
    }
    
    Logger.log('getSchema: Final inferred schema: %s', JSON.stringify(fields));
    return { schema: fields };
    
  } catch (e) {
    Logger.log('Error in getSchema: %s', e.message);
    Logger.log('getSchema Error Stack: %s', e.stack);
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
    
    if ((!configParams.collection || configParams.collection.trim() === '') && 
        (!configParams.documentKeyPath || configParams.documentKeyPath.trim() === '')) {
      throwUserError('Either collection or document key path must be specified.');
    }
    
    // Get requested fields
    const requestedFieldsObject = getRequestedFields(request);
    const requestedFieldsArray = requestedFieldsObject.asArray();
    const requestedFieldIds = requestedFieldsArray.map(field => field.getId());
    
    // Determine max rows
    const maxRows = parseInt(configParams.maxRows, 10) || 100;
    
    // Construct the API URL
    const apiUrl = constructApiUrl(path);
    
    // Variable to store the documents
    let documents = [];
    
    if (configParams.documentKeyPath && configParams.documentKeyPath.trim() !== '') {
      // Use document key path to get a specific document
      const docPathParts = configParams.documentKeyPath.split('/');
      if (docPathParts.length !== 4) {
        throwUserError('Invalid document key path. Format should be "bucket/scope/collection/documentKey"');
      }
      
      const [bucket, scope, collection, documentKey] = docPathParts;
      const documentUrl = `${apiUrl}/v1/buckets/${bucket}/scopes/${scope}/collections/${collection}/documents/${documentKey}`;
      
      Logger.log('getData: Retrieving specific document using URL: %s', documentUrl);
      
      const options = {
        method: 'get',
        contentType: 'application/json',
        headers: {
          'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password)
        },
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      
      const response = UrlFetchApp.fetch(documentUrl, options);
      const responseCode = response.getResponseCode();
      
      if (responseCode !== 200) {
        Logger.log('API error in getData for document: %s, Error: %s', responseCode, response.getContentText());
        throwUserError(`Couchbase API error (${responseCode}): ${response.getContentText()}`);
      }
      
      // Add the single document to our list
      documents.push(JSON.parse(response.getContentText()));
      Logger.log('getData: Successfully retrieved document data');
      
    } else {
      // Use collection path to get documents
      const collectionParts = configParams.collection.split('.');
      if (collectionParts.length !== 3) {
        throwUserError('Invalid collection path specified. Use format: bucket.scope.collection');
      }
      
      const [bucket, scope, collection] = collectionParts;
      const documentsUrl = `${apiUrl}/v1/buckets/${bucket}/scopes/${scope}/collections/${collection}/docs?limit=${maxRows}`;
      
      Logger.log('getData: Retrieving documents using URL: %s', documentsUrl);
      
      const options = {
        method: 'get',
        contentType: 'application/json',
        headers: {
          'Authorization': 'Basic ' + Utilities.base64Encode(username + ':' + password)
        },
        muteHttpExceptions: true,
        validateHttpsCertificates: false
      };
      
      const response = UrlFetchApp.fetch(documentsUrl, options);
      const responseCode = response.getResponseCode();
      
      if (responseCode !== 200) {
        Logger.log('API error in getData for collection: %s, Error: %s', responseCode, response.getContentText());
        throwUserError(`Couchbase API error (${responseCode}): ${response.getContentText()}`);
      }
      
      const responseData = JSON.parse(response.getContentText());
      
      if (!responseData.results || responseData.results.length === 0) {
        Logger.log('No documents found in collection');
        return {
          schema: requestedFieldsObject.build(),
          rows: []
        };
      }
      
      // Extract the documents from the response
      documents = responseData.results.map(result => result.document);
      Logger.log('getData: Successfully retrieved %s documents', documents.length);
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
    
    // Process the documents into rows
    const rows = documents.map(document => {
      const values = [];
      
      requestedFieldsArray.forEach(field => {
        const fieldName = field.getId();
        const fieldType = field.getType();
        let value = getNestedValue(document, fieldName);
        
        // Process and format value based on field type
        let formattedValue = null;
        
        if (value === null || value === undefined) {
          formattedValue = '';
        } else {
          switch (fieldType) {
            case DataStudioApp.createCommunityConnector().FieldType.NUMBER:
              formattedValue = Number(value);
              if (isNaN(formattedValue)) {
                formattedValue = null;
              }
              break;
            case DataStudioApp.createCommunityConnector().FieldType.BOOLEAN:
              if (typeof value === 'string') {
                const lowerValue = value.toLowerCase();
                if (lowerValue === 'true') {
                  formattedValue = true;
                } else if (lowerValue === 'false') {
                  formattedValue = false;
                } else {
                  formattedValue = null;
                }
              } else {
                formattedValue = Boolean(value);
              }
              break;
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
      
      return { values };
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
  
  // Check if port is already present (handles IPv4 and IPv6)
  const hasPort = /:\d+$|]:\d+$/.test(hostAndPort);
  
  // For Data API, the default port is 18095
  if (!hasPort) {
    hostAndPort += ':18094';
    Logger.log('constructApiUrl: Added default port 18094 for URL: %s', hostAndPort);
  } else {
    Logger.log('constructApiUrl: Port already present in URL: %s', hostAndPort);
  }
  
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
