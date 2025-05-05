const dotenv = require('dotenv');
const columnarClient = require('./columnarClient');

// Load variables from .env file
dotenv.config();

// Configuration
const DATABASE_NAME = 'travel-sample';
const SCOPE_NAME = 'inventory';
// Standard collections to test
const STANDARD_COLLECTIONS = ['airline', 'airport', 'hotel', 'landmark', 'route'];
// Temporary TAV name
const TEMP_TAV_NAME = 'temp_airline_tav';
const TEMP_TAV_FULL_PATH = `\`${DATABASE_NAME}\`.\`${SCOPE_NAME}\`.\`${TEMP_TAV_NAME}\``;
const TEMP_TAV_SOURCE_PATH = `\`${DATABASE_NAME}\`.\`${SCOPE_NAME}\`.airline`;

/**
 * Creates a temporary Tabular View for testing.
 */
async function createTemporaryTav() {
  console.log(`\n--- Creating Temporary TAV: ${TEMP_TAV_FULL_PATH} ---`);
  // Simple TAV based on airline collection
  const createViewQuery = `
    CREATE OR REPLACE VIEW ${TEMP_TAV_FULL_PATH} (
      iata STRING,
      name STRING,
      country STRING
      // Add other relevant fields if needed, ensure types match TAV requirements
    )
    DEFAULT NULL
    AS
      SELECT air.iata, air.name, air.country
      FROM ${TEMP_TAV_SOURCE_PATH} AS air;
  `;
  console.log(`Executing: ${createViewQuery.replace(/\n\s*/g, ' ')}`);
  try {
    await columnarClient.submitRequest(createViewQuery);
    console.log(`✅ Successfully created/replaced TAV: ${TEMP_TAV_NAME}`);
  } catch (error) {
    console.error(`❌ Error creating TAV ${TEMP_TAV_NAME}:`, error.message);
     if (error.response?.data?.errors) {
        console.error("  -> Server Errors:", JSON.stringify(error.response.data.errors, null, 2));
    }
    // Don't necessarily stop the whole test if creation fails, maybe it already exists in a bad state
  }
}

/**
 * Drops the temporary Tabular View.
 */
async function dropTemporaryTav() {
  console.log(`\n--- Dropping Temporary TAV: ${TEMP_TAV_FULL_PATH} ---`);
  const dropViewQuery = `DROP VIEW ${TEMP_TAV_FULL_PATH} IF EXISTS;`;
   console.log(`Executing: ${dropViewQuery}`);
  try {
    await columnarClient.submitRequest(dropViewQuery);
    console.log(`✅ Successfully dropped TAV: ${TEMP_TAV_NAME}`);
  } catch (error) {
    console.error(`❌ Error dropping TAV ${TEMP_TAV_NAME}:`, error.message);
     if (error.response?.data?.errors) {
        console.error("  -> Server Errors:", JSON.stringify(error.response.data.errors, null, 2));
    }
  }
}


/**
 * Fetches and logs the schema for a given collection/view from System Metadata.
 * @param {string} databaseName
 * @param {string} scopeName
 * @param {string} objectName Collection or View name
 */
async function getObjectSchemaFromMetadata(databaseName, scopeName, objectName) {
  const fullPath = `\`${databaseName}\`.\`${scopeName}\`.\`${objectName}\``;
  console.log(`\n--- Getting Schema from Metadata for: ${fullPath} ---`);

  // Revised query based on closer look at JDBC driver
  const schemaQuery = `
    SELECT field.FieldName AS COLUMN_NAME,
           field.FieldType AS TYPE_NAME,
           CASE WHEN field.IsNullable OR field.IsMissable THEN 1 ELSE 0 END AS NULLABLE,
           fieldpos AS ORDINAL_POSITION
    FROM Metadata.\`Dataset\` ds
    JOIN Metadata.\`Datatype\` dt ON ds.DatatypeDataverseName = dt.DataverseName AND ds.DatatypeName = dt.DatatypeName
    UNNEST dt.Derived.Record.Fields AS field AT fieldpos
    WHERE ds.DatabaseName = $dbName
      AND ds.DataverseName = $scopeName
      AND ds.DatasetName = $objectName
      AND ARRAY_LENGTH(dt.Derived.Record.Fields) > 0
    ORDER BY ORDINAL_POSITION;
  `;

  const params = {
    dbName: databaseName,
    scopeName: scopeName,
    objectName: objectName
  };

  console.log(`Executing Metadata Query: ${schemaQuery.replace(/\n\s*/g, ' ')}`);
  console.log(`With Params: ${JSON.stringify(params)}`);

  try {
    const response = await columnarClient.submitRequest({ statement: schemaQuery, args: params });

    if (response && response.results && response.results.length > 0) {
      console.log(`Defined Schema for ${objectName} (from Metadata):`);
      const fields = response.results;

      fields.forEach((field, index) => {
        // Use the field names directly from the revised query result
        console.log(`  [${index + 1}] ${field.COLUMN_NAME}: ${field.TYPE_NAME} (Nullable: ${field.NULLABLE === 1 ? true : false}) (Pos: ${field.ORDINAL_POSITION})`);
      });

    } else {
      // This is expected for standard collections
      if (STANDARD_COLLECTIONS.includes(objectName)) {
          console.log(`  -> OK: No schema fields found in Metadata for standard collection ${fullPath}. (Expected)`);
      } else {
          console.log(`  -> WARNING: No schema fields found in Metadata for ${fullPath}. Is it a correctly defined TAV or does metadata need refreshing?`); // Updated warning
      }
    }
  } catch (error) {
    console.error(`  -> Error getting schema from Metadata for ${fullPath}:`, error.message);
     if (error.response?.data?.errors) {
        console.error("  -> Server Errors:", JSON.stringify(error.response.data.errors, null, 2));
    } else if (error.response?.data) {
        console.error("  -> Server Response Data:", JSON.stringify(error.response.data, null, 2));
    }
  }
}

/**
 * Main function to run the schema metadata tests, including a temporary TAV.
 */
async function runSchemaMetadataTestWithTav() {
  console.log('Starting Schema Metadata Test (with Temporary TAV)...');
  let tavCreated = false;

  try {
    // Test basic connectivity
    console.log(`\nConnecting to Couchbase at: ${columnarClient.getBaseUrl()}`);
    const testResponse = await columnarClient.submitRequest('SELECT 1+1 AS sum');
    if (!testResponse) {
      console.error('Basic connectivity test failed. Cannot continue.');
      return;
    }
    console.log('Basic connectivity test passed.');

    // Create the temporary TAV
    await createTemporaryTav();
    tavCreated = true; // Assume creation succeeded for cleanup purposes, drop is idempotent

    // List of objects to test (collections + TAV)
    const objectsToTest = [...STANDARD_COLLECTIONS, TEMP_TAV_NAME];

    // Get schema for each object from Metadata
    for (const objectName of objectsToTest) {
      await getObjectSchemaFromMetadata(DATABASE_NAME, SCOPE_NAME, objectName);
    }

    console.log('\nSchema Metadata Test Complete!');

  } catch (error) {
     console.error('\n--- Top Level Error ---');
     console.error('An error occurred during the test run:', error.message);
     if (error.stack) {
       console.error("Stack Trace:", error.stack);
     }
     if (error.response?.data) {
       console.error("Response Data:", JSON.stringify(error.response.data, null, 2));
     }
  } finally {
      // Ensure TAV is dropped even if errors occurred
      if (tavCreated) { // Only attempt drop if we tried to create it
        await dropTemporaryTav();
      }
  }
}

// Run the test if this file is executed directly
if (require.main === module) {
  runSchemaMetadataTestWithTav(); // Changed function call here
}

module.exports = {
  getObjectSchemaFromMetadata,
  createTemporaryTav,
  dropTemporaryTav,
  runSchemaMetadataTestWithTav
}; 