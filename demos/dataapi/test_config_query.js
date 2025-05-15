const fetch = require('node-fetch');
const path = require('path'); // Import path module

// Determine path to .env file
const envPath = path.resolve(__dirname, '../../.env');
console.log(`Attempting to load .env file from: ${envPath}`);

const dotenvResult = require('dotenv').config({ path: envPath });

if (dotenvResult.error) {
  console.error('Error loading .env file:', dotenvResult.error);
} else {
  console.log('.env file loaded successfully. Parsed content (first few vars shown):', dotenvResult.parsed ? Object.keys(dotenvResult.parsed).slice(0,5).reduce((obj, key) => { obj[key] = dotenvResult.parsed[key]; return obj; }, {}) : 'No content parsed (or dotenv version doesn\'t return parsed)');
}

console.log('DATA_API_ENDPOINT from process.env:', process.env.DATA_API_ENDPOINT);
console.log('DATA_API_USERNAME from process.env:', process.env.DATA_API_USERNAME);
console.log('DATA_API_PASSWORD from process.env (should be defined, value hidden):', process.env.DATA_API_PASSWORD ? '********' : undefined);

const couchbaseEndpoint = process.env.DATA_API_ENDPOINT;
const username = process.env.DATA_API_USERNAME;
const password = process.env.DATA_API_PASSWORD;

const n1qlQuery = `
SELECT
  b.name AS \`bucket\`,
  s.name AS \`scope\`,
  k.name AS \`collection\`
FROM system:buckets AS b
JOIN system:all_scopes AS s ON s.\`bucket\` = b.name
JOIN system:keyspaces AS k ON k.\`bucket\` = b.name AND k.\`scope\` = s.name
ORDER BY b.name, s.name, k.name;
`;

async function executeConfigQuery() {
  if (!couchbaseEndpoint || !username || !password) {
    console.error('Error: DATA_API_ENDPOINT, DATA_API_USERNAME, and DATA_API_PASSWORD must be set in .env file');
    return;
  }

  const apiUrl = `https://${couchbaseEndpoint}/_p/query/query/service`;
  console.log(`Executing N1QL query against: ${apiUrl}`);
  console.log(`Query:\n${n1qlQuery}`);

  try {
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + Buffer.from(username + ':' + password).toString('base64'),
        // Add any other necessary headers, e.g., for Capella:
        // 'Couchbase-Remote-Party': 'query-ui-rs-a.europe-west1.cloud.couchbase.com',
        // 'Query-Context': 'default:default' // Or the appropriate bucket.scope if needed for system queries
      },
      body: JSON.stringify({ statement: n1qlQuery }),
    });

    const responseBody = await response.text(); // Read as text first to handle non-JSON errors
    console.log('\nResponse Status:', response.status);

    if (!response.ok) {
      console.error('Error Response Body:', responseBody);
      return;
    }

    try {
      const jsonData = JSON.parse(responseBody);
      console.log('\nQuery Results:');
      console.log(JSON.stringify(jsonData, null, 2));
      if (jsonData.results && jsonData.results.length > 0) {
        console.log('\nSuccessfully fetched keyspace information.');
      } else {
        console.log('\nQuery executed successfully, but no keyspaces found or system catalogs are empty/inaccessible with current permissions.');
      }
    } catch (jsonError) {
      console.error('Error parsing JSON response:', jsonError);
      console.error('Raw Response Body (that caused JSON parse error):', responseBody);
    }

  } catch (error) {
    console.error('Error executing N1QL query:', error);
  }
}

executeConfigQuery(); 