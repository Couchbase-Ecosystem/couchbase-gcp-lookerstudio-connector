// demos/sampleQueries.js

// Load environment variables from .env file in the same directory
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '.env') });

// Example of how to run a N1QL query using the Couchbase Query Service REST API
// This uses the standard `fetch` API, similar to how connector.js works,
// but in a regular Node.js environment.

// --- Configuration ---
// Credentials and URL are now loaded from demos/.env
const API_BASE_URL = process.env.API_BASE_URL;
const CB_USERNAME = process.env.CB_USERNAME;
const CB_PASSWORD = process.env.CB_PASSWORD;

// Basic check to ensure variables are loaded
if (!API_BASE_URL || !CB_USERNAME || !CB_PASSWORD) {
    console.error('Error: Missing required environment variables from demos/.env file.');
    console.error('Please ensure demos/.env exists and contains API_BASE_URL, CB_USERNAME, and CB_PASSWORD.');
    process.exit(1); // Exit if configuration is missing
}

/**
 * Executes a N1QL query against the Couchbase Query Service REST API.
 *
 * @param {string} statement - The N1QL query statement.
 * @param {string} baseUrl - The base URL of the Couchbase Query Service (e.g., http://localhost:8093).
 * @param {string} username - Couchbase username for authentication.
 * @param {string} password - Couchbase password for authentication.
 * @returns {Promise<object>} - The parsed JSON result from the query service.
 * @throws {Error} - If the API request fails or returns an error status.
 */
async function executeN1qlQuery(statement, baseUrl, username, password) {
    // Parse the base URL to get hostname and port
    const url = new URL(baseUrl);
    const hostname = url.hostname;
    const port = url.port || (url.protocol === 'https:' ? 443 : 80); // Default port if not specified
    const path = '/query/service'; // The API path

    const queryPayload = {
        statement: statement,
    };
    const payloadString = JSON.stringify(queryPayload);

    const credentials = Buffer.from(`${username}:${password}`).toString('base64');
    const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${credentials}`,
        'Content-Length': Buffer.byteLength(payloadString) // Required for https.request
    };

    console.log(`Executing N1QL query via https.request against ${hostname}:${port}${path}:`);
    console.log(statement);

    const https = require('https');
    const agent = new https.Agent({
        rejectUnauthorized: false // Keep bypassing certificate check for testing
    });

    const options = {
        hostname: hostname,
        port: port,
        path: path,
        method: 'POST',
        headers: headers,
        agent: agent
    };

    // Use a Promise to handle the async nature of https.request
    return new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
            let responseData = '';

            // A chunk of data has been received.
            res.on('data', (chunk) => {
                responseData += chunk;
            });

            // The whole response has been received.
            res.on('end', () => {
                try {
                    if (res.statusCode < 200 || res.statusCode >= 300) {
                       // Treat non-2xx status codes as errors
                       reject(new Error(`HTTP error! status: ${res.statusCode} - ${responseData}`));
                    } else {
                        const result = JSON.parse(responseData);
                        if (result.status !== 'success') {
                            console.warn(`Query executed with status: ${result.status}`);
                            if (result.errors) {
                                console.error('Query Errors:', JSON.stringify(result.errors, null, 2));
                                // Optionally reject here if errors indicate failure
                                // reject(new Error(`Query failed with status ${result.status}: ${JSON.stringify(result.errors)}`));
                            }
                        }
                        resolve(result); // Resolve the promise with the parsed result
                    }
                } catch (parseError) {
                    console.error('Error parsing response JSON:', parseError);
                    reject(new Error(`Failed to parse response JSON: ${parseError.message}`));
                }
            });
        });

        // Handle request errors (e.g., network issues)
        req.on('error', (error) => {
            console.error('Error executing N1QL query via https.request:', error.message);
            reject(error); // Reject the promise on request error
        });

        // Write the payload and end the request
        req.write(payloadString);
        req.end();
    });
}

// --- Example Usage ---
async function runSampleQuery() {
    // Example query: Select first 10 airlines from the travel-sample bucket
    // Note: Bucket/scope/collection names with hyphens need backticks ``
    const sampleQuery = "SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;";

    // Check if placeholders are still set
     if (API_BASE_URL.includes('localhost') && (CB_USERNAME === 'your_username' || CB_PASSWORD === 'your_password')) {
        console.warn('\n!!! WARNING: API URL and/or credentials might be placeholders. Ensure they are set correctly. !!!\n');
        // Decide if you want to stop execution if placeholders are detected
        // return;
    }


    try {
        const queryResult = await executeN1qlQuery(sampleQuery, API_BASE_URL, CB_USERNAME, CB_PASSWORD);
        console.log('\n--- Query Result ---');
        console.log(JSON.stringify(queryResult, null, 2)); // Pretty print the JSON result

        if (queryResult.results && queryResult.results.length > 0) {
            console.log(`\nSuccessfully retrieved ${queryResult.results.length} airline documents.`);
        } else if (queryResult.status === 'success') {
            console.log('\nQuery successful, but returned no results.');
        }

    } catch (error) {
        console.error('\n--- Query Execution Failed ---');
        console.error(error);
        // Error already logged in executeN1qlQuery, could add more handling here if needed.
    }
}

// Run the example
runSampleQuery();
