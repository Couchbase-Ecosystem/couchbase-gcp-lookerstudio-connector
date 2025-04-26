// Import the client functions
const {
    submitRequest,
    getActiveRequests,
    cancelRequest,
    getCompletedRequests,
    // Import link functions if needed for testing
    // getLink,
    // createLink,
    // updateLink,
    // deleteLink
} = require('./columnarClient');

// No need for config, httpsAgent, or api instance here anymore
// They are managed within columnarClient.js

// Example usage (Test Script)
async function runDemo() {
  try {
    // The client initialization (including credential/URL checks)
    // now happens when columnarClient.js is required.
    // No need to repeat the checks here.

    // Example 1: Submit a simple query
    console.log('\n--- Test: Submit a simple query ---');
    const queryResult = await submitRequest('SELECT 1 AS test');
    // Optional: Add assertions here in a real test framework
    // expect(queryResult).toHaveProperty('requestID');

    // Example 2: Check active requests
    console.log('\n--- Test: Check active requests ---');
    await getActiveRequests();
    // Optional: Assertions

    // Example 3: Get completed requests
    console.log('\n--- Test: Get completed requests ---');
    await getCompletedRequests();
    // Optional: Assertions

    // Example 4: Submit a more complex query
    console.log('\n--- Test: Submit a complex query ---');
    await submitRequest('SELECT * FROM Metadata.`Database`');
    // Optional: Assertions

    // Example 5: Submit and then cancel a request using clientContextId
    console.log('\n--- Test: Submit and Cancel Request --- ');
    const clientContextId = `test-cancel-${Date.now()}`;
    let requestIdToCancel = null;
    try {
        const submitResult = await submitRequest('SELECT "long running query simulation" AS status', clientContextId);
        requestIdToCancel = submitResult?.requestID; // Get request ID if possible
        console.log(`Submitted request for cancellation test with clientContextId: ${clientContextId}, requestId: ${requestIdToCancel}`);

        // Add a small delay if needed for the request to potentially become active
        await new Promise(resolve => setTimeout(resolve, 500));

        if (requestIdToCancel) {
             console.log(`Attempting to cancel using Request ID: ${requestIdToCancel}`);
             await cancelRequest(requestIdToCancel);
        } else {
            // Fallback to clientContextId if requestID wasn't returned or found quickly
            console.warn("Could not get requestID quickly, attempting cancellation using clientContextId.");
            await cancelRequest(clientContextId);
        }
    } catch (error) {
        console.error('Error during submit/cancel test:', error.message);
        // Don't stop the whole test run if cancellation fails
    }

    // --- Add Link Management Tests Here (Optional) ---
    // const linkTest = require('./linkManagementTests'); // Example: Move tests to another file
    // await linkTest.runLinkTests();


  } catch (error) {
    console.error('Test Demo execution failed:', error.message);
    // In a real test runner, this would fail the test suite
    process.exitCode = 1; // Indicate failure
  }
}

// Run the demo test script
console.log('Starting Columnar Client Test Script...');
runDemo().then(() => {
  console.log('\nTest Script completed!');
}).catch(error => {
  console.error('Error running test script:', error.message);
  process.exitCode = 1; // Indicate failure
});
