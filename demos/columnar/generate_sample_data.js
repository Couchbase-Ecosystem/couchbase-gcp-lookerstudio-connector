const fs = require('fs');
const path = require('path');
const { submitRequest } = require('./columnarClient'); // Import the query function

// --- Configuration ---
const sampleLimit = 4500; // Number of samples to fetch per collection
const outputDir = path.join(__dirname, 'sample_output'); // Output directory relative to this script

// --- Queries ---
const landmarkQuery = `SELECT * FROM \`travel-sample\`.inventory.landmark LIMIT ${sampleLimit};`;
const hotelQuery = `SELECT * FROM \`travel-sample\`.inventory.hotel LIMIT ${sampleLimit};`;

// --- File Writing Helper ---
async function writeSamplesToFile(filename, results) {
    const filePath = path.join(outputDir, filename);

    try {
        // Ensure output directory exists
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
            console.log(`Created output directory: ${outputDir}`);
        }

        // Format data: one JSON string per line
        // Ensure each result is stringified individually
        const fileContent = results.map(doc => JSON.stringify(doc)).join('\n'); 

        await fs.promises.writeFile(filePath, fileContent);
        console.log(`Successfully wrote ${results.length} samples to ${filePath}`);
    } catch (error) {
        console.error(`Error writing to file ${filePath}:`, error);
        throw error; // Re-throw to stop execution if writing fails
    }
}

// --- Main Function ---
async function generateSamples() {
    console.log('Starting sample data generation...');
    console.log(`Fetching up to ${sampleLimit} samples each for landmark and hotel.`);
    console.log('Ensure CB_COLUMNAR_URL, CB_USERNAME, and CB_PASSWORD environment variables are set correctly (or defined in .env).'); // Updated message
    
    try {
        // --- Fetch Landmarks ---
        console.log('\nFetching landmark samples...');
        const landmarkResponse = await submitRequest(landmarkQuery);

        if (!landmarkResponse || !landmarkResponse.results) {
            console.error('Failed to fetch landmark data or received unexpected format:', landmarkResponse);
            return; // Stop if data is missing
        }
        await writeSamplesToFile('landmark_samples.txt', landmarkResponse.results);

        // --- Fetch Hotels ---
        console.log('\nFetching hotel samples...');
        const hotelResponse = await submitRequest(hotelQuery);

        if (!hotelResponse || !hotelResponse.results) {
            console.error('Failed to fetch hotel data or received unexpected format:', hotelResponse);
            return; // Stop if data is missing
        }
        await writeSamplesToFile('hotel_samples.txt', hotelResponse.results);

        console.log('\nSample files generated successfully in:', outputDir);

    } catch (error) {
        console.error('\nError during sample data generation:', error.message || error);
        // Log original error if available from columnarClient
        if (error.originalError) {
             console.error('Original Error:', error.originalError.message || error.originalError);
        }
        process.exit(1);
    }
}

// --- Run ---
generateSamples(); 