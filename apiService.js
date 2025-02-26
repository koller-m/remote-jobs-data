// Load environment variables from .env file
require('dotenv').config();

const axios = require('axios');
const {BigQuery} = require('@google-cloud/bigquery');
const fs = require('fs');
const path = require('path');

async function fetchRemoteJobs() {
  try {
    console.log('Fetching remote software development jobs...');
    
    const response = await axios.get('https://remotive.com/api/remote-jobs?category=software-dev');
    
    if (response.status === 200) {
      const data = response.data;
      const jobs = data.jobs || [];
      console.log(`Successfully fetched ${jobs.length} jobs`);
      
      // Save jobs to a local JSON file
      const tempFile = path.join(__dirname, 'temp_jobs.json');
      await saveJobsToFile(jobs, tempFile);
      
      // Insert data into BigQuery
      const success = await loadIntoBigQuery(tempFile);
      
      // Clean up temp file
      fs.unlinkSync(tempFile);
      
      if (success) {
        console.log(`Data loaded into BigQuery successfully`);
      } else {
        console.log(`Failed to load data into BigQuery`);
      }
    } else {
      console.error(`Failed to fetch data: ${response.status}`);
    }
  } catch (error) {
    console.error('Error fetching remote jobs:', error.message);
  }
}

async function saveJobsToFile(jobs, filePath) {
  // Transform jobs data to match BigQuery schema
  const rows = jobs.map(job => ({
    id: job.id,
    url: job.url,
    title: job.title,
    company_name: job.company_name,
    category: job.category,
    tags: job.tags,
    job_type: job.job_type,
    publication_date: job.publication_date,
    candidate_required_location: job.candidate_required_location,
    salary: job.salary,
    description: job.description
  }));
  
  // Write to file
  fs.writeFileSync(filePath, JSON.stringify(rows, null, 2));
  console.log(`Saved ${rows.length} jobs to ${filePath}`);
}

async function loadIntoBigQuery(filePath) {
  // Initialize BigQuery client
  const bigquery = new BigQuery();
  
  const datasetId = 'remote_jobs_dataset';
  const tableId = 'jobs';
  
  try {
    // Check if dataset exists, create if it doesn't
    const [datasets] = await bigquery.getDatasets();
    const datasetExists = datasets.some(dataset => dataset.id === datasetId);
    
    let dataset;
    if (!datasetExists) {
      console.log(`Dataset ${datasetId} does not exist. Creating it now...`);
      [dataset] = await bigquery.createDataset(datasetId);
      console.log(`Dataset ${datasetId} created.`);
    } else {
      dataset = bigquery.dataset(datasetId);
      console.log(`Dataset ${datasetId} already exists.`);
    }
    
    // Check if table exists, create if it doesn't
    const [tables] = await dataset.getTables();
    const tableExists = tables.some(table => table.id === tableId);
    
    let table;
    if (!tableExists) {
      console.log(`Table ${tableId} does not exist. Creating it now...`);
      
      // Define schema for the table
      const schema = [
        {name: 'id', type: 'INTEGER'},
        {name: 'url', type: 'STRING'},
        {name: 'title', type: 'STRING'},
        {name: 'company_name', type: 'STRING'},
        {name: 'category', type: 'STRING'},
        {name: 'tags', type: 'STRING', mode: 'REPEATED'},
        {name: 'job_type', type: 'STRING'},
        {name: 'publication_date', type: 'TIMESTAMP'},
        {name: 'candidate_required_location', type: 'STRING'},
        {name: 'salary', type: 'STRING'},
        {name: 'description', type: 'STRING'}
      ];
      
      const options = {
        schema: schema,
        location: 'US',  // Specify your preferred location
      };
      
      [table] = await dataset.createTable(tableId, options);
      console.log(`Table ${tableId} created.`);
    } else {
      table = dataset.table(tableId);
      console.log(`Table ${tableId} already exists.`);
    }
    
    // Load data from file
    const metadata = {
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
      schema: {
        fields: [
          {name: 'id', type: 'INTEGER'},
          {name: 'url', type: 'STRING'},
          {name: 'title', type: 'STRING'},
          {name: 'company_name', type: 'STRING'},
          {name: 'category', type: 'STRING'},
          {name: 'tags', type: 'STRING', mode: 'REPEATED'},
          {name: 'job_type', type: 'STRING'},
          {name: 'publication_date', type: 'TIMESTAMP'},
          {name: 'candidate_required_location', type: 'STRING'},
          {name: 'salary', type: 'STRING'},
          {name: 'description', type: 'STRING'}
        ]
      },
      writeDisposition: 'WRITE_TRUNCATE', // This will overwrite the table if it exists
    };
    
    // Convert JSON to NDJSON (Newline Delimited JSON)
    const ndjsonFile = filePath.replace('.json', '.ndjson');
    const jsonData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const ndjson = jsonData.map(item => JSON.stringify(item)).join('\n');
    fs.writeFileSync(ndjsonFile, ndjson);
    
    // Load data
    console.log(`Loading data into BigQuery...`);
    const [job] = await table.load(ndjsonFile, metadata);
    
    // Check the job's status - fixed to use the correct API
    if (job && job.status && job.status.errors && job.status.errors.length > 0) {
      throw job.status.errors;
    }
    
    console.log(`BigQuery load job completed successfully.`);
    
    // Clean up NDJSON file
    fs.unlinkSync(ndjsonFile);
    
    return true; // Indicate success
  } catch (error) {
    console.error('ERROR:', error);
    return false; // Indicate failure
  }
}

// Execute the function
fetchRemoteJobs();
