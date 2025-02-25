const axios = require('axios');
const fs = require('fs');
const path = require('path');

async function fetchRemoteJobs() {
  try {
    console.log('Fetching remote software development jobs...');
    
    const response = await axios.get('https://remotive.com/api/remote-jobs?category=software-dev');
    
    if (response.status === 200) {
      const data = response.data;
      console.log(`Successfully fetched ${data.jobs ? data.jobs.length : 0} jobs`);
      
      // Save data to a JSON file
      const filePath = path.join(__dirname, 'remote-jobs.json');
      fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
      
      console.log(`Data saved to ${filePath}`);
    } else {
      console.error(`Failed to fetch data: ${response.status}`);
    }
  } catch (error) {
    console.error('Error fetching remote jobs:', error.message);
  }
}

// Execute the function
fetchRemoteJobs();
