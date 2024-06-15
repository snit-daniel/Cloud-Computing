// Import the required modules: http for creating the server and url for parsing the request URLs
const http = require('http');
const url = require('url');

// Define an array of student objects with id, name, and score properties
const students = [
  { id: 11111, name: 'Bruce Lee', score: 84 },
  { id: 22222, name: 'Jackie Chan', score: 93 }, // Corrected 'Jackie Chen' to 'Jackie Chan'
  { id: 33333, name: 'Jet Li', score: 88 },
];

// Create the HTTP server
const server = http.createServer((req, res) => {
  // Parse the incoming request URL and extract the query parameters and pathname
  const parsedUrl = url.parse(req.url, true);
  const query = parsedUrl.query;
  const pathname = parsedUrl.pathname;

  // Check if the request is for the '/api/score' endpoint
  if (pathname === '/api/score') {
    // Extract the 'student_id' parameter from the query string and convert it to an integer
    const studentId = parseInt(query.student_id, 10);

    // Find the student object in the 'students' array that matches the given student ID
    const student = students.find(s => s.id === studentId);

    // If a student is found, send a 200 response with the student data in JSON format
    if (student) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(student));
    } else {
      // If no student is found, send a 404 response with an error message
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Student not found' }));
    }
  } else {
    // If the request is not for the '/api/score' endpoint, send a 404 response with an error message
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
  }
});

// Define the port number where the server will listen for requests
const PORT = 8000;

// Start the server and listen on the defined port
server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
