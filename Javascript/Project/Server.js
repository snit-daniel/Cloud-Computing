// Import the 'http' module to create an HTTP server
var http = require('http');
// Import the 'url' module to parse URL strings
var url = require('url');

// Function to parse a given time and return an object with hour, minute, and second properties
function parsetime(time) {
  return {
    hour: time.getHours(),
    minute: time.getMinutes(),
    second: time.getSeconds()
  };
}

// Function to convert a given time to UNIX epoch time and return an object with 'unixtime' property
function unixtime(time) {
  return { unixtime: time.getTime() };
}

// Function to retrieve the current time and return an object with year, month, date, hour, and minute properties
function currenttime() {
  var now = new Date();
  return {
    year: now.getFullYear(),
    month: now.getMonth() + 1, // Months are zero-indexed, so we add 1 to get the correct month
    date: now.getDate(),
    hour: now.getHours(),
    minute: now.getMinutes()
  };
}

// Create an HTTP server that listens for requests
var server = http.createServer(function (req, res) {
  // Parse the request URL to extract its components, including the path and query parameters
  var parsedUrl = url.parse(req.url, true);

  // Check the requested path and respond accordingly
  if (parsedUrl.pathname === '/api/parsetime') {
    // If the path is '/api/parsetime', parse the provided time and return the result as JSON
    var time = new Date(parsedUrl.query.iso);
    res.writeHead(200, { 'Content-Type': 'application/json' }); // Set the response header
    res.end(JSON.stringify(parsetime(time))); // Send the JSON response
  } else if (parsedUrl.pathname === '/api/unixtime') {
    // If the path is '/api/unixtime', convert the provided time to UNIX epoch time and return it as JSON
    var time = new Date(parsedUrl.query.iso);
    res.writeHead(200, { 'Content-Type': 'application/json' }); // Set the response header
    res.end(JSON.stringify(unixtime(time))); // Send the JSON response
  } else if (parsedUrl.pathname === '/api/currenttime') {
    // If the path is '/api/currenttime', retrieve the current time and return it as JSON
    var currentTime = currenttime();
    res.writeHead(200, { 'Content-Type': 'application/json' }); // Set the response header
    res.end(JSON.stringify(currentTime)); // Send the JSON response
  } else {
    // If the requested path doesn't match any of the defined endpoints, respond with a 404 status code
    res.writeHead(404);
    res.end();
  }
});

// Start the server and make it listen on port 8000
server.listen(8000);
