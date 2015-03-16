importScripts('lunr.js');

var index;

// The worker receives a message everytime the web app wants to query the index
onmessage = function(e) {
  switch(e.data.e) {
    case 'load':
      var searchIndexRequest = new XMLHttpRequest();
      searchIndexRequest.onload = function() {

        // Store the pages data to be used in mapping query results back to pages
        index = lunr.Index.load(JSON.parse(this.responseText));

        postMessage({ e: 'index-ready' });
      };
      searchIndexRequest.open('GET', e.data.p + '/json/search-index.json')
      searchIndexRequest.send();
      break;
    case 'search':
      var q = e.data.q;
      var hits = index.search(q);
      var results = [];
      // Only return the array of paths to pages
      hits.forEach(function(hit) {
        results.push(hit.ref);
      });
      // The results of the query are sent back to the web app via a new message
      postMessage({ e: 'query-ready', q: q, d: results });
      break;
  }
};

postMessage({ e: 'ready' });