// A scripted gRPC server for testing BigQuery.jl's Storage Read API client over
// real HTTP/2 (cleartext, "prior knowledge"). It uses only Node's standard
// library and knows nothing about protobuf: responses are byte files prepared
// by the Julia tests, and requests are logged for the tests to inspect.
//
//   node grpc_mock_server.js <dir>
//
// <dir>/plan.json:  { "generation": 1, "calls": { "<key>": [ step, ... ] } }
//   key  = "CreateReadSession"  or  "ReadRows:<stream name>"
//   step = { "file": "x.bin" | null, "status": 0, "message": "", "chunk": 1000 }
// The n-th call for a key replays step n (the last step repeats).
// Every request is appended to <dir>/requests.log as one JSON object per line.

const http2 = require('http2');
const fs = require('fs');
const path = require('path');

const dir = process.argv[2];
const counters = new Map();

const server = http2.createServer();

server.on('stream', (stream, headers) => {
  const chunks = [];
  stream.on('data', (c) => chunks.push(c));
  stream.on('error', () => {});
  stream.on('end', () => {
    const body = Buffer.concat(chunks);
    const method = headers[':path'].split('/').pop();
    const routing = headers['x-goog-request-params'] || '';
    let key = method;
    if (method === 'ReadRows') {
      key = 'ReadRows:' + decodeURIComponent(routing.replace(/^read_stream=/, ''));
    }
    fs.appendFileSync(path.join(dir, 'requests.log'), JSON.stringify({
      method,
      path: headers[':path'],
      contentType: headers['content-type'],
      te: headers['te'],
      authorization: headers['authorization'],
      routing,
      flag: body.length > 0 ? body[0] : null,
      declaredLength: body.length >= 5 ? body.readUInt32BE(1) : null,
      message: body.slice(5).toString('base64'),
    }) + '\n');

    const plan = JSON.parse(fs.readFileSync(path.join(dir, 'plan.json'), 'utf8'));
    const steps = plan.calls[key];
    if (!steps) {
      stream.respond({ ':status': 200, 'content-type': 'application/grpc',
                       'grpc-status': '5', 'grpc-message': encodeURIComponent('no plan for ' + key) },
                     { endStream: true });
      return;
    }
    const counterKey = plan.generation + '|' + key;
    const n = counters.get(counterKey) || 0;
    counters.set(counterKey, n + 1);
    const step = steps[Math.min(n, steps.length - 1)];

    stream.respond({ ':status': 200, 'content-type': 'application/grpc' }, { waitForTrailers: true });
    stream.on('wantTrailers', () => {
      stream.sendTrailers({ 'grpc-status': String(step.status || 0),
                            'grpc-message': encodeURIComponent(step.message || '') });
    });
    const data = step.file ? fs.readFileSync(path.join(dir, step.file)) : Buffer.alloc(0);
    const size = step.chunk || 4096;
    let offset = 0;
    const pump = () => {
      // Small writes with pauses in between, so messages arrive split at arbitrary points.
      if (offset >= data.length) { stream.end(); return; }
      stream.write(data.subarray(offset, offset + size));
      offset += size;
      setImmediate(pump);
    };
    pump();
  });
});

server.listen(0, '127.0.0.1', () => {
  console.log('PORT ' + server.address().port);
});
