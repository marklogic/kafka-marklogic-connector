/**
 * This is an example of what a bulk endpoint could look like. It is not intended to be prescriptive or suggestive
 * of what an endpoint developer should do. Instead, it inserts data in a manner that facilitates the writing of
 * automated tests in this repository to ensure that a bulk endpoint receives the correct data from the connector.
 */
'use strict';

declareUpdate();

// Passed in via the bulk call
var input;
var endpointConstants;

// Normalize to a sequence
const inputSequence = input instanceof Document ? [input] : input;

endpointConstants = fn.head(xdmp.fromJSON(endpointConstants));

const permissions = [
  xdmp.permission('rest-reader', 'read'),
  xdmp.permission('rest-writer', 'update')
];

const collections = ["bulk-ds-test"];

// Write a document for each item in the sequence
for (let item of inputSequence) {
  // Each item in the input sequence is expected to be a JSON object
  item = fn.head(xdmp.fromJSON(item));
  const doc = {
    content: item['content'],
    kafkaMetadata: item['kafka-metadata'],
    endpointConstants
  };

  const uri = "/test/" + sem.uuidString() + ".json";
  xdmp.documentInsert(uri, doc, {permissions, collections});
}
