'use strict';

/**
 * MarkLogic Server-Side Javascript client for usage with the Marklogic Jetty Source Connector.
 *
 * Usage:
 *
 * const KafkaClient = require('/kafka-client.sjs');
 * const client = new KafkaClient(host, port, isSsl, httpOptions);
 * client.post('topic', { foo: 'bar' });
 */

let KafkaClient = function(host = 'localhost', port = 9090, ssl = false, httpOptions = {}) {

    function getMimeType(payload) {
        let mimeType = 'application/json';

        if (Node.DOCUMENT_NODE === payload.nodeType) {
            payload = payload.root;
        }

        if ('string' === typeof payload) mimeType = 'text/plain';
        else if (Node.TEXT_NODE === payload.nodeType) mimeType = 'text/plain';
        else if (Node.BINARY_NODE === payload.nodeType) mimeType = 'application/octet-stream';
        else if (Node.OBJECT_NODE === payload.nodeType) mimeType = 'application/json';
        else if (payload.nodeType) mimeType = 'text/xml';

        return mimeType;
    }

    function getUrl(topic) {
        let url = `//${host}:${port}/topics/${topic}`;
        let protocol = ssl ? 'https' : 'http';
        return `${protocol}:${url}`;
    }

    function postToKafka(topic, payload) {
        let mimeType = getMimeType(payload);
        let options = Object.assign({
            headers: {
                'Content-Type': mimeType
            }
        }, httpOptions);

        const resp = xdmp.httpPost(getUrl(topic), options, payload);
        return resp;
    }

    return {
        post: postToKafka
    };
};

module.exports = KafkaClient;