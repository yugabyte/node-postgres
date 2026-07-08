'use strict'

const winston = require('winston');

/*

Users can set the log level either by using an enviornment variable:

`LOG_LEVEL = debug`

or by using `setLogLevel` method:`

const { logger, setLogLevel } = require('./logger');

// Change log level dynamically
setLogLevel('debug');

// Log messages
logger.debug('This is a debug message');
logger.info('This is an info message');

*/

// Set the initial log level from the environment variable, default to 'info'
let logLevel = process.env.LOG_LEVEL || 'info';

// Configure the winston logger
var logger = winston.createLogger({
    level: logLevel,
    levels: winston.config.npm.levels,
    format: winston.format.combine(
        winston.format.colorize(),
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `${timestamp} [${level}]: ${message}`;
        })
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'app.log' })
    ]
});

// Function to set the log level dynamically and update the environment variable
var setLogLevel = (level) => {
    logLevel = level;
    process.env.LOG_LEVEL = level;  // Update the environment variable
    logger.level = level;  // Update the logger's level
    logger.info('Log level set to ' + level);
};

const REDACTED_PLACEHOLDER = '****';

/*
Key names whose values are secrets and must never be written to logs/disk.
Matching is done on a normalized key (lowercased, non-alphanumerics removed)
against these substrings, so casing/separators don't matter: "Password",
"PASSWORD", "db_password", "user-password" and "userPassword" all match.

The list is a superset of the node-postgres config secrets (`password`,
`connectionString` which is handled separately, and the `ssl` fields
`passphrase`/`pfx`/`key`) plus the common credential names used elsewhere
(mirroring what logging libraries like pino/fast-redact redact by default).
Password spellings/typos are covered explicitly rather than with a fuzzy
regex, to avoid redacting unrelated keys.
*/
const REDACTED_KEY_SUBSTRINGS = [
    // password family (incl. common misspellings)
    'password', 'passwrd', 'passwd', 'psswrd', 'passphrase', 'passcode', 'passkey', 'pwd',
    // generic credentials / secrets
    'secret', 'token', 'credential', 'apikey', 'accesskey', 'privatekey', 'authorization',
    'sessionid', 'cookie'
];

// TLS/SSL config objects (node-postgres passes `ssl` straight to tls.connect)
// carry private-key material under bare keys like `key`/`pfx`/`passphrase`.
// Those bare names are too generic to redact everywhere, so we only redact
// them when the surrounding object looks like a TLS config.
const TLS_SECRET_KEYS = new Set(['key', 'pfx', 'passphrase']);
const looksLikeTlsConfig = (holder) =>
    holder !== null && typeof holder === 'object' &&
    ('cert' in holder || 'ca' in holder || 'pfx' in holder ||
        'rejectUnauthorized' in holder || 'passphrase' in holder);

const isSecretKey = (key, holder) => {
    if (typeof key !== 'string' || key.length === 0) {
        return false;
    }
    const normalized = key.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (REDACTED_KEY_SUBSTRINGS.some((s) => normalized.includes(s))) {
        return true;
    }
    return TLS_SECRET_KEYS.has(normalized) && looksLikeTlsConfig(holder);
};

// Redact the password embedded in a connection string, e.g.
// postgresql://user:secret@host/db -> postgresql://user:****@host/db
const redactConnectionString = (str) =>
    str.replace(/(:\/\/[^:/?#@\s]+:)[^@/\s]+@/, '$1' + REDACTED_PLACEHOLDER + '@');

/*
Throw-safe JSON serializer for log messages. Unlike a bare JSON.stringify it:
  - never throws (circular refs and BigInt are handled, other errors are caught),
    so a log line can never break the code path it is instrumenting; and
  - redacts secret fields so they are not persisted to disk via the winston
    File transport. A `connectionString` value has only its embedded password
    stripped (the rest stays useful for debugging); every other secret field
    is fully masked.
*/
const safeStringify = (value, pretty = false) => {
    const seen = new WeakSet();
    try {
        // Uses a non-arrow function so `this` is the object holding `key`,
        // which lets us apply context-sensitive rules (e.g. TLS `key`).
        return JSON.stringify(value, function (key, val) {
            if (key === 'connectionString' && typeof val === 'string') {
                return redactConnectionString(val);
            }
            if (isSecretKey(key, this)) {
                return val == null ? val : REDACTED_PLACEHOLDER;
            }
            if (typeof val === 'bigint') {
                return val.toString();
            }
            if (val !== null && typeof val === 'object') {
                if (seen.has(val)) {
                    return '[Circular]';
                }
                seen.add(val);
            }
            return val;
        }, pretty ? 2 : undefined);
    } catch (err) {
        return '[unserializable: ' + err.message + ']';
    }
};

// Serialize a Map/Set (or any iterable) consistently and throw-safely.
// Maps are rendered as their [key, value] entries; Sets/arrays as their values.
const collectionToJSON = (collection) => {
    const entries = collection instanceof Map
        ? Array.from(collection.entries())
        : Array.from(collection);
    return safeStringify(entries);
};

/*
Log a message only when the given level is active, building it lazily via the
supplied callback. This avoids eagerly running (potentially expensive)
serialization on hot paths when the message would be discarded anyway.
*/
const logLazy = (level, buildMessage) => {
    if (logger.isLevelEnabled(level)) {
        logger.log(level, buildMessage());
    }
};

module.exports = {
    logger,
    setLogLevel,
    safeStringify,
    collectionToJSON,
    logLazy,
    redactConnectionString
};