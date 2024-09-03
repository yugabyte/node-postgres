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

module.exports = {
    logger,
    setLogLevel
};