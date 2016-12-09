'use strict';

// Load modules

const Redis = require('redis');

// Declare internals

const internals = {
    connections: {}
};

/**
 * @param {String=} type
 * @param {Object=} config
 */
internals.connect = function (type, config)
{
    type = type || 'DEFAULT';
    config = config || {};

    if (!internals.hasOwnProperty('config'))
        internals.config = config;

    if (!internals.connections[type] || !internals.connections[type].connected)
        internals.connections[type] = internals.newConnection();

    return internals.connections[type];
};

/**
 * @returns {RedisClient}
 */
internals.newConnection = function ()
{
    const port = internals.config.port || 6379,
          host = internals.config.host || '127.0.0.1';

    return Redis.createClient(port, host);
};

/**
 * @param {String=} type
 */
internals.kill = function (type)
{
    type = type || 'DEFAULT';

    internals.connections[type].end();

    delete internals.connections[type];
};

module.exports = internals.connect;
module.exports.kill = internals.kill;
module.exports.killAll = function ()
{
    Object.keys(internals.connections)
        .forEach(internals.kill);
};