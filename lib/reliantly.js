'use strict';

const _ = require('lodash');
const Joi = require('joi');
const Redis = require('./connection');
const Schema = require('./schema');
const Uuid = require('uuid');

// Declare internals

const internals = {
    brpoplpushing: {},
    instanceKey: + new Date()
};

/**
 * @param server
 * @param options
 * @param next
 */
module.exports.register = function (server, options, next)
{
    Joi.assert(options, Schema.options, 'Bad plugin options passed to reliantly');

    internals.queue = Redis('queue', options.connection);
    internals.crud = Redis('crud', options.connection);
    internals.identity = options.identity;

    if (options.hasOwnProperty('subscribeTo') && options.hasOwnProperty('consumerCallback'))
        internals.subscribe(options.subscribeTo, options.consumerCallback);

    server.decorate('reply', 'publish', internals.publish);

    next();
};

/**
 * @param action
 * @param eventPayload
 */
internals.publish = function (action, eventPayload)
{
    const producer = internals.identity;

    internals.crud.smembers(producer + '.' + action + '.consumers', (err, consumers) =>
    {
        if (err)
            throw err;

        const key = action + ':' + Uuid(),
              count = consumers.length;

        if (count.length === 0)
        {
            console.log('Warning! No consumers registered for \'' + producer + '\'');
            return;
        }
        else
            internals.crud.set(producer + '.message.' + key, eventPayload);

        _.forEach(consumers, consumer =>
        {
            // don't publish to yourself
            if (consumer === producer)
                return;

            internals.crud.set(producer + '.reads.' + key, count);
            internals.crud.lpush(producer + '.' + consumer + '.message', key);
        });
    });
};

/**
 * @param subscribeTo
 * @param consumerCallback
 */
internals.subscribe = function (subscribeTo, consumerCallback)
{
    internals.consumerCallback = consumerCallback;

    _.forEach(subscribeTo, (events, producer) =>
    {
        if (!_.isArray(events))
            return;

        _.forEach(events, event =>
        {
            internals.crud.sadd(producer + '.' + event + '.consumers', internals.identity);
        });

        internals.listen(producer);
    });
};

/**
 * @param producer
 */
internals.listen = function (producer)
{
    if (internals.brpoplpushing[producer])
        return;

    internals._brpoplpush(producer);

    internals.brpoplpushing[producer] = true;
};

/**
 * @param producer
 * @private
 */
internals._brpoplpush = function (producer)
{
    internals.queue.brpoplpush(producer + '.' + internals.identity + '.message',
        internals.identity + '.processing.' + internals.instanceKey, 0, err =>
    {
        if (err)
            throw err;

        internals._lrange(producer);
    });
};

/**
 * @param producer
 * @private
 */
internals._lrange = function (producer)
{
    internals.crud.lrange(internals.identity + '.processing.' + internals.instanceKey, -1, -1, (err, messageKey) =>
    {
        if (err)
            throw err;

        if (!_.isArray(messageKey) || messageKey.length === 0)
            return;

        internals._get(producer, messageKey[0]);
    });
};

/**
 * @param producer
 * @param key
 * @private
 */
internals._get = function (producer, key)
{
    internals.crud.get(producer + '.message.' + key, (err, message) =>
    {
        if (err)
            throw err;

        const action = key.split(':')[0];

        internals.consumerCallback(producer + '.' + action, message, internals._ack(producer, key, internals.instanceKey));
    });
};

/**
 * @param producer
 * @param messageKey
 * @param instanceKey
 * @returns {function()}
 * @private
 */
internals._ack = function (producer, messageKey, instanceKey)
{
    return () =>
    {
        internals.crud.rpop(internals.identity + '.processing.' + instanceKey, err =>
        {
            if (err)
                throw err;

            internals.crud.decr(producer + '.reads.' + messageKey, (err, readsRemaining) =>
            {
                if (err)
                    throw err;

                if (readsRemaining === 0)
                {
                    internals.crud.del(producer + '.reads.' + messageKey);
                    internals.crud.del(producer + '.message.' + messageKey);
                }
            });

            internals._brpoplpush(producer);
        });
    }
};

module.exports.register.attributes = {
    pkg: require('../package.json')
};
