'use strict';

const _ = require('lodash');
const Joi = require('joi');
const Promise = require('bluebird');
const Redis = require('./connection');
const Schema = require('./schema');
const Uuid = require('uuid');

// Declare internals

const internals = {
    brpoplpushing: {}
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

    // decorate the reply callback with the publish function
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
            console.log('Warning, no consumers registered for \'' + producer + '\'');
            return;
        }
        else
            internals.crud.set(producer + '.message.' + key, eventPayload, 'EX', 60 * 60 * 24);

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

    internals.recover(producer)
        .then(() =>
        {
            internals._brpoplpush(producer);
            internals.brpoplpushing[producer] = true;
        }, err => { throw err; });
};

/**
 * @param producer
 * @returns {Promise}
 */
internals.recover = function (producer)
{
    return new Promise((resolve, reject) =>
    {
        internals.crud.lrange(internals.identity + '.dequeued', 0, -1, (err, keys) =>
        {
            if (err)
                return reject(err);

            _.forEach(_.reverse(keys), key =>
            {
                internals._get(producer, key);
            });

            resolve();
        });
    });
};

/**
 * @param producer
 * @private
 */
internals._brpoplpush = function (producer)
{
    internals.queue.brpoplpush(producer + '.' + internals.identity + '.message',
        internals.identity + '.dequeued', 0, (err, messageKey) =>
        {
            if (err)
                throw err;

            internals._get(producer, messageKey);
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

        internals.consumerCallback(producer + '.' + action, message, internals._ack(producer, key));

        // don't call _brpoplpush if we're currently recovering
        if (internals.brpoplpushing[producer])
            internals._brpoplpush(producer);
    });
};

/**
 * Callback for the consumer when an event has been handled
 *
 * @param producer
 * @param messageKey
 * @returns {function()}
 * @private
 */
internals._ack = function (producer, messageKey)
{
    return () =>
    {
        internals.crud.lrem(internals.identity + '.dequeued', 0, messageKey, (err, removedCount) =>
        {
            if (err)
                throw err;

            if (removedCount === 0)
                return;

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
        });
    }
};

module.exports.register.attributes = {
    pkg: require('../package.json')
};
