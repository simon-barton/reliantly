'use strict';

// Load modules

const Joi = require('joi');

module.exports.options = Joi.object().keys(
    {
        connection: Joi.object().keys(
            {
                host: Joi.string(),
                port: Joi.number()
            }
        ),
        consumerCallback: Joi.func().arity(3),
        identity: Joi.string().required(),
        subscribeTo: Joi.object().pattern(
            /[\w\d]+/,
            Joi.array().unique().items(Joi.string().required())
        )
    }
);
