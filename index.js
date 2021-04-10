const request = (process.type === 'renderer') ? require('ut-browser-request') : require('request');
const Hapi = require('@hapi/hapi');
const errors = require('./errors');
const bourne = require('bourne');
const querystring = require('querystring');
const Boom = require('@hapi/boom');
const uuid = require('uuid').v4;

module.exports = ({utPort, registerErrors, utMethod}) => class WebhookPort extends utPort {
    get defaults() {
        return {
            type: 'webhook',
            // set to async to false to wait for all http requests to be executed before returning server response
            // set to async to 'client' to only wait for server handler to be executed before returning server response
            // set to async to 'server' to not wait for server handler and http requests before returning server response
            async: 'server',
            bail: false, // set to true to bail out on the first request failure
            mode: 'reduce', // requests execution mode. Available modes: map, reduce, reply
            minLatencyRequest: 100,
            disconnectOnError: false,
            request: {
                method: 'POST',
                json: true,
                jar: true,
                qs: {},
                headers: {},
                url: '',
                requestTimeout: 5000
            },
            response: {
                body: 'EVENT_RECEIVED',
                code: 200
            },
            server: {
                port: 8080
            }
        };
    }

    getUriFromMeta({headers, serverInfo, requestInfo}) {
        let uri = '';
        // protocol
        if (headers['x-forwarded-proto']) { // in case of proxy / load balancer
            uri += headers['x-forwarded-proto'].split(',')[0];
        } else {
            uri += serverInfo.protocol;
        }
        uri += '://';
        // host
        if (headers['x-forwarded-host']) { // in case of proxy / load balancer
            uri += headers['x-forwarded-host'].split(',')[0];
        } else {
            uri += requestInfo.host;
        }
        return uri;
    }

    sendRequest(params = {}) {
        if (params === false) {
            return;
        }
        // todo check timeout
        // const timeout = $meta.timeout && this.timing && Math.floor(this.timing.diff(this.timing.now(), $meta.timeout));
        // if (Number.isFinite(timeout) && timeout <= this.config.minLatencyRequest) throw this.errors.timeout();
        return new Promise((resolve, reject) => {
            const request = {...this.config.request, ...params};
            this.httpClient(request, (error, response, body) => {
                try {
                    const {
                        statusCode,
                        statusText,
                        statusMessage
                    } = response || {};
                    const {
                        method,
                        uri,
                        url
                    } = (response && response.request) || request;
                    this.log && this.log.debug && this.log.debug({
                        error,
                        http: {
                            method,
                            url: (uri && uri.href) || url,
                            statusCode,
                            statusText,
                            statusMessage,
                            body
                        }
                    });
                    if (error) {
                        this.log && this.log.error && this.log.error(error);
                        reject(error);
                    } else if (response.statusCode < 200 || response.statusCode >= 300) {
                        const error = this.errors['webhook.http']({
                            message: (response.body && response.body.message) || 'HTTP error',
                            statusCode,
                            statusText,
                            statusMessage,
                            validation: response.body && response.body.validation,
                            debug: response.body && response.body.debug,
                            code: statusCode,
                            body: response.body
                        });
                        reject(error);
                    } else {
                        resolve(body);
                    }
                } catch (e) {
                    this.log && this.log.error && this.log.error(e);
                    reject(e);
                }
            });
        });
    }

    getConversion($meta, type) {
        const {fn, name} = super.getConversion($meta, type);
        const validation = fn && this.validations && this.validations[$meta.method];
        if (validation) {
            let schema;
            if ($meta.mtid === 'request') {
                schema = validation.params;
            } else if ($meta.mtid === 'response') {
                schema = validation.result;
            }
            if (schema) {
                const validate = msg => schema.validateAsync(msg);
                if (type === 'send') {
                    return {
                        name,
                        fn: function(msg, ...rest) {
                            return fn.call(this, msg, ...rest).then(validate);
                        }
                    };
                } else if (type === 'receive') {
                    return {
                        name,
                        fn: function(msg, ...rest) {
                            return validate(msg).then(result => fn.call(this, result, ...rest));
                        }
                    };
                }
            }
        }
        return {fn, name};
    }

    async init() {
        if (this.config.validations) {
            const flatten = (prefix, methods) => {
                return methods && Object.keys(methods).reduce((all, key) => {
                    all[`${prefix}.${key}`] = methods[key]();
                    return all;
                }, {});
            };
            const {server, client} = this.config.validations;
            const {namespace, hook} = this.config;
            this.validations = {...flatten(namespace, client), ...flatten(hook, server)};
        }
        const webhookErrors = registerErrors(errors);
        Object.keys(webhookErrors).forEach(error => {
            this.errors[error] = (params = {}) => {
                params.platform = this.config.id;
                return webhookErrors[error](params);
            };
        });
        const result = await super.init(...arguments);
        this.httpServer = new Hapi.Server(this.config.server);
        if (this.config.capture) {
            await this.httpServer.register({
                plugin: require('ut-function.capture-hapi'),
                options: {name: this.config.id + '-receive', ...this.config.capture}
            });
        }
        this.httpClient = this.config.capture ? require('ut-function.capture-request')(request,
            {name: this.config.id + '-send', ...this.config.capture}) : request;
        this.config.k8s = {
            ports: [{
                name: 'http-webhook',
                service: true,
                ingress: {
                    host: this.config.server.host,
                    path: this.config.path.replace(/\/\{.*/, '')
                },
                containerPort: this.config.server.port
            }]
        };
        return result;
    }

    async start() {
        const result = await super.start(...arguments);
        const bail = promise => {
            if (!promise) return promise;
            if (this.config.bail === false) {
                promise = promise
                    .then(result => ({result}))
                    .catch(error => ({error}));
            }
            return promise;
        };
        let send;
        switch (this.config.mode) {
            case 'reply':
                send = () => {
                    throw this.errors['webhook.invalidSend']();
                };
                break;
            case 'map':
                send = requests => {
                    return Promise.all([].concat(requests).map(request => {
                        return bail(this.sendRequest(request));
                    }));
                };
                break;
            case 'reduce':
                send = requests => {
                    return [].concat(requests).reduce((promise, request) => {
                        return promise.then(() => {
                            return bail(this.sendRequest(request));
                        });
                    }, Promise.resolve());
                };
                break;
            default:
                throw this.errors['webhook.invalidMode']();
        }
        const stream = this.pull({exec: this.sendRequest}, {requests: {}});
        this.httpServer.route({
            method: 'POST',
            path: this.config.path,
            options: {
                auth: false,
                payload: {
                    parse: 'gunzip',
                    allow: ['application/json', 'application/x-www-form-urlencoded'],
                    output: 'data'
                },
                pre: [
                    {
                        assign: 'body',
                        failAction: 'error',
                        method: ({payload, mime}) => {
                            switch (mime) {
                                case 'application/json':
                                    return payload.length ? bourne.parse(payload.toString('utf8')) : null; // see https://hueniverse.com/a-tale-of-prototype-poisoning-2610fa170061
                                case 'application/x-www-form-urlencoded':
                                    return payload.length ? querystring.parse(payload.toString('utf8')) : {};
                            }
                        }
                    },
                    {
                        assign: 'auth',
                        failAction: 'error',
                        method: async({payload, headers, params, server, info, url, pre}) => {
                            const {host, hostname, id, received, referrer, remoteAddress, remotePort} = info;
                            return new Promise((resolve, reject) => {
                                stream.push([
                                    pre.body,
                                    {
                                        mtid: 'request',
                                        method: this.config.hook + '.identity',
                                        payload,
                                        headers,
                                        params,
                                        serverInfo: server.info,
                                        requestInfo: {host, hostname, id, received, referrer, remoteAddress, remotePort},
                                        forward: {'x-b3-traceid': uuid().replace(/-/g, '')},
                                        url,
                                        reply: (auth, $meta) => {
                                            if (!$meta || $meta.mtid === 'error') {
                                                return reject(Boom.unauthorized(auth.message));
                                            }
                                            resolve(auth);
                                        }
                                    }
                                ]);
                            });
                        }
                    }
                ],
                handler: ({payload, headers, params, server, info, url, pre}, h) => {
                    const {host, hostname, id, received, referrer, remoteAddress, remotePort} = info;
                    const $meta = {
                        mtid: 'request',
                        method: this.config.hook + '.message',
                        auth: pre.auth,
                        payload,
                        headers,
                        params,
                        serverInfo: server.info,
                        requestInfo: {host, hostname, id, received, referrer, remoteAddress, remotePort},
                        url
                    };
                    const reply = (params = {}) => {
                        const { body, code } = {...this.config.response, ...params};
                        return h.response(body).code(code);
                    };
                    const response = () => {
                        if (this.methods[this.config.hook + '.server.response.send']) return this.methods[this.config.hook + '.server.response.send'](pre.body, $meta);
                    };
                    return new Promise((resolve, reject) => {
                        const chain = promise => (promise || Promise.resolve({}))
                            .then(response)
                            .then(reply)
                            .then(resolve, reject);
                        stream.push([pre.body, {
                            ...$meta,
                            reply: (requests, $meta) => {
                                if (requests === undefined) {
                                    throw new Error('Missing request value');
                                }
                                if (!$meta || $meta.mtid === 'error') {
                                    return reject(requests);
                                }
                                if (this.config.mode === 'reply') {
                                    resolve(reply(requests));
                                    return true;
                                }
                                if (this.config.async === 'server') {
                                    return send(requests);
                                }
                                if (this.config.async === 'client') {
                                    chain();
                                    return send(requests);
                                }
                                return chain(send(requests));
                            }
                        }]);
                        if (this.config.async === 'server') {
                            chain();
                        }
                    });
                }
            }
        });
        this.httpServer.route({
            method: 'GET',
            path: '/healthz',
            options: {
                auth: false,
                handler: (request, h) => {
                    const code = this.isReady ? 200 : 202;
                    return h.response({state: this.state}).code(code);
                }
            }
        });
        await this.httpServer.start();
        return result;
    }

    async stop() {
        if (this.httpServer) {
            await this.httpServer.stop();
            delete this.httpServer;
        }
        return super.stop(...arguments);
    }
};
