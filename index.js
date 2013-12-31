var _     = require('underscore'),
    Q     = require('q'),
    redis = require('redis');

function RedisService(config) {
  this.config = config;
  if(this.config.client) {
    this.client = this.config.client;
  } else {
    if(config.path) {
      this.client = redis.createClient(config.path);
    } else {
      this.client = redis.createClient(config.port, config.hostname);
    }
    if(config.password) this.client.auth(config.password);
  }

  this.client.on('error', console.warn);
}

RedisService.prototype.fetchObject = function(key) {
  key = this._toKey(key);

  return Q
  .ninvoke(this.client, 'hgetall', key)
  .then(this._decodeObject.bind(this));
};

RedisService.prototype.storeObject = function(key, fields, options) {
  var _this = this;
  options = options || {};
  key = this._toKey(key);
  fields = this._encodeObject(fields);

  var promise = Q.ninvoke(this.client, 'hmset', key, fields);

  if(options.ttl) {
    promise = promise.then(function() {
      return Q.ninvoke(_this.client, 'expire', key, options.ttl);
    });
  }

  return promise;
};

RedisService.prototype.destroyObject = function(key) {
  key = this._toKey(key);
  return Q.ninvoke(this.client, 'del', key);
};

RedisService.prototype.fetch = function(key) {
  key = this._toKey(key);
  return Q.ninvoke(this.client, 'get', key)
  .then(JSON.parse);
};

RedisService.prototype.store = function(key, value) {
  key = this._toKey(key);
  return Q.ninvoke(this.client, 'set', key, JSON.stringify(value));
};

RedisService.prototype.expire = function(key, ttl) {
  key = this._toKey(key);
  return Q.ninvoke(this.client, 'expire', key, ttl);
};

RedisService.prototype._toKey = function(key) {
  return (_.isArray(key) ? key.join(':') : key);
};

// For Redis hash objects we want to allow arbitrary objects as values
// Therefore we add a ! to the end of keys to signal that their values are
// serialized JSON.

RedisService.prototype._decodeObject = function(obj) {
  var decodedArray = _.map(obj, this._decodeField.bind(this));
  return _.object(decodedArray);
};

RedisService.prototype._decodeField = function(value, key) {
  if(key.slice(-1) === '!') {
    return [key.slice(0, -1), JSON.parse(value)];
  } else {
    return [key, value];
  }
};

RedisService.prototype._encodeObject = function(obj) {
  var encodedArray = _.map(obj, this._encodeField.bind(this));
  return _.object(encodedArray);
};

RedisService.prototype._encodeField = function(value, key) {
  if(_.isString(value)) {
    return [key, value];
  } else {
    return [key + '!', JSON.stringify(value)];
  }
};

/**
 * Fetch sso-configs from Redis for a certain institution
 *
 * @param String endpoint
 * @param String secretId
 */
RedisService.prototype.getSsoConfig = function(endpoint, secretId) {

  return this.fetchObject('sso-configs')
    .then( function(ssoConfigs) {
      var field = [endpoint, secretId].join(':');
      return ssoConfigs[field];
    });
};

module.exports = RedisService;

