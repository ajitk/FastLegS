/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter;
var _ = require('underscore')._;
var url = require('url');

/**
 * Client.
 */

function Client(connParams, poolSize) {
  this.connParams = connParams;
  this.poolSize = poolSize;
  this.connected  = false;
  this.lastError = null;
};

Client.prototype.__proto__ = EventEmitter.prototype;

Client.prototype.connect = function() {
  var self = this;
  self.statements = require("./statements")
  var pg = require('pg');
  if(self.poolSize === 1) {
    self.client = new pg.Client(self.connParams);
  } else {
    var poolParams = _.extend(parseParams(self.connParams), {
      max: self.poolSize,
    });
    self.client = new pg.Pool(poolParams);
  }
  // if there are any problems w/ the pg client, record the error
  self.client.on('error', function(err){
    self.lastError = err;
    return true;
  });
  self.on('query', function(query, values, callback) {
    // if there were any errors with the pg client, surface them here then clear
    if(self.lastError !== null){
      var error = self.lastError;
      self.lastError = null;
      callback(error,0);
    }else{
      if (!_.isUndefined(values[0])) values = _.flatten(values);
      self.connected || self.doConnect();
      self.client.query(query, values, callback);
    }
  });
}

Client.prototype.disconnect = function() {
  if (this.client.queryQueue.length === 0) {
    this.client.end();
  } else {
    this.client.on('drain', this.client.end.bind(this.client));
  }
}

Client.prototype.doConnect = function() {
  this.client.connect();
  return this.connected = true;
}

function parseParams(pgUrl) {
  var
  urlObj        = url.parse(pgUrl),
  auth          = urlObj.auth.split(':');

  return {
    user: auth[0],
    password: auth[1],
    database: urlObj.path.slice(1),
    port: urlObj.port,
  }
}

module.exports = Client;
