/**
 * Module dependencies.
 */

var _ = require('underscore');

/**
 * Utils.
 */

var doubleQuote = exports.doubleQuote = function(value, outValues) {
  if (nil(value)) {
    return "NULL";
  } else if (_(value).isNumber()) {
    return value;
  } else if (_(value).isArray()) {
    return "(" + toCsv(value, outValues) + ")";
  } else if (_(value).isDate()) {
    return '"' + toDateTime(value) + '"';
  } else {
    return '"' + value + '"';
  }
};

var fieldIsValid = exports.fieldIsValid = function(model, field) {
  var columns = _(model._fields).pluck('column_name');
  return _.include(columns, field.split('.')[0]);
};

var hasWhiteSpace = exports.hasWhiteSpace = function(value) {
  return /\s/g.test(value);
};

var keysFromObject = exports.keysFromObject = function(fields) {
  return _(fields).chain()
    .map(function(field) {
      return _(field).keys();
    })
    .flatten()
    .uniq()
    .value();
};

var nil = exports.nil = function(value) {
  if (_(value).isUndefined() || _(value).isNull() || _(value).isNaN()) {
    return true;
  } else if (_(value).isArray() && _(value).isEmpty()) {
    return true;
  } else if (value.toString() === '[object Object]' && _(value).isEmpty()) {
    return true;
  } else if (_(value).isString() && _(value).isEmpty()) {
    return true;
  } else {
    return false;
  }
};

var quote = exports.quote = function(outValues, operator, value) {
  if (operator === 'IN' || operator === 'NOT IN') {
    var startIndex = outValues.length + 1;
    var values = _.map(value, function(aVal, i) { // value is a list
      outValues.push(fixPgIssues(aVal));
      return '$' + (startIndex + i);
    });
    return '(' + values.join(',') + ')';
  } else {
    outValues.push(fixPgIssues(value));
    return '$' + outValues.length;
  }
};

var toCsv = exports.toCsv = function(list, keys, outValues) {
  return  _(list).chain()
          .values()
          .map(function(o) { outValues.push(o); return '$' + outValues.length; })
          .join(',')
          .value();
};

var toPlaceholder = exports.toPlaceholder = function(list, keys, outValues) {
  return _(list).chain()
         .values()
         .map(function(o) { outValues.push(o); return '?'; })
         .join(', ')
         .value();
};

var toDateTime = exports.toDateTime = function(value) {
  if (_(value).isDate()) {
    return value.getFullYear()
    + '/' + (value.getMonth()+1)
    + '/' + (value.getDate())
    + ' ' + (value.getHours())
    + ':' + (value.getMinutes())
    + ':' + (value.getSeconds());
  }
};

var validFields = exports.validFields = function(model, fields) {
  var returnFields = {};
  _(fields).each(function(value, key) {
    if (fieldIsValid(model, key)) {
      returnFields[key] = value;
    }
  });
  return returnFields;
};

function fixPgIssues(val) {
  /* The current build of pg doesn't know how to bind an
   * undefined value, so we're going to be nice and coerce
   * any of 'em to null for now */
  if (val === undefined)
    return null;

  return val;
};

