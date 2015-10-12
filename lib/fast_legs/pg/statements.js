/**
 * Module dependencies.
 */

var utils = require('./utils');
var _ = require('underscore');

/**
 * Statements.
 */

function fixPgIssues(val) {
  /* The current build of pg doesn't know how to bind an
   * undefined value, so we're going to be nice and coerce
   * any of 'em to null for now */
  if (val === undefined)
    return null;

  return val;
};

exports.select = function(model, selector, opts, outValues) {
  var fields = buildSelectFields(model, opts)
    , stmt   = "SELECT " + fields + " FROM " + '"' + model.tableName + '"'
    , join   = buildJoinClause(model, opts)
    , where  = buildWhereClause(model, selector, outValues)
    , limit  = buildLimitClause(opts)
    , offset = buildOffsetClause(opts)
    , order  = buildOrderClause(opts);

  return stmt + join + where + order + limit + offset + ';';
};

exports.insert = function(model, obj, outValues) {
  var stmt = "INSERT INTO " + '"' + model.tableName + '"'
    , fields = buildInsertFields(model, obj, outValues);

  return stmt + fields + ';';
};

exports.update = function(model, selector, obj, outValues) {
  var stmt  = "UPDATE " + '"' + model.tableName + '"'
    , set   = buildUpdateFields(model, obj, outValues)
    , where = buildWhereClause(model, selector, outValues);

  return stmt + set + where + ';';
};

exports.destroy = function(model, selector, outValues) {
  var stmt  = "DELETE FROM " + '"' + model.tableName + '"'
    , where = buildWhereClause(model, selector, outValues);

  return stmt + where + ";"
};

exports.truncate = function(model, opts) {
  var opts = opts === undefined ? {} : opts
    , stmt = "TRUNCATE " + '"' + model.tableName + '"';

  if (opts.cascade) {
    stmt += " CASCADE";
  }

  return stmt + ";"
};

exports.information = function(model) {
  var stmt =  "SELECT column_name, is_nullable, data_type, " +
              "character_maximum_length, column_default " +
              "FROM information_schema.columns " +
              "WHERE table_name = '" + model.tableName + "';";

  return stmt;
};

var buildInsertFields = function(model, fields, outValues) {
  if (!_(fields).isArray()) {
    fields = [fields]
  }
  fields = _.map(fields, function(field) {
   return utils.validFields(model, field)
  })
  var keys =  utils.keysFromObject(fields)
    , vals =  buildMultiInsert(fields, keys, outValues);

  return "(" + keys + ") VALUES" + vals + ' RETURNING *';
};

var buildJoinClause = function(model, opts) {
  if (_(opts.join).isUndefined()) {
    return "";
  } else {
    model._fields = model._fields.concat(opts.join.model._fields);
    return " INNER JOIN "  + opts.join.model.tableName + " ON " +
      '"' + model.tableName + '"' + "." +
      (opts.join.selfKey || model.primaryKey) + "=" +
      opts.join.model.tableName + "." + opts.join.key;
  }
};

var buildLimitClause = function(opts) {
  if (_(opts.limit).isUndefined()) {
    return "";
  } else {
    return " LIMIT " + opts.limit;
  }
};

var buildOffsetClause = function(opts) {
  if(_(opts.offset).isUndefined()) {
    return "";
  } else {
    return " OFFSET " + opts.offset;
  }
};

var buildMultiInsert = function(fields, keys, outValues) {
  return _(fields).chain()
    .map(function(field) {
      var vals = _(keys).map(function(key) {
        outValues.push(fixPgIssues(field[key]));
        return '$' + (outValues.length);
      });
      return "(" + vals + ")";
    })
    .join(', ')
    .value();
};

var buildAndStatement = function(model, or, outValues) {
  var statement = _.map(or, function(value, key) {
    return buildOperator(model, key, value, outValues);
  });

  return '(' + statement.join(' AND ') + ')';
}

var buildOrStatement = function(model, or, outValues) {
  var statement = _.map(or, function(value, key) {
    if (key.slice(0,4) === '$and')
      return buildAndStatement(model, value, outValues)
    if (utils.fieldIsValid(model, key))
      return buildOperator(model, key, value, outValues);
    return ' INVALID_FIELD ';
  });

  return '(' + statement.join(' OR ') + ')';
}

var buildOperator = function(model, key, value, outValues) {
  if(_.isNumber(key)) {
    key = value[0];
    key = value[1];
  }
  var
  parts = key.split('.'),
  field = parts[0],
  keyOp = parts[1],
  order = 0,
  operator;

  switch(keyOp) {
  case 'ne': case 'not':
    operator = "<>";
    break;
  case 'gt':
    operator = ">";
    break;
  case 'lt':
    operator = "<";
    break;
  case 'gte':
    operator = ">=";
    break;
  case 'lte':
    operator = "<=";
    break;
  case 'like':
    operator = "LIKE";
    break;
  case 'nlike': case 'not_like':
    operator = "NOT LIKE";
    break;
  case 'ilike':
    operator = "ILIKE";
    break;
  case 'nilike': case 'not_ilike':
    operator = "NOT ILIKE";
    break;
  case 'in':
    operator = "IN";
    break;
  case 'nin': case 'not_in':
    operator = "NOT IN";
    break;
  case 'textsearch':
    operator = "@@";
	  break;
  case 'pcre':
    operator = "~";
    order = 1;
	  break;
  case 'npcre':
    operator = "!~";
    order = 1;
	  break;
  case 'ipcre':
    operator = "~*";
    order = 1;
	  break;
  case 'nipcre':
    operator = "!~*";
    order = 1;
	  break;
  default:
    if (value === null) return field + ' IS NULL';
    var operator = "=";
  }
  // XXX Need to handle IS NOT NULL?

  // Support non-primitive values when we need to operate on a referenced field.
  if(_.isObject(value) && value.type == 'field') {
    var name = value.name;
    if(!utils.fieldIsValid(model, name)) {
      name == 'INVALID_NAME';
    }
    if(order) {
      return name + ' ' + operator + ' ' + field;
    } else {
      return field + ' ' + operator + ' ' + name;
    }
  }

  outValues.push(fixPgIssues(value));
  outValues = _.flatten(outValues);
  if (keyOp == 'textsearch') {
    if(order) {
      return 'to_tsquery(\'english\', ' + utils.quote(outValues, operator) + ')'
            + operator +
            ' to_tsvector(\'english\', ' + field + ') ';
    } else {
      return 'to_tsvector(\'english\', ' + field + ') ' + operator +
      ' to_tsquery(\'english\', ' + utils.quote(outValues, operator) + ')';
    }
  } else {
    if(order) {
      return utils.quote(outValues, operator, value) + ' ' + operator + ' ' + field;
    } else {
      return field + ' ' + operator + ' ' + utils.quote(outValues, operator, value);
    }
  }
};

var buildOrderClause = function(opts) {
  if (_(opts.order).isUndefined()) {
    return "";
  } else {
    var orderFields = _(opts.order).chain()
      .map(function(orderField) {
        var direction  = orderField[0] === '-' ? "DESC" : "ASC";
        var orderField = orderField[0] === '-' ?
          utils.doubleQuote(orderField.substring(1, orderField.length)) :
          utils.doubleQuote(orderField);
        return orderField + " " + direction;
      })
      .join(', ')
      .value();

    return " ORDER BY " + orderFields;
  }
};

var buildSelectFields = function(model, opts) {
  if (_(opts.only).isUndefined()) {
    if (!_(opts.join).isUndefined()) {
      return '"' + model.tableName + '"' + ".*";
    } else {
      return (opts.count)?"COUNT(*) AS _count":"*";
    }
  } else if (_(opts.only).isArray()) {
    var columns = _(model._fields).pluck('column_name');
    var valid_fields = _.select(opts.only, function(valid_field) {
      return _.include(columns, valid_field);
    });
    // Add table name to column names. This will avoid conflicts in joins.
    valid_fields = _.map(valid_fields, function(valid_field) {
      return model.tableName + '.' + valid_field;
    });
    var select = _(valid_fields).isEmpty() ? "*" : valid_fields.join(',');
    if(opts.count) {
      select += ', count(*) OVER() AS _count';
    }
    return select;
  } else {
    var columns = _(model._fields).pluck('column_name');
    var alias_fields = [];
    _.map(opts.only, function(value, key) {
      if (_.include(columns, key))
        alias_fields.push(key+' AS '+utils.doubleQuote(value));
    });
    var select = _(alias_fields).isEmpty() ? "*" : alias_fields.join(', ');
    if(opts.count) {
      select += ', count(*) OVER() AS _count';
    }
    return select;
  }
};

var buildUpdateFields = function(model, fields, outValues) {
  var fields = utils.validFields(model, fields)
    , pred   =  _(fields).chain()
                .map(function(value, key) {
                  outValues.push(fixPgIssues(value));
                  return key + '= $' + outValues.length;
                })
                .join(', ')
                .value();

  return utils.nil(pred) ? '' : " SET " + pred;
};

var buildWhereClause = function(model, selector, outValues) {
  if (utils.nil(selector)) {
    var pred = '';
  } else if (_(selector).isArray()) {
    var ids = utils.toCsv(selector, undefined, outValues);
    var pred = model.primaryKey + " IN (" + ids + ")";
  } else if (_(selector).isNumber() || _(selector).isString()) {
    var id = selector;
    var pred = model.primaryKey + " = '" + id + "'";
  } else {
    var pred =  _(selector).chain()
                .map(function(value, key) {
                  if (key === '$or')
                    return buildOrStatement(model, value, outValues)
                  if (utils.fieldIsValid(model, key))
                    return buildOperator(model, key, value, outValues);
                })
                .compact()
                .join(' AND ')
                .value();
    pred += utils.nil(pred) ? 'INVALID' : '';
  }

  return utils.nil(pred) ? '' : " WHERE " + pred;
};
