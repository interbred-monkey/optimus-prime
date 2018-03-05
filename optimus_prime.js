#! /usr/local/bin/node

var _         = require('underscore'),
    fs        = require('fs'),
    csv       = require('csv-parser'),
    util      = require('util'),
    moment    = require('moment'),
    Transform = require('stream').Transform;

var desktop       = process.env['HOME'] + '/Desktop/',
    input_file    = desktop + '<%input_file%>',
    output_file   = desktop + '<%output_file%>',
    wrote_header  = false;

// make sure the files exist
if (!fs.existsSync(input_file)) {

  console.log('Input file does not exist');
  process.exit();

}

function createObj(headers, values) {

  var obj = {};

  for (var v in values) {

    obj[headers[v]] = values[v]

  }

  return obj;

}

function normaliseString(str) {

  str = encodeURIComponent(str.trim());

  str = str.replace(/'/g, '%27');
  str = str.replace(/"/g, "%22");

  return str;

}

var output = fs.createWriteStream(output_file, {encoding: 'utf8'});

function OptimusPrime() {

  if (!(this instanceof OptimusPrime)) {

    return new OptimusPrime();

  }

  Transform.call(this, {objectMode: true});

}

// add in the transform functionality
util.inherits(OptimusPrime, Transform);

// add in the tranforming functionality
OptimusPrime.prototype._transform = function(data, encoding, callback) {

  var headers = [
    '<%header1%>',
    '<%header2%>',
    '<%header3%>',
    '<%header4%>',
    '<%header5%>'
  ]

   if (wrote_header === false) {

    this.push(headers.join('\t') + '\n');
    wrote_header = true;

  }

  try {

    var data = createObj(headers, data[_.keys(data)[0]].split('<%separator%>'));
    data.publisher_urls = (_.isString(data.publisher_urls)?JSON.parse(data.publisher_urls).join(', '):"");

    if (_.isString(data.messages)) {

      var messages = JSON.parse(data.messages);
      data.messages = (_.isArray(messages) && !_.isEmpty(messages) && _.isObject(messages[0])?messages[0].messages[0]:"");

    }

    else {

      data.messages = "";

    }

    data = _.values(data).map(function(val) { return (val?val.toString():"") });

    this.push(_.values(data).join('\t') + '\n');

  }

  catch(e) {

    console.log(e.message);
    console.log(data);

  }

  return setImmediate(callback);

}

// do this thing
var transformer = new OptimusPrime();

fs.createReadStream(input_file, {encoding: 'utf8'})
.pipe(csv({separator: '\t', quote: '"', newline: '\n', strict: false}))
.pipe(transformer)
.pipe(output)
.on('finish', function() {

  console.log("Finished");

});