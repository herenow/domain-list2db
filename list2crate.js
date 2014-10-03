// domain.txt list to crate data storage
var cratejs = require("cratejs")
var optimist = require("optimist")

// configs
var argv = optimist
    .usage('Parse a domain csv separeted by commas and insert it a db.\nUsage: $0')
    .alias('e', 'endpoint')
    .describe('e', 'Url to the db endpoint.')
    .default('e', 'http://127.0.0.1/')
    .alias('l', 'list')
    .describe('l', 'Path the the list file.')
    .default('l', 'top-1m.csv')
    .alias('t', 'table')
    .describe('t', 'Db table name to insert to.')
    .default('t', 'known_domains')
	.alias('b', 'buffer')
	.describe('b', 'Insert buffer bulk size, how many inserts at once')
	.default('b', 5000)
 	.alias('d', 'delimeter')
	.describe('d', 'Delimiter string on the list file')
	.default('d', ',')
   	.alias('h', 'help')
    .describe('h', 'For help.')
    .argv;

if (argv.h) {
    optimist.showHelp()
    return
}

argv.e = argv.e.replace(/\/$/, '') // Remove last / from the endpoint url
var endpoint = argv.e + "/_sql"
var list_file = argv.l
var table_name = argv.t
var schema = "create table " + table_name + " (\
				domain string primary key\
				)"
var insert_buf = argv.b // Insert buffer size, do 5000 inserts ber query
var start_fromline = 0; // Start reading from what line


// Connect to crate
var crate = new cratejs({
    host: 'localhost',
    port: 4200,
})

// Crate table
crate.Query(schema).execute(function(err, res) {
    console.log(err, res)
})


// Read file
var fs = require('fs'),
    util = require('util'),
    stream = require('stream'),
    es = require("event-stream");

// Process list
var buf = [];
var buf_size = 0;
var total = 0

s = fs.createReadStream(list_file)
    .pipe(es.split())
    .pipe(es.mapSync(function(line) {
            total++
            // Jump until
            if (start_fromline >= total) return

            // Split line, format: INT, DOMAIN_NAME
            var v = line.split(argv.d)
			var domain = v[1]

			if(! domain) {
				return
			}

            buf.push({
                domain: domain 
            })

            // Check if we should flush the buffer
            if (buf.length >= insert_buf) {
				s.pause();
                flushBuffer(s.resume)
                buf = [] // clean buffer
            }
        })
        .on('error', function() {
            console.log('Error while reading file.');
        })
        .on('end', function() {
            console.log('Read entirefile.')
			flushBuffer()
        })
    );

// Flush the buffer and insert it
function flushBuffer(cb) {
    crate.Insert(table_name).data(buf).run(function(err, res) {
		if(cb) {
			cb()
		}
        if (err) {
            console.log("Failed to insert :(", err)
            return
        }
        console.log("Inserted sucessfuly.")
    });

    console.log("Inserting ", buf.length, " documents to the database, current pos:", total)
}
