const fs = require('fs');
const Papa = require('papaparse');
const { AsyncIterable } = require('ix');
const {
    Bool, Utf8, Int64, Float64, Struct, Map_, Dictionary, Int32,
    Field, Builder, RecordBatch, RecordBatchWriter
} = require('apache-arrow');

const parseOptions = { header: true, dynamicTyping: true };
const csvToJSONStream = fs
    .createReadStream('./big.csv')
    .pipe(Papa.parse(Papa.NODE_STREAM_INPUT, parseOptions));

AsyncIterable.fromNodeStream(csvToJSONStream)
    // multicast the source CSV stream so we can share a single
    // underlying iterator between multiple consumers.
    .publish((JSONRows) => {
        return AsyncIterable.defer(async () => {
            // Determine the schema from the types of values present in the first row
            const rest = JSONRows.skip(1);
            const row0 = await JSONRows.first();
            // This top-level Builder builds an Arrow MapVector, which is a
            // nested Vector that parents other Vectors, addressing them by
            // field name. The MapBuilder has built-in support for plucking
            // child values from arbitrary JS objects by key and writing them
            // into the child Vector Builders, which makes it the perfect type
            // to use to transpose a stream of JSON rows to a columnar layout
            const outermostDataType = jsToArrowType(row0);
            const transform = Builder.throughAsyncIterable({
                type: outermostDataType,
                // flush chunks once their size grows beyond 256kb
                queueingStrategy: 'bytes', highWaterMark: 1 << 18,
                // null-value sentinels that will signify "null" slots
                nullValues: [null, undefined, 'n/a', 'NULL'],
            });
            // Concatenate the first row with the rest of the rows, and
            // pipe them through the Arrow MapBuilder transform function
            return AsyncIterable.of(row0).concat(rest).pipe(transform);
        });
    })
    // Translate each Arrow MapVector chunk into a RecordBatch so it can be
    // flushed as an Arrow IPC Message by the RecordBatchStreamWriter transform stream
    .map((chunk) => RecordBatch.new(chunk.data.childData, chunk.type.children))
    // Pipe each RecordBatch through the stream writer transform
    .pipe(RecordBatchWriter.throughNode())
    // And finally, direct each Arrow IPC Message to stdout
    .pipe(process.stdout);


// Naively translate JS values to their rough Arrow equivalents
function jsToArrowType(value) {
    switch (typeof value) {
        case 'bigint': return new Int64();
        case 'boolean': return new Bool();
        case 'number': return new Float64();
        case 'string': return new Dictionary(new Utf8(), new Int32());
        case 'object':
            const fields = Object.keys(value).map((name) => {
                const type = jsToArrowType(value[name]);
                return type ? new Field(name, type, true) : null;
            }).filter(Boolean); 
            return Array.isArray(value) ? new Struct(fields) : new Map_(fields);
    }
    return null;
}
