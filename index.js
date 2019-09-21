const fs = require('fs');
const csvToJSON = require('csvtojson')
const { AsyncIterable } = require('ix');
const { metrohash64 } = require('metrohash');
const {
    Bool, Utf8, Int64, Float64, Struct, Dictionary, Int32,
    Field, Builder, RecordBatch, RecordBatchWriter
} = require('apache-arrow');

const csvToJSONStream = fs
    .createReadStream('./big.csv')
    .pipe(csvToJSON({}, { objectMode: true }));

AsyncIterable.fromNodeStream(csvToJSONStream)
    // multicast the source CSV stream so we can share a single
    // underlying iterator between multiple consumers.
    .publish((JSONRows) => {
        return AsyncIterable.defer(async () => {
            // Determine the schema from the types of values present in the first row
            const rest = JSONRows.skip(1);
            const row0 = await JSONRows.first();
            // This top-level Builder builds an Arrow StructVector, which is a
            // nested Vector that parents other Vectors, addressing them by
            // field name. The StructBuilder has built-in support for plucking
            // child values from arbitrary JS objects by key and writing them
            // into the child Vector Builders, which makes it the perfect type
            // to use to transpose a stream of JSON rows to a columnar layout
            const { type, ...otherBuilderOptions } = jsValueToArrowBuilderOptions(row0);
            const transform = Builder.throughAsyncIterable({
                type, ...otherBuilderOptions,
                // flush chunks once their size grows beyond 64kb
                queueingStrategy: 'bytes', highWaterMark: 1 << 16,
                // null-value sentinels that will signify "null" slots
                nullValues: [null, undefined, 'n/a', 'NULL'],
            });
            // Concatenate the first row with the rest of the rows, and
            // pipe them through the Arrow StructBuilder transform function
            return AsyncIterable.of(row0).concat(rest).pipe(transform);
        });
    })
    // Translate each Arrow StructVector chunk into a RecordBatch so it can be
    // flushed as an Arrow IPC Message by the RecordBatchStreamWriter transform stream
    .map((chunk) => RecordBatch.new(chunk.data.childData, chunk.type.children))
    // Pipe each RecordBatch through the stream writer transform
    .pipe(RecordBatchWriter.throughNode())
    // And finally, direct each Arrow IPC Message to stdout
    .pipe(process.stdout);


// Naively translate JS values to their rough Arrow equivalents
function jsValueToArrowBuilderOptions(value) {
    if (value) {
        switch (typeof value) {
            case 'bigint':
                return { type: new Int64() };
            case 'boolean':
                return { type: new Bool() };
            case 'number':
                return { type: new Float64() };
            case 'string':
                return { type: new Dictionary(new Utf8(), new Int32()), dictionaryHashFunction: metrohash64 };
            case 'object':

                const { childFields, childBuilderOptions } = Object.keys(value).reduce((memo, name) => {
                    const { type, ...childBuilderOptions } = jsValueToArrowBuilderOptions(value[name]);
                    if (type) {
                        memo.childBuilderOptions.push(childBuilderOptions);
                        memo.childFields.push(new Field(name, type, true));
                    }
                    return memo;
                }, { childFields: [], childBuilderOptions: [] });

                if (Array.isArray(value)) {
                    return { type: new Struct(childFields), children: childBuilderOptions };
                }

                return {
                    type: new Struct(childFields),
                    children: childBuilderOptions.reduce((children, childBuilderOptions, childIndex) => ({
                        ...children, [childFields[childIndex].name]: childBuilderOptions
                    }), {})
                };
        }
    }
    return {};
}
