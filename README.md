# csv-to-arrow-js
A proof of concept demo to transform CSV to Arrow in JS using [`csvtojson`](https://www.npmjs.com/package/csvtojson) and the new Arrow Builder stream APIs.

See this PR for more information: https://github.com/apache/arrow/pull/4476

### Cloning

This demo uses the file `big.csv` from the PapaParse examples. I've committed this file with [git large-file storage](https://git-lfs.github.com/).

After installing `git-lfs`, run these commands to clone the repository and pull the large files:

```sh
git clone https://github.com/trxcllnt/csv-to-arrow-js.git
cd csv-to-arrow-js
git lfs pull
```

If you can't install `git-lfs`, you can download the "Large file" from the [PapaParse demo page](https://www.papaparse.com/demo).

### Running

```sh
# install the dependencies
npm install
# run the demo to convert big.csv to an Arrow RecordBatch stream.
# The Arrow table is printed to the console with the `arrow2csv` utility
npm start
# or time how long it takes
time node index.js > big.arrow
```
