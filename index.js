const express = require("express");
const bodyParser = require("body-parser");
const fs = require('fs');
const path = require('path');
const urllib = require('urllib');
const BigIntBuffer = require('bigint-buffer');
const JSONbig = require('json-bigint');
const { JSONRPCServer } = require("json-rpc-2.0");
const { exit } = require("process");

// Globals
var currentHeight = 0;
var mainDir;
var port;
var ravendURL;

var argv = require('minimist')(process.argv.slice(2));
if (!('port' in argv) || !('daemon' in argv) || (!'dir' in argv)) {
    console.log('Usage: node index.js --port port --daemon daemon --dir database_path');
    exit();
}
mainDir = argv['dir'];
if (mainDir.charAt(0) == '.') {
    mainDir = path.join(__dirname, mainDir);
}
port = argv['port'];
ravendURL = argv['daemon'];
const lockFile = path.join(mainDir, 'lock');
const heightFile = path.join(mainDir, 'height');
const tsFile = path.join(mainDir, 'timestamps');

// Returns number of elements to remove
async function checkHeightFile(path, currentHeight) {
    if (fs.existsSync(path)) {
        var dummy_height = currentHeight + 1;
        var cnt = 0;
        var first = true;
        while (dummy_height >= currentHeight) {
            cnt++;
            let height_buff = await readLastNBytesFromOffset(path, 4, 4*cnt).catch((e) => {
                if(e != 'Negative Offset') {
                    Promise.reject("checkHeightFile: " + e);
                }
                return true;
            });

            if (height_buff === true) {
                await cutLastNBytesFromOffset(path, 4*cnt);
                return cnt;
            }
            dummy_height = height_buff.readUInt32BE();
            if (first && dummy_height < currentHeight) {
                return 0;
            }
            first = false;
        }
        await cutLastNBytesFromOffset(path, 4*cnt);
        return cnt;
    }
    return 0;
}

async function cutLastNBytesFromOffset(path, n) {
    return new Promise((resolve, reject) => {
        if (n == 0) {
            return resolve();
        }
        fs.stat(path, (err, stats) => {
            if (err) {
                return reject("cut last n1: " + err);
            }
            const offset = stats.size - n;
            
            fs.truncateSync(path, offset, (err) => {
                if (err) {
                    return reject("cut last n2: " + err);
                }
            });
            return resolve();
        })
    });
}

async function readLastNBytes(path, n) {
    return readLastNBytesFromOffset(path, n, n);
}

async function readLastNBytesFromOffset(path, n, negative_offset) {
    return new Promise((resolve, reject) => {
        fs.stat(path, (err, stats) => {
            if (err) {
                return reject('Read last n1: '+err);
            }
            const position = stats.size - negative_offset;
            if (position < 0) {
                return reject('Negative Offset');
            }
            fs.open(path, 'r', function(errOpen, fd) {
                if (errOpen) {
                    return reject('Read last n2' + errOpen);
                }
                fs.read(fd, Buffer.alloc(n), 0, n, position, function(errRead, bytesRead, buffer) {
                    fs.closeSync(fd);
                    if (errRead) {
                        return reject('Read last n3' + errRead);
                    }
                    return resolve(buffer);
                });
            });
        })
    });
}

async function readNBytesFromOffset(path, offset, n) {
    return new Promise((resolve, reject) => {
        fs.open(path, 'r', function(errOpen, fd) {
            if (errOpen) {
                return reject('read n1: ' + errOpen);
            }
            fs.read(fd, Buffer.alloc(n), 0, n, offset, function(errRead, bytesRead, buffer) {
                fs.closeSync(fd);
                if (errRead) {
                    return reject('read n2: ' + errRead);
                }
                return resolve(buffer);
            });
        });
    });
}

// https://www.geeksforgeeks.org/find-closest-number-array/
// Type 0: returns closest value
// Type 1: returns floor
// Type 2: returns ceil
async function binarySearchClosest(path, chunkSize, n, type, func) {
    const nearest = BigInt(n);
    function getClosest(val1, val2, target) {
        if (target - val1 >= val2 - target) {
            return false;
        } else {
            return true;
        }
    }

    return new Promise(async(resolve, reject) => {
        fs.stat(path, (err, stats) => {
            if (err) {
                return reject('bin search 1: ' + err);
            }
            const file_size = stats.size;
            fs.open(path, 'r', async function(errOpen, fd) {
                if (errOpen) {
                    return reject('bin search 2: ' + errOpen);
                }
                const elements = Math.floor(file_size / chunkSize);
                const left_most_value_buf = await readNBytesFromOffset(path, 0, chunkSize);
                const left_most_value = left_most_value_buf[func]();
                if (nearest <= left_most_value) {
                    fs.closeSync(fd);
                    if (type == 1) {
                        if (nearest < left_most_value) {
                            return resolve(-1);
                        } else {
                            return resolve(0);
                        }
                    }
                    return resolve(0);
                }
                const right_most_value_buf = await readLastNBytes(path, chunkSize);
                const right_most_value = right_most_value_buf[func]();
                if (nearest >= right_most_value) {
                    fs.closeSync(fd);
                    return resolve(elements - 1);
                }

                let i = 0;
                let j = elements;
                let mid = 0;
                let mid_value;
                while (i < j) {
                    mid = Math.floor((i + j) / 2);
                    const mid_value_buf = await readNBytesFromOffset(path, chunkSize*mid, chunkSize);
                    mid_value = mid_value_buf[func]();

                    if (mid_value == nearest) {
                        fs.closeSync(fd);
                        return resolve(mid);
                    } else if (nearest < mid_value) {
                        if (mid > 0) {
                            const mid_sub_value_buf = await readNBytesFromOffset(path, chunkSize*(mid-1), chunkSize);
                            const mid_sub_value = mid_sub_value_buf[func]();

                            if (nearest > mid_sub_value) {
                                fs.closeSync(fd);
                                if (type == 1) {
                                    return resolve(mid-1);
                                } else if (type == 2) {
                                    return resolve(mid);
                                }
                                return resolve(getClosest(mid_sub_value, mid_value, nearest) ? (mid - 1) : mid);
                            }
                        }
                        j = mid;
                    } else {
                        if (mid < elements - 1) {
                            const mid_plus_value_buf = await readNBytesFromOffset(path, chunkSize*(mid+1), chunkSize);
                            const mid_plus_value = mid_plus_value_buf[func]();

                            if (nearest < mid_plus_value) {
                                fs.closeSync(fd);
                                if (type == 1) {
                                    return resolve(mid);
                                } else if (type == 2) {
                                    return resolve(mid+1);
                                }
                                return resolve(getClosest(mid_value, mid_plus_value, nearest) ? mid : mid + 1);
                            }
                        }
                        i = mid + 1;
                    }
                }
                fs.closeSync(fd);
                if (type == 1 && nearest < mid_value) {
                    return resolve(mid - 1);
                }
                if (type == 2 && nearest > mid_value) {
                    return resolve(mid + 1);
                }
                return resolve(mid);
            });
        })
    });
}

const server = new JSONRPCServer();

const app = express();
app.use(bodyParser.json());

server.addMethod("currentHeight", (params) => {
    return currentHeight;
});
app.get("/currentHeight", (req, res) => {
    res.end(currentHeight.toString());
});

app.get("/timestamp/:height", async (req, res) => {
    const height = req.params.height;
    if (isNan(height)) {
        return res.end(`height ${height} is not an integer`);
    }
    if (height < 1) {
        return res.end('The height cannot be less than 1');
    } else if (height > currentHeight) {
        return res.end('Invalid block height');
    }
    const tsBuf = await readNBytesFromOffset(tsFile, height*8, 8);
    res.end(tsBuf.readBigUInt64BE().toString());
});
server.addMethod("timestamp", async (params) => {
    const height = params[0];
    if (isNan(height)) {
        throw `height ${height} is not an integer`;
    }
    if (height < 1) {
        throw 'The height cannot be less than 1';
    } else if (height > currentHeight) {
        throw 'Invalid block height';
    }
    const tsBuf = await readNBytesFromOffset(tsFile, height*8, 8);
    return tsBuf.readBigUInt64BE().toString();
});

app.get("/closest_block/:timestamp", async (req, res) => {
    const ts = req.params.timestamp;
    if (isNan(ts)) {
        return res.end(`time ${ts} is not an integer`);
    }
    const closest = await binarySearchClosest(tsFile, 8, ts, 0, 'readBigUInt64BE');
    res.end(closest.toString());
});

server.addMethod("closest_block", async (params) => {
    const ts = params[0];
    if (isNan(ts)) {
        return `time ${ts} is not an integer`;
    }
    return await binarySearchClosest(tsFile, 8, ts, 0, 'readBigUInt64BE');
});

app.get("/first_block/*", async (req, res) => {
    const asset = req.url.replace('/first_block/', '');
    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        return res.end(`asset ${asset} does not exist`);
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const heightBytes = await readNBytesFromOffset(heightFile, 0, 4);
    const height = heightBytes.readUInt32BE();
    res.end(height.toString());
});

server.addMethod("first_block", async (params) => {
    const asset = params[0];
    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        throw `asset ${asset} does not exist`;
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const heightBytes = await readNBytesFromOffset(heightFile, 0, 4);
    return heightBytes.readUInt32BE();
})

app.get("/last_block/*", async (req, res) => {
    const asset = req.url.replace('/last_block/', '');
    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        return res.end(`asset ${asset} does not exist`);
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const heightBytes = await readLastNBytes(heightFile, 4);
    const height = heightBytes.readUInt32BE();
    res.end(height.toString());
});

server.addMethod("last_block", async (params) => {
    const asset = params[0];
    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        throw `asset ${asset} does not exist`;
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const heightBytes = await readLastNBytes(heightFile, 4);
    return heightBytes.readUInt32BE();
});

async function getStatsForOffset(dataDir, offset) {
    const heightFile = path.join(dataDir, 'height');
    const feeFile = path.join(dataDir, 'fees'); // 8
    const volumeFile = path.join(dataDir, 'volume'); // 16
    const voutFile = path.join(dataDir, 'vouts'); // 8
    const reissueFile = path.join(dataDir, 'reissues'); // 8
    const transferFile = path.join(dataDir, 'transfers'); // 8

    const dbHeightBytes = await readNBytesFromOffset(heightFile, offset*4, 4);
    const dbHeight = dbHeightBytes.readUInt32BE();
    const tsBytes = await readNBytesFromOffset(tsFile, dbHeight*8, 8);
    const ts = tsBytes.readBigUInt64BE();
    const feeBytes = await readNBytesFromOffset(feeFile, offset*8, 8);
    const fee = feeBytes.readBigUInt64BE();
    const volumeBytes = await readNBytesFromOffset(volumeFile, offset*16, 16);
    const volume = BigIntBuffer.toBigIntBE(volumeBytes);
    const voutBytes = await readNBytesFromOffset(voutFile, offset*8, 8);
    const vouts = voutBytes.readBigUInt64BE();
    const reissueBytes = await readNBytesFromOffset(reissueFile, offset*8, 8);
    const reissues = reissueBytes.readBigUInt64BE();
    const transferBytes = await readNBytesFromOffset(transferFile, offset*8, 8);
    const transfers = transferBytes.readBigUInt64BE();

    return [dbHeight, ts, volume, fee, vouts, reissues, transfers];
}

async function getStatsForBlockFrame(assetDir, from, to) {
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const closest_from = await binarySearchClosest(heightFile, 4, from, 2, 'readUInt32BE');
    const closest_to = await binarySearchClosest(heightFile, 4, to, 1, 'readUInt32BE');

    //res.send('{\n');
    let ret = '{\n';
    for (let i = closest_from; i <= closest_to; i++) {
        const [dbHeight, ts, volume, fee, vouts, reissues, transfers] = await getStatsForOffset(dataDir, i);
        //res.end(`\t"${dbHeight}": {"total_volume":"${volume}", "total_relative_fees":${fee}, "total_vouts":${vouts}, "total_reissue_vouts":${reissues}, "total_transfer_vouts":${transfers}}${i == closest_to ? '' : ','}\n`);
        ret += `\t"${dbHeight}": {"timestamp":"${ts}", "total_volume":"${volume}", "total_relative_fees":${fee}, "total_vouts":${vouts}, "total_reissue_vouts":${reissues}, "total_transfer_vouts":${transfers}}${i == closest_to ? '' : ','}\n`;
    }
    return ret + '}\n';
}
app.get("/blockframe/*", async (req, res) => {
    let url = req.url;
    let offset = url.lastIndexOf('/');
    const to = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    offset = url.lastIndexOf('/');
    const from = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    const asset = url.replace('/blockframe/', '');

    if (isNan(from)) {
        return res.end(`from block ${from} is not an integer`);
    }
    if (isNan(to)) {
        return res.end(`to block ${to} is not an integer`);
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        return res.end(`asset ${asset} does not exist`);
    }
    res.end(await getStatsForBlockFrame(assetDir, from, to));
});

server.addMethod("blockframe", async (params) => {
    const [asset, from, to] = params;

    if (isNan(from)) {
        throw `from block ${from} is not an integer`;
    }
    if (isNan(to)) {
        throw `to block ${to} is not an integer`;
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        throw `asset ${asset} does not exist`;
    }
    const parsed = JSON.parse(await getStatsForBlockFrame(assetDir, from, to));
    return parsed;
});

app.get("/timeframe/*", async (req, res) => {
    let url = req.url;
    let offset = url.lastIndexOf('/');
    const to = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    offset = url.lastIndexOf('/');
    const from = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    const asset = url.replace('/timeframe/', '');

    if (isNan(from)) {
        return res.end(`from time ${from} is not an integer`);
    }
    if (isNan(to)) {
        return res.end(`to time ${to} is not an integer`);
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        return res.end(`asset ${asset} does not exist`);
    }

    const from_block = await binarySearchClosest(tsFile, 8, from, 0, 'readBigUInt64BE');
    const to_block = await binarySearchClosest(tsFile, 8, to, 0, 'readBigUInt64BE');
    res.end(await getStatsForBlockFrame(assetDir, from_block, to_block));
});

server.addMethod("timeframe", async (params) => {
    const [asset, from, to] = params;
    if (isNan(from)) {
        throw `from time ${from} is not an integer`;
    }
    if (isNan(to)) {
        throw `to time ${to} is not an integer`;
    }
    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        throw `asset ${asset} does not exist`;
    }
    const from_block = await binarySearchClosest(tsFile, 8, from, 0, 'readBigUInt64BE');
    const to_block = await binarySearchClosest(tsFile, 8, to, 0, 'readBigUInt64BE');
    const parsed = JSON.parse(await getStatsForBlockFrame(assetDir, from_block, to_block));
    return parsed;
});

app.get("/timedelta/*", async (req, res) => {
    let url = req.url;
    let offset = url.lastIndexOf('/');
    const to = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    offset = url.lastIndexOf('/');
    const from = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    const asset = url.replace('/timedelta/', '');
    
    if (isNan(from)) {
        return res.end(`from time ${from} is not an integer`);
    }
    if (isNan(to)) {
        return res.end(`to time ${to} is not an integer`);
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        return res.end(`asset ${asset} does not exist`);
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const from_block = await binarySearchClosest(tsFile, 8, from, 0, 'readBigUInt64BE');
    const to_block = await binarySearchClosest(tsFile, 8, to, 0, 'readBigUInt64BE');
    const closest_from = await binarySearchClosest(heightFile, 4, from_block, 2, 'readUInt32BE');
    const closest_to = await binarySearchClosest(heightFile, 4, to_block, 1, 'readUInt32BE');

    const [dbHeight1, ts1, volume1, fee1, vouts1, reissues1, transfers1] = await getStatsForOffset(dataDir, closest_from);
    const [dbHeight2, ts2, volume2, fee2, vouts2, reissues2, transfers2] = await getStatsForOffset(dataDir, closest_to);

    res.end(`{\n\t"starting_block":${dbHeight1},\n\t"starting_timestamp":${ts1},\n\t"ending_block":${dbHeight2},\n\t"ending_timestamp":${ts2},\n\t"d_volume":"${volume2-volume1}",\n\t"d_fees":${fee2-fee1},\n\t"d_vouts":${vouts2-vouts1},\n\t"d_reissues":${reissues2-reissues1},\n\t"d_transfers":${transfers2-transfers1}\n}\n`);

});

server.addMethod("timedelta", async (params) => {
    const [asset, from, to] = params;

    if (isNan(from)) {
        throw `from time ${from} is not an integer`;
    }
    if (isNan(to)) {
        throw `to time ${to} is not an integer`;
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        throw `asset ${asset} does not exist`;
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const from_block = await binarySearchClosest(tsFile, 8, from, 0, 'readBigUInt64BE');
    const to_block = await binarySearchClosest(tsFile, 8, to, 0, 'readBigUInt64BE');
    const closest_from = await binarySearchClosest(heightFile, 4, from_block, 2, 'readUInt32BE');
    const closest_to = await binarySearchClosest(heightFile, 4, to_block, 1, 'readUInt32BE');

    const [dbHeight1, ts1, volume1, fee1, vouts1, reissues1, transfers1] = await getStatsForOffset(dataDir, closest_from);
    const [dbHeight2, ts2, volume2, fee2, vouts2, reissues2, transfers2] = await getStatsForOffset(dataDir, closest_to);

    const parsed = JSON.parse(`{\n\t"starting_block":${dbHeight1},\n\t"starting_timestamp":${ts1},\n\t"ending_block":${dbHeight2},\n\t"ending_timestamp":${ts2},\n\t"d_volume":"${volume2-volume1}",\n\t"d_fees":${fee2-fee1},\n\t"d_vouts":${vouts2-vouts1},\n\t"d_reissues":${reissues2-reissues1},\n\t"d_transfers":${transfers2-transfers1}\n}\n`);
    return parsed;
});

app.get("/blockdelta/*", async (req, res) => {
    let url = req.url;
    let offset = url.lastIndexOf('/');
    const to = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    offset = url.lastIndexOf('/');
    const from = parseInt(url.substring(offset + 1));
    url = url.substring(0, offset);
    const asset = url.replace('/blockdelta/', '');

    if (isNan(from)) {
        return res.end(`from height ${from} is not an integer`);
    }
    if (isNan(to)) {
        return res.end(`to height ${to} is not an integer`);
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        return res.end(`asset ${asset} does not exist`);
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const closest_from = await binarySearchClosest(heightFile, 4, from, 2, 'readUInt32BE');
    const closest_to = await binarySearchClosest(heightFile, 4, to, 1, 'readUInt32BE');

    const [dbHeight1, ts1, volume1, fee1, vouts1, reissues1, transfers1] = await getStatsForOffset(dataDir, closest_from);
    const [dbHeight2, ts2, volume2, fee2, vouts2, reissues2, transfers2] = await getStatsForOffset(dataDir, closest_to);

    res.end(`{\n\t"starting_block":${dbHeight1},\n\t"starting_timestamp":${ts1},\n\t"ending_block":${dbHeight2},\n\t"ending_timestamp":${ts2},\n\t"d_volume":"${volume2-volume1}",\n\t"d_fees":${fee2-fee1},\n\t"d_vouts":${vouts2-vouts1},\n\t"d_reissues":${reissues2-reissues1},\n\t"d_transfers":${transfers2-transfers1}\n}\n`);

});

server.addMethod("blockdelta", async (params) => {
    const [asset, from, to] = params;

    if (isNan(from)) {
        throw `from height ${from} is not an integer`;
    }
    if (isNan(to)) {
        throw `to height ${to} is not an integer`;
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        throw `asset ${asset} does not exist`;
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const closest_from = await binarySearchClosest(heightFile, 4, from, 2, 'readUInt32BE');
    const closest_to = await binarySearchClosest(heightFile, 4, to, 1, 'readUInt32BE');

    const [dbHeight1, ts1, volume1, fee1, vouts1, reissues1, transfers1] = await getStatsForOffset(dataDir, closest_from);
    const [dbHeight2, ts2, volume2, fee2, vouts2, reissues2, transfers2] = await getStatsForOffset(dataDir, closest_to);

    const parsed = JSON.parse(`{\n\t"starting_block":${dbHeight1},\n\t"starting_timestamp":${ts1},\n\t"ending_block":${dbHeight2},\n\t"ending_timestamp":${ts2},\n\t"d_volume":"${volume2-volume1}",\n\t"d_fees":${fee2-fee1},\n\t"d_vouts":${vouts2-vouts1},\n\t"d_reissues":${reissues2-reissues1},\n\t"d_transfers":${transfers2-transfers1}\n}\n`);
    return parsed;
});

app.get("/stats/*", async (req, res) => {
    let url = req.url;
    let offset = url.lastIndexOf('/');
    const height = parseInt(url.substring(offset + 1));

    if (isNaN(height)) {
        return res.end(`height ${url.substring(offset + 1)} is not an integer`);
    }

    url = url.substring(0, offset);
    const asset = url.replace('/stats/', '');

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        return res.end(`asset ${asset} does not exist`);
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const closest = await binarySearchClosest(heightFile, 4, height, 1, 'readUInt32BE');
    let dbHeight;
    let fee;
    let volume;
    let vouts;
    let reissues;
    let transfers
    if (closest < 0) {
        dbHeight = -1;
        ts = -1;
        fee = 0;
        volume = 0;
        vouts = 0;
        reissues = 0;
        transfers = 0;
    } else {
        [dbHeight, ts, volume, fee, vouts, reissues, transfers] = await getStatsForOffset(dataDir, closest);
    }
    res.end(`{\n\t"last_height":${dbHeight},\n\t"last_timestamp":${ts},\n\t"cum_volume":"${volume}",\n\t"cum_fees":${fee},\n\t"cum_vouts":${vouts},\n\t"cum_reissues":${reissues},\n\t"cum_transfers":${transfers}\n}\n`);
});

server.addMethod("stats", async (params) => {
    const [asset, height] = params;

    if (isNan(height)) {
        throw `height ${height} is not a number`;
    }

    const assetDir = path.join(mainDir, asset);
    if (!fs.existsSync(assetDir)) {
        throw `asset ${asset} does not exist`;
    }
    const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
    const heightFile = path.join(dataDir, 'height');
    const closest = await binarySearchClosest(heightFile, 4, height, 1, 'readUInt32BE');
    let dbHeight;
    let fee;
    let volume;
    let vouts;
    let reissues;
    let transfers
    if (closest < 0) {
        dbHeight = -1;
        ts = -1;
        fee = 0;
        volume = 0;
        vouts = 0;
        reissues = 0;
        transfers = 0;
    } else {
        [dbHeight, ts, volume, fee, vouts, reissues, transfers] = await getStatsForOffset(dataDir, closest);
    }
    const parsed = JSON.parse(`{\n\t"last_height":${dbHeight},\n\t"last_timestamp":${ts},\n\t"cum_volume":"${volume}",\n\t"cum_fees":${fee},\n\t"cum_vouts":${vouts},\n\t"cum_reissues":${reissues},\n\t"cum_transfers":${transfers}\n}\n`);
    return parsed;
});

app.post("/", (req, res) => {
  const jsonRPCRequest = req.body;
  server.receive(jsonRPCRequest).then((jsonRPCResponse) => {
    if (jsonRPCResponse) {
      res.json(jsonRPCResponse);
    } else {
      res.sendStatus(204);
    }
  });
});

async function ravendQuery() {
    var id_cnt = 0;
    async function query(method, params) {
        const dataString = {jsonrpc:"2.0",id:id_cnt++,method:method,params:params};
        const res = await urllib.request(ravendURL, 
            {
                method: 'POST', 
                data: JSON.stringify(dataString),
                headers: {'Content-Type': 'application/json'}
            });
        if (res.status != 200) {
            throw "POST status error from " + JSON.stringify(dataString) + ": " + res.status + " " + res.statusMessage;
        }
        let json_resp;
        let jsonStr = res.data.toString('utf8');
        try {
            json_resp = JSONbig.parse(jsonStr);
        } catch (e) {
            console.log(e)
            throw "POST return error from " + jsonStr + ": " + res.data.toString('utf8');
        }
        if (json_resp.error) {
            throw "Json error: " + res.data.error;
        }
        return json_resp.result;
    }

    function onExit() {
        if(fs.existsSync(lockFile)) {
            fs.unlinkSync(lockFile)
        }
        process.exit();
    }
    
    if(!fs.existsSync(mainDir)) {
        fs.mkdirSync(mainDir);
    }

    if (fs.existsSync(lockFile)) {
        console.log('The lock file exists! Is something already using this database?');
        process.exit(1);
    }

    process.on('exit', onExit);
    process.on('SIGINT', onExit);
    process.on('SIGUSR1', onExit);
    process.on('SIGUSR2', onExit);
    process.on('uncaughtException', function(e) {
        console.log('Uncaught Exception...');
        console.log(e.stack);
        onExit();
        process.exit(99);
      });
    fs.writeFileSync(lockFile, process.pid.toString());

    if(fs.existsSync(heightFile)) {
        currentHeight = parseInt(fs.readFileSync(heightFile));
    }

    if(fs.existsSync(tsFile)) {
        fs.truncateSync(tsFile, 8 * currentHeight);
    }
    const issue_names = new Set(['new_asset', 'reissue_asset']);

    while(true) {
        const node_height = BigInt(await query('getblockcount', []));
        if(currentHeight < node_height - BigInt(200)) { //Buffer for reorgs
            currentHeight += 1;
            const block_hash_to_parse = await query('getblockhash', [currentHeight]);
            const block_to_parse = await query('getblock', [block_hash_to_parse]);

            const tsBytes = Buffer.alloc(8);
            tsBytes.writeBigUint64BE(BigInt(block_to_parse.time));
            fs.appendFileSync(tsFile, tsBytes);
            
            for (const tx_hash of block_to_parse.tx) {
                const tx = await query('getrawtransaction', [tx_hash, 1]);
                let sats_in = BigInt(0);
                let sats_out = BigInt(0);
                let asset_map = {}; //asset to {tot_bytes, asset volume, number of transactions, number of vouts, (re)issuances, transfers}
                for (const vout of tx.vout) {
                    sats_out += BigInt(vout.valueSat);
                    if('asset' in vout.scriptPubKey) {
                        const asset_name = vout.scriptPubKey.asset.name;
                        const asset_amount = Math.round(vout.scriptPubKey.asset.amount * 100000000);
                        const asset_type = vout.scriptPubKey.type;
                        const size = vout.scriptPubKey.hex.length / 2;
                        
                        if (asset_name in asset_map) {
                            asset_map[asset_name].byte_amt += size;
                            asset_map[asset_name].volume += asset_amount;
                            asset_map[asset_name].vouts += 1;
                            asset_map[asset_name].reissuances += issue_names.has(asset_type) ? 1 : 0;
                            asset_map[asset_name].transfers += asset_type == 'transfer_asset' ? 1 : 0;
                        } else {
                            asset_map[asset_name] = {
                                byte_amt:size,
                                volume:asset_amount, 
                                vouts:1,
                                reissuances: issue_names.has(asset_type) ? 1 : 0,
                                transfers: asset_type == 'transfer_asset' ? 1 : 0};
                        }
                    }
                }
                const assets = Object.keys(asset_map);
                if (assets.length > 0) {
                    for (const vin of tx.vin) {
                        if (vin.txid == null) continue;
                        const vin_tx = await query('getrawtransaction', [vin.txid, 1]);
                        sats_in += BigInt(vin_tx.vout[vin.vout].valueSat);
                    }
                    fee_scalar = parseInt(sats_in - sats_out) / parseInt(tx.size);
                    for (const asset of assets) {
                        const scaled_fee_sats = fee_scalar < 0 ? 0 : Math.ceil(asset_map[asset].byte_amt * fee_scalar);
                        //asset to {fee, asset volume, number of transactions, number of vouts, (re)issuances, transfers}
                        const assetDir = path.join(mainDir, asset);
                        const dataDir = path.join(assetDir, 'a_spacer_that_is_greater_than_32');
                        if (!fs.existsSync(assetDir)) {
                            fs.mkdirSync(assetDir);
                            fs.mkdirSync(dataDir);
                        }
                        
                        // Write height
                        const feeHeightFile = path.join(dataDir, 'height');
                        const elements = await checkHeightFile(feeHeightFile, currentHeight);
                        const heightBytes = Buffer.alloc(4); // Max 16777216 for height
                        heightBytes.writeUInt32BE(currentHeight);
                        fs.appendFileSync(feeHeightFile, heightBytes);
                        
                        // Write fees
                        let old_fees = BigInt(0);
                        const feeFile = path.join(dataDir, 'fees');
                        if (fs.existsSync(feeFile)) {
                            await cutLastNBytesFromOffset(feeFile, elements*8);
                            const old_fee_bytes = await readLastNBytes(feeFile, 8).catch((e) => Buffer.alloc(8));
                            old_fees = old_fee_bytes.readBigUInt64BE();
                        }
                        const feeBytes = Buffer.alloc(8); // Max 1.844674407×10E19 fee sats
                        feeBytes.writeBigUint64BE(old_fees + BigInt(scaled_fee_sats));
                        fs.appendFileSync(feeFile, feeBytes);

                        // Write Volume
                        const volumeFile = path.join(dataDir, 'volume');
                        let old_volume = BigInt(0);
                        if (fs.existsSync(volumeFile)) {
                            await cutLastNBytesFromOffset(volumeFile, elements*16);
                            const old_volume_bytes = await readLastNBytes(volumeFile, 16).catch((e) => Buffer.alloc(16));
                            old_volume = BigIntBuffer.toBigIntBE(old_volume_bytes);
                        }
                        const volumeBytes = BigIntBuffer.toBufferBE(old_volume + BigInt(asset_map[asset].volume), 16)
                        fs.appendFileSync(volumeFile, volumeBytes);

                        // Write vouts
                        const voutFile = path.join(dataDir, 'vouts');
                        let old_vouts = BigInt(0);
                        if (fs.existsSync(voutFile)) {
                            await cutLastNBytesFromOffset(voutFile, elements*8);
                            const old_vouts_bytes = await readLastNBytes(voutFile, 8).catch((e) => Buffer.alloc(8));
                            old_vouts = old_vouts_bytes.readBigUInt64BE();
                        }
                        const voutBytes = Buffer.alloc(8);
                        voutBytes.writeBigUint64BE(old_vouts + BigInt(asset_map[asset].vouts));
                        fs.appendFileSync(voutFile, voutBytes);

                        // Write reissuances
                        const reissueFile = path.join(dataDir, 'reissues');
                        let old_reissues = BigInt(0);
                        if (fs.existsSync(reissueFile)) {
                            await cutLastNBytesFromOffset(reissueFile, elements*8);
                            const old_reissues_bytes = await readLastNBytes(reissueFile, 8).catch((e) => Buffer.alloc(8));
                            old_reissues = old_reissues_bytes.readBigUInt64BE();
                        }
                        const reissueBytes = Buffer.alloc(8);
                        reissueBytes.writeBigUint64BE(old_reissues + BigInt(asset_map[asset].reissuances));
                        fs.appendFileSync(reissueFile, reissueBytes);

                        // Write transfers
                        const transferFile = path.join(dataDir, 'transfers');
                        let old_transfers = BigInt(0);
                        if (fs.existsSync(transferFile)) {
                            await cutLastNBytesFromOffset(transferFile, elements*8);
                            const old_transfers_bytes = await readLastNBytes(transferFile, 8).catch((e) => Buffer.alloc(8));
                            old_transfers = old_transfers_bytes.readBigUInt64BE();
                        }
                        const transferBytes = Buffer.alloc(8);
                        transferBytes.writeBigUint64BE(old_transfers + BigInt(asset_map[asset].transfers));
                        fs.appendFileSync(transferFile, transferBytes);
                    }
                }
            }
            fs.writeFileSync(heightFile, currentHeight.toString());
            if(currentHeight % 1000 == 0) {
                console.log('Parsing height ' + currentHeight);
            }
        } else {
            await new Promise(resolve => setTimeout(resolve, 60000)); //Wait a minute
        }
    }
    
}


Promise.all([
    ravendQuery().catch((e) => {
        console.log(`ravendQuery error: ${e}`);
        exit(1);
    }),
    app.listen(port)
]);
