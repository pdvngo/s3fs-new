'use strict';

(function (module, Writable, util, extend, Promise) {

    var defaultOptions = {},
        maxParts = 1000,
        partBufferSize = 5242880;

    function MultiPartManager(client, bucket, key, options) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.parts = [];
        this.partNumber = 0;
        this.currentBuffer = new Buffer(0);
        this.bytesWritten = 0;
        this.options = options || {};
    }

    MultiPartManager.prototype.addChunk = function (chunk) {
        this.currentBuffer = Buffer.concat([this.currentBuffer, chunk]);
        if (this.currentBuffer.length >= partBufferSize) {
            var promise = this.addPart(this.currentBuffer);
            this.parts.push(promise);
            this.currentBuffer = new Buffer(0);
        }
    };

    MultiPartManager.prototype.flush = function () {
        if (this.currentBuffer.length) {
            var promise = this.addPart(this.currentBuffer);
            this.parts.push(promise);
            this.currentBuffer = new Buffer(0);
        }
    };

    MultiPartManager.prototype.addPart = function (buffer) {
        var self = this,
            partNumber = ++this.partNumber,
            error;

        if (partNumber > maxParts) {
            error = util.format('Unable to create partNumber:%d. The max partNumber is %d', partNumber, maxParts);
            return this.abort().then(function () {
                return Promise.reject(error);
            }, function () {
                // TODO: combine reason with this error
                return Promise.reject(error);
            });
        }

        return this.uploadId().then(function (uploadId) {
            return new Promise(function (resolve, reject) {
                self.client.uploadPart({
                    Bucket: self.bucket,
                    Key: self.key,
                    Body: buffer,
                    UploadId: uploadId,
                    PartNumber: partNumber
                }, function (err, result) {
                    if (err) {
                        return self.abort().then(function () {
                            reject(err);
                        }, function () {
                            //TODO: combine the multipart upload error with the abort error
                            reject(err);
                        });
                    }
                    result.PartNumber = partNumber;
                    self.bytesWritten += buffer.length;
                    resolve(result);
                });
            });
        });
    };

    MultiPartManager.prototype.abort = function () {
        var self = this;
        return this.uploadId().then(function (uploadId) {
            return new Promise(function (resolve, reject) {
                self.client.abortMultipartUpload({
                    Bucket: self.bucket,
                    Key: self.key,
                    UploadId: uploadId
                }, function (err) {
                    if (err) {
                        return reject(err);
                    }
                    resolve();
                });
            });
        });
    };

    MultiPartManager.prototype.uploadId = function () {
        var self = this;
        /* jscs: disable disallowDanglingUnderscores */
        if (!this._uploadIdPromise) {
            this._uploadIdPromise = new Promise(function (resolve, reject) {
                self.client.createMultipartUpload(extend({
                    Bucket: self.bucket,
                    Key: self.key
                }, self.options), function (err, data) {
                    if (err) {
                        return reject(err);
                    }
                    resolve(data.UploadId);
                });
            });
        }

        return this._uploadIdPromise;

        /* jscs: enable disallowDanglingUnderscores */
    };

    MultiPartManager.prototype.put = function () {
        var self = this;
        return new Promise(function (resolve, reject) {
            self.client.putObject(extend(true, {
                Bucket: self.bucket,
                Key: self.key,
                Body: self.currentBuffer
            }, self.options), function (err, data) {
                if (err) {
                    return reject(err);
                }
                self.bytesWritten += self.currentBuffer.length;
                resolve(data);
            });
        });
    };

    MultiPartManager.prototype.complete = function () {
        var self = this;
        return this.partNumber ? this.uploadId().then(function (uploadId) {
            self.flush();
            return Promise.all(self.parts).then(function (parts) {
                return new Promise(function (resolve, reject) {
                    self.client.completeMultipartUpload({
                        Bucket: self.bucket,
                        Key: self.key,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: parts
                        }
                    }, function (err, data) {
                        if (err) {
                            return reject(err);
                        }
                        resolve(data);
                    });
                });
            });
        }) : this.put(); //if we did not reach the part limit of 5M just use putObject
    };

    function S3WriteStream(client, bucket, key, options) {
        if (!(this instanceof S3WriteStream)) {
            return new S3WriteStream(client, bucket, key, options);
        }
        this.multiPartManager = new MultiPartManager(client, bucket, key, options);
        var streamOptions = extend(defaultOptions, options);
        //initialize
        Writable.call(this, streamOptions);
        this.bytesWritten = 0;
    }

    util.inherits(S3WriteStream, Writable);

    function execCb(cb) {
        if (cb && typeof cb === 'function') {
            cb();
        }
    }

    S3WriteStream.prototype.write = function (chunk, enc, cb) {
        this.multiPartManager.addChunk(chunk);
        execCb(cb);
    };

    S3WriteStream.prototype.end = function (chunk, encoding, cb) {
        var self = this;
        if (chunk) {
            this.multiPartManager.addChunk(chunk);
        }
        this.multiPartManager.complete().then(function () {
            self.bytesWritten = self.multiPartManager.bytesWritten;
            self.emit('finish');
            execCb(cb);
        }, function (reason) {
            self.bytesWritten = self.multiPartManager.bytesWritten;
            self.emit('error', reason);
            execCb(cb);
        });

    };

    module.exports = S3WriteStream;
}(module, require('stream').Writable, require('util'), require('extend'), require('bluebird')));
