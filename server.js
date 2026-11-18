var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod) => function __require() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports2, module2) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob) BINARY_TYPES.push("blob");
    module2.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: /* @__PURE__ */ Symbol("kIsForOnEventAttribute"),
      kListener: /* @__PURE__ */ Symbol("kListener"),
      kStatusCode: /* @__PURE__ */ Symbol("status-code"),
      kWebSocket: /* @__PURE__ */ Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports2, module2) {
    "use strict";
    var { websocketMask, websocketUnmask } = require("http");
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module2.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (websocketMask) {
      module2.exports.mask = function(source, mask, output, offset, length) {
        if (length < 24) _mask(source, mask, output, offset, length);
        else websocketMask(source, mask, output, offset, length);
      };
      module2.exports.unmask = function(buffer, mask) {
        if (buffer.length < 24) _unmask(buffer, mask);
        else websocketUnmask(buffer, mask);
      };
    } else if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = require("bufferutil");
        module2.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bufferUtil.mask(source, mask, output, offset, length);
        };
        module2.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports2, module2) {
    "use strict";
    var kDone = /* @__PURE__ */ Symbol("kDone");
    var kRun = /* @__PURE__ */ Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency) return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module2.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports2, module2) {
    "use strict";
    var zlib2 = require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = /* @__PURE__ */ Symbol("permessage-deflate");
    var kTotalLength = /* @__PURE__ */ Symbol("total-length");
    var kCallback = /* @__PURE__ */ Symbol("callback");
    var kBuffers = /* @__PURE__ */ Symbol("buffers");
    var kError = /* @__PURE__ */ Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate2 = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num2 = +value;
                if (!Number.isInteger(num2) || num2 < 8 || num2 > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num2;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num2 = +value;
              if (!Number.isInteger(num2) || num2 < 8 || num2 > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num2;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib2.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib2.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib2.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib2.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib2.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module2.exports = PerMessageDeflate2;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports2, module2) {
    "use strict";
    var { isUtf8 } = require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module2.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module2.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = require("utf-8-validate");
        module2.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports2, module2) {
    "use strict";
    var { Writable } = require("stream");
    var PerMessageDeflate2 = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver2 = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxBufferedChunks = options.maxBufferedChunks | 0;
        this._maxFragments = options.maxFragments | 0;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._numFragments = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
          cb(
            this.createError(
              RangeError,
              "Too many buffered chunks",
              false,
              1008,
              "WS_ERR_TOO_MANY_BUFFERED_PARTS"
            )
          );
          return;
        }
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored) cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate2.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num2 = buf.readUInt32BE(0);
        if (num2 > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num2 * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        this._mask = void 0;
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
          const error = this.createError(
            RangeError,
            "Too many message fragments",
            false,
            1008,
            "WS_ERR_TOO_MANY_BUFFERED_PARTS"
          );
          cb(error);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO) this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._numFragments = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module2.exports = Receiver2;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports2, module2) {
    "use strict";
    var { Duplex } = require("stream");
    var { randomFillSync } = require("crypto");
    var {
      types: { isUint8Array }
    } = require("util");
    var PerMessageDeflate2 = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = /* @__PURE__ */ Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender2 = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask) {
          return dataLength ? [target, data] : [target];
        }
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (!dataLength) return [target];
        if (skipMasking) return [target, data];
        if (merge) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else if (isUint8Array(data)) {
            buf.set(data, 2);
          } else {
            throw new TypeError("Second argument must be a string or a Uint8Array");
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module2.exports = Sender2;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function") cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function") callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports2, module2) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = /* @__PURE__ */ Symbol("kCode");
    var kData = /* @__PURE__ */ Symbol("kData");
    var kError = /* @__PURE__ */ Symbol("kError");
    var kMessage = /* @__PURE__ */ Symbol("kMessage");
    var kReason = /* @__PURE__ */ Symbol("kReason");
    var kTarget = /* @__PURE__ */ Symbol("kTarget");
    var kType = /* @__PURE__ */ Symbol("kType");
    var kWasClean = /* @__PURE__ */ Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module2.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0) dest[name] = [elem];
      else dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            let value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1) end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension2) => {
        let configurations = extensions[extension2];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension2].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module2.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var https = require("https");
    var http2 = require("http");
    var net = require("net");
    var querystring = require("querystring");
    var tls = require("tls");
    var { randomBytes, createHash } = require("crypto");
    var { Duplex, Readable } = require("stream");
    var { URL } = require("url");
    var PerMessageDeflate2 = require_permessage_deflate();
    var Receiver2 = require_receiver();
    var Sender2 = require_sender();
    var { isBlob } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = /* @__PURE__ */ Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            if (!options || options.protocols === void 0) {
              protocols = [];
            } else if (Array.isArray(options.protocols)) {
              protocols = options.protocols;
            } else {
              protocols = [options.protocols];
            }
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              if (options.protocols === void 0) {
                protocols = [];
              } else if (Array.isArray(options.protocols)) {
                protocols = options.protocols;
              } else {
                protocols = [options.protocols];
              }
            } else {
              protocols = [protocols];
            }
          }
          options = initAsClient(this, address, protocols, options);
        } else {
          this._isServer = true;
        }
        if (options.autoPong) {
          this._autoPong = true;
          this._autoPongComplete = () => autoPongComplete(this);
          this._pendingAutoPongs = 0;
          this._maxPendingAutoPongs = options.maxPendingAutoPongs | 0;
        }
        this._closeTimeout = options.closeTimeout;
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver2({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxBufferedChunks: options.maxBufferedChunks,
          maxFragments: options.maxFragments,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender2(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout) socket.setTimeout(0);
        if (socket.setNoDelay) socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate2.extensionName]) {
          this._extensions[PerMessageDeflate2.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        this._readyState = _WebSocket.CLOSING;
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain) this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate2.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function") return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module2.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxBufferedChunks: 256 * 1024,
        maxFragments: 16 * 1024,
        maxPayload: 100 * 1024 * 1024,
        maxPendingAutoPongs: 60,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        protocols: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:" || parsedUrl.protocol === "https:") {
        parsedUrl = new URL(parsedUrl);
        parsedUrl.protocol = parsedUrl.protocol === "http:" ? "ws:" : "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes(16).toString("base64");
      const request = isSecure ? https.request : http2.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate2({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate2.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        const username = querystring.unescape(parsedUrl.username);
        const password = querystring.unescape(parsedUrl.password);
        opts.auth = `${username}:${password}`;
      }
      if (isIpcUrl) {
        const i = opts.path.indexOf(":");
        if (i === -1) {
          opts.socketPath = opts.path;
          opts.path = "";
        } else {
          opts.socketPath = opts.path.slice(0, i);
          opts.path = opts.path.slice(i + 1);
        }
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost) delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted]) return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate2.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate2.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxBufferedChunks: opts.maxBufferedChunks,
          maxFragments: opts.maxFragments,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
      return opts;
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream[kAborted] = true;
        stream.abort();
        if (stream.socket && !stream.socket.destroyed) {
          stream.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob(data) ? data.size : toBuffer(data).length;
        if (websocket._socket) websocket._sender._bufferedBytes += length;
        else websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeCode = code;
      if (reason.length === 0) {
        websocket._closeMessage = EMPTY_BUFFER;
      } else {
        websocket._closeMessage = Buffer.allocUnsafeSlow(reason.length);
        websocket._closeMessage.set(reason);
      }
      if (websocket._socket[kWebSocket] === void 0) return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused) websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong && websocket.readyState === WebSocket2.OPEN) {
        if (websocket._maxPendingAutoPongs > 0 && websocket._pendingAutoPongs >= websocket._maxPendingAutoPongs) {
          websocket.terminate();
        } else {
          websocket._pendingAutoPongs++;
          websocket.pong(data, !this._isServer, websocket._autoPongComplete);
        }
      }
      websocket.emit("ping", data);
    }
    function autoPongComplete(websocket) {
      websocket._pendingAutoPongs--;
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED) return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports2, module2) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream2(ws, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data)) ws.pause();
      });
      ws.once("error", function error(err) {
        if (duplex.destroyed) return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws.once("close", function close() {
        if (duplex.destroyed) return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws.once("close", function close() {
          if (!called) callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy) ws.terminate();
      };
      duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws._socket === null) return;
        if (ws._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted) duplex.destroy();
        } else {
          ws._socket.once("finish", function finish() {
            callback();
          });
          ws.close();
        }
      };
      duplex._read = function() {
        if (ws.isPaused) ws.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module2.exports = createWebSocketStream2;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1) end = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1) end = i;
          const protocol2 = header.slice(start, end);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module2.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var http2 = require("http");
    var { Duplex } = require("stream");
    var { createHash } = require("crypto");
    var extension2 = require_extension();
    var PerMessageDeflate2 = require_permessage_deflate();
    var subprotocol2 = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer2 = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=16384] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Number} [options.maxPendingAutoPongs=60] The maximum number of
       *     pending automatic pongs
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxBufferedChunks: 256 * 1024,
          maxFragments: 16 * 1024,
          maxPayload: 100 * 1024 * 1024,
          maxPendingAutoPongs: 60,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http2.createServer((req, res) => {
            const body = http2.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb) this.once("close", cb);
        if (this._state === CLOSING) return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server2 = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server2.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol2.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate2({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension2.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate2.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate2.extensionName]);
              extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate2.extensionName]) {
          const params = extensions[PerMessageDeflate2.extensionName].params;
          const value = extension2.format({
            [PerMessageDeflate2.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxBufferedChunks: this.options.maxBufferedChunks,
          maxFragments: this.options.maxFragments,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => {
            this.clients.delete(ws);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws, req);
      }
    };
    module2.exports = WebSocketServer2;
    function addListeners(server2, map) {
      for (const event of Object.keys(map)) server2.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server2.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server2) {
      server2._state = CLOSED;
      server2.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http2.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http2.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server2, req, socket, code, message, headers) {
      if (server2.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server2.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// server/main.js
var import_node_http = __toESM(require("node:http"), 1);
var import_node_fs = __toESM(require("node:fs"), 1);
var import_node_path = __toESM(require("node:path"), 1);
var import_node_zlib = __toESM(require("node:zlib"), 1);

// node_modules/ws/wrapper.mjs
var import_stream = __toESM(require_stream(), 1);
var import_extension = __toESM(require_extension(), 1);
var import_permessage_deflate = __toESM(require_permessage_deflate(), 1);
var import_receiver = __toESM(require_receiver(), 1);
var import_sender = __toESM(require_sender(), 1);
var import_subprotocol = __toESM(require_subprotocol(), 1);
var import_websocket = __toESM(require_websocket(), 1);
var import_websocket_server = __toESM(require_websocket_server(), 1);

// src/config.js
var CFG = {
  COURT_HL: 9,
  // meia-quadra no eixo X (comprimento total 18 m)
  COURT_HW: 4.5,
  // meia-largura no eixo Z (largura total 9 m)
  NET_H: 2.2,
  BALL_R: 0.11,
  G: 9.81 * 0.85,
  // gravidade com fator de "game feel"
  DRAG: 0.035,
  // arrasto linear leve (1/s)
  DT: 1 / 60,
  BOUNDS_X: 13.5,
  // limites de movimentacao dos atletas
  BOUNDS_Z: 8,
  ATHLETE_SPEED: 5.6,
  ATHLETE_SPRINT: 7,
  ATHLETE_ACCEL: 17,
  REACH: 1.15,
  // alcance horizontal de um toque
  JUMP_V: 3.7,
  POINT_PAUSE: 2.6,
  SERVE_DELAY: 1.3
};
var INTENTS = {
  dominar: { yMin: 0.12, yMax: 2.15, ideal: 1.25 },
  levantar: { yMin: 0.12, yMax: 2.15, ideal: 1.3 },
  pingo: { yMin: 0.12, yMax: 2.95, ideal: 2.45 },
  chapa: { yMin: 0.12, yMax: 3, ideal: 2.7 },
  shark: { yMin: 1.5, yMax: 3.3, ideal: 2.95 }
};
function makeRng(seed) {
  let a = seed >>> 0;
  const r = () => {
    a = a + 1831565813 >>> 0;
    let t = a;
    t = Math.imul(t ^ t >>> 15, t | 1);
    t ^= t + Math.imul(t ^ t >>> 7, t | 61);
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
  r.range = (lo, hi) => lo + (hi - lo) * r();
  r.gauss = () => {
    return (r() + r() + r() + r() - 2) * 1.7320508;
  };
  return r;
}
var clamp = (v, lo, hi) => v < lo ? lo : v > hi ? hi : v;
var lerp = (a, b, t) => a + (b - a) * t;
var smooth = (t) => {
  t = clamp(t, 0, 1);
  return t * t * (3 - 2 * t);
};

// src/physics/Trajectory.js
function stepState(s, dt) {
  const px = s.x, py = s.y, pz = s.z;
  const k = 1 - CFG.DRAG * dt;
  s.vx *= k;
  s.vz *= k;
  s.vy = s.vy * k - CFG.G * dt;
  s.x += s.vx * dt;
  s.y += s.vy * dt;
  s.z += s.vz * dt;
  let ev = null;
  if (px < 0 && s.x >= 0 || px > 0 && s.x <= 0) {
    const f = px / (px - s.x);
    const yc = py + (s.y - py) * f;
    const zc = pz + (s.z - pz) * f;
    if (Math.abs(zc) <= CFG.COURT_HW + 0.45) {
      const r = CFG.BALL_R;
      if (yc < CFG.NET_H - r * 0.35) {
        s.x = Math.sign(px) * (r + 0.02);
        s.y = yc;
        s.z = zc;
        s.vx = -s.vx * 0.1;
        s.vy *= 0.35;
        s.vz *= 0.3;
        ev = "net";
      } else if (yc < CFG.NET_H + r * 0.85) {
        s.vx *= 0.42;
        s.vz *= 0.6;
        s.vy = Math.abs(s.vy) * 0.25 + 1.1;
        s.y = Math.max(s.y, CFG.NET_H + r);
        ev = "tape";
      }
    }
  }
  if (s.y <= CFG.BALL_R) {
    s.y = CFG.BALL_R;
    ev = "ground";
  }
  return ev;
}
function solveApexRaw(from, to, apexY) {
  const g = CFG.G;
  const apex = Math.max(apexY, from.y + 0.05, to.y + 0.05);
  const vy = Math.sqrt(2 * g * (apex - from.y));
  const T = vy / g + Math.sqrt(2 * (apex - to.y) / g);
  return { vx: (to.x - from.x) / T, vy, vz: (to.z - from.z) / T, T };
}
function solveTimeRaw(from, to, T) {
  return {
    vx: (to.x - from.x) / T,
    vy: (to.y - from.y) / T + 0.5 * CFG.G * T,
    vz: (to.z - from.z) / T,
    T
  };
}
function flyTo(from, v, yTarget) {
  const s = { x: from.x, y: from.y, z: from.z, vx: v.vx, vy: v.vy, vz: v.vz };
  const dt = 1 / 120;
  const k = 1 - CFG.DRAG * dt;
  for (let i = 0; i < 1200; i++) {
    s.vx *= k;
    s.vz *= k;
    s.vy = s.vy * k - CFG.G * dt;
    s.x += s.vx * dt;
    s.y += s.vy * dt;
    s.z += s.vz * dt;
    if (s.vy < 0 && s.y <= yTarget) break;
  }
  return s;
}
function refine(from, to, v, solver) {
  let aim = { x: to.x, y: to.y, z: to.z };
  let out = v;
  for (let i = 0; i < 3; i++) {
    const hit = flyTo(from, out, to.y);
    aim = { x: aim.x + (to.x - hit.x), y: to.y, z: aim.z + (to.z - hit.z) };
    out = solver(aim);
  }
  return out;
}
function solveApex(from, to, apexY) {
  let aimApex = apexY, v = null;
  for (let i = 0; i < 3; i++) {
    const a = aimApex;
    v = refine(from, to, solveApexRaw(from, to, a), (aim) => solveApexRaw(from, aim, a));
    aimApex += apexY - peakOf(from, v);
  }
  return v;
}
function peakOf(from, v) {
  let y = from.y, vy = v.vy;
  const dt = 1 / 120, k = 1 - CFG.DRAG * dt;
  let top = y;
  for (let i = 0; i < 600 && vy > 0; i++) {
    vy = vy * k - CFG.G * dt;
    y += vy * dt;
    if (y > top) top = y;
  }
  return top;
}
function heightAtNet(from, v) {
  if (Math.abs(v.vx) < 1e-6) return null;
  const t = -from.x / v.vx;
  if (t <= 0) return null;
  return from.y + v.vy * t - 0.5 * CFG.G * t * t;
}
function solveAttack(from, to, speed, margin = 0.14) {
  const dist = Math.hypot(to.x - from.x, to.z - from.z);
  let T = Math.max(0.12, dist / speed);
  const need = CFG.NET_H + CFG.BALL_R + margin;
  let v = solveTimeRaw(from, to, T);
  for (let i = 0; i < 60; i++) {
    const h = heightAtNet(from, v);
    if (h === null || h >= need) break;
    T *= 1.05;
    v = solveTimeRaw(from, to, T);
  }
  return refine(from, to, v, (aim) => solveTimeRaw(from, aim, T));
}
function solveLob(from, to, apexY, margin = 0.3) {
  let apex = apexY;
  let v = solveApex(from, to, apex);
  for (let i = 0; i < 30; i++) {
    const h = heightAtNet(from, v);
    if (h === null || h >= CFG.NET_H + CFG.BALL_R + margin) break;
    apex += 0.2;
    v = solveApex(from, to, apex);
  }
  return v;
}
function predictPath(state, maxT = 8) {
  const s = { x: state.x, y: state.y, z: state.z, vx: state.vx, vy: state.vy, vz: state.vz };
  const dt = CFG.DT;
  const path2 = [{ x: s.x, y: s.y, z: s.z, vy: s.vy, t: 0 }];
  let hitNet = false, hitTape = false;
  const n = Math.ceil(maxT / dt);
  for (let i = 1; i <= n; i++) {
    const ev = stepState(s, dt);
    if (ev === "net") hitNet = true;
    if (ev === "tape") hitTape = true;
    path2.push({ x: s.x, y: s.y, z: s.z, vy: s.vy, t: i * dt });
    if (ev === "ground") break;
  }
  const last = path2[path2.length - 1];
  return { path: path2, land: { x: last.x, z: last.z }, T: last.t, hitNet, hitTape };
}
function isIn(x, z) {
  return Math.abs(x) <= CFG.COURT_HL + CFG.BALL_R * 0.6 && Math.abs(z) <= CFG.COURT_HW + CFG.BALL_R * 0.6;
}

// src/physics/Ball.js
var Ball = class {
  constructor() {
    this.x = 0;
    this.y = 1;
    this.z = 0;
    this.vx = 0;
    this.vy = 0;
    this.vz = 0;
    this.px = 0;
    this.py = 1;
    this.pz = 0;
    this.live = false;
    this.held = true;
    this.shotId = 0;
    this.pred = null;
    this.predStep = 0;
    this.spin = { x: 0, y: 0, z: 0 };
    this.restTimer = 0;
    this.power = 0;
    this.rough = 0;
  }
  hold(x, y, z) {
    this.x = this.px = x;
    this.y = this.py = y;
    this.z = this.pz = z;
    this.vx = this.vy = this.vz = 0;
    this.held = true;
    this.live = false;
    this.pred = null;
  }
  launch(v, power = 0.3) {
    this.vx = v.vx;
    this.vy = v.vy;
    this.vz = v.vz;
    this.held = false;
    this.live = true;
    this.power = power;
    this.spin.x = v.vz * 0.9;
    this.spin.z = -v.vx * 0.9;
    this.spin.y = (v.vx + v.vz) * 0.15;
    this.repredict();
  }
  repredict() {
    this.shotId++;
    this.pred = predictPath(this);
    this.predStep = 0;
  }
  // amostra futura da trajetoria prevista, i passos a frente
  future(i) {
    if (!this.pred) return null;
    const p = this.pred.path;
    const k = this.predStep + i;
    return k < p.length ? p[k] : null;
  }
  remaining() {
    return this.pred ? this.pred.path.length - 1 - this.predStep : 0;
  }
  step(dt) {
    this.px = this.x;
    this.py = this.y;
    this.pz = this.z;
    if (this.held) return null;
    if (this.live) {
      const ev = stepState(this, dt);
      this.predStep++;
      if (ev === "net" || ev === "tape") {
        return ev;
      }
      if (ev === "ground") {
        this.live = false;
        this.vy = Math.abs(this.vy) * 0.22;
        this.vx *= 0.45;
        this.vz *= 0.45;
        this.restTimer = 0;
      }
      return ev;
    }
    this.vy -= CFG.G * dt;
    this.x += this.vx * dt;
    this.y += this.vy * dt;
    this.z += this.vz * dt;
    if (Math.abs(this.x) < CFG.BALL_R + 0.03 && this.y < CFG.NET_H) {
      this.x = Math.sign(this.x || 1) * (CFG.BALL_R + 0.03);
      this.vx *= -0.2;
    }
    if (this.y <= CFG.BALL_R) {
      this.y = CFG.BALL_R;
      this.vy = this.vy < -1.2 ? -this.vy * 0.25 : 0;
      this.vx *= 0.8;
      this.vz *= 0.8;
    }
    this.spin.x *= 0.96;
    this.spin.z *= 0.96;
    this.spin.y *= 0.96;
    return null;
  }
};

// src/core/Rules.js
var Rules = class {
  constructor(targetPoints = 12) {
    this.target = targetPoints;
    this.scores = [0, 0];
    this.servingTeam = 0;
    this.serverIdx = [0, 0];
    this.touches = 0;
    this.lastToucher = -1;
    this.lastTeam = -1;
    this.lastMove = null;
    this.rallyTouches = 0;
    this.crossings = 0;
    this.winner = -1;
    this.stats = [mkStats(), mkStats()];
  }
  startRally() {
    this.touches = 0;
    this.lastToucher = -1;
    this.lastTeam = -1;
    this.lastMove = null;
    this.rallyTouches = 0;
    this.crossings = 0;
  }
  // Registra um toque. Retorna null (valido) ou 'duplo' | 'quatro'.
  touch(athleteId, team, move = null) {
    let fault = null;
    if (team !== this.lastTeam) {
      if (this.lastTeam !== -1) this.crossings++;
      this.touches = 1;
    } else {
      if (this.lastToucher === athleteId) fault = "duplo";
      this.touches++;
      if (!fault && this.touches > 3) fault = "quatro";
    }
    this.lastToucher = athleteId;
    this.lastTeam = team;
    this.lastMove = move;
    this.rallyTouches++;
    return fault;
  }
  // Proximo numero de toque que o time `team` daria agora
  nextTouch(team) {
    return team === this.lastTeam ? this.touches + 1 : 1;
  }
  // Bola tocou a areia: quem ganha o ponto e por que.
  grounded(x, z) {
    const inside = isIn(x, z);
    const side = x < 0 ? 0 : 1;
    if (this.lastTeam === -1) return { winner: 1 - this.servingTeam, reason: "erro", inside };
    const recvFail = this.rallyTouches === 2 && this.lastTeam !== this.servingTeam;
    if (recvFail && (!inside || side === this.lastTeam)) return { winner: this.servingTeam, reason: "ace", inside };
    if (!inside) return { winner: 1 - this.lastTeam, reason: "fora", inside };
    if (side === this.lastTeam) return { winner: 1 - side, reason: this.rallyTouches === 1 ? "saque_rede" : "nao_passou", inside };
    const ace = this.rallyTouches === 1;
    return { winner: 1 - side, reason: ace ? "ace" : "ponto", inside };
  }
  award(winner, reason) {
    this.scores[winner]++;
    const st = this.stats[winner];
    st.pontos++;
    if (reason === "ace") st.aces++;
    if (reason === "ponto") {
      if (this.lastMove === "shark") st.sharks++;
      if (this.lastMove === "chapa") st.chapas++;
      if (this.lastMove === "pingo") st.pingos++;
      if (this.lastMove === "bloqueio") st.bloqueios++;
    }
    if (reason !== "ponto" && reason !== "ace") this.stats[1 - winner].erros++;
    if (winner !== this.servingTeam) {
      this.servingTeam = winner;
      this.serverIdx[winner] = 1 - this.serverIdx[winner];
    }
    if (this.isOver()) this.winner = this.scores[0] > this.scores[1] ? 0 : 1;
  }
  isOver() {
    const [a, b] = this.scores;
    return Math.max(a, b) >= this.target && Math.abs(a - b) >= 2;
  }
};
function mkStats() {
  return { pontos: 0, aces: 0, sharks: 0, chapas: 0, pingos: 0, bloqueios: 0, erros: 0 };
}

// src/entities/Athlete.js
var RECOVERY = {
  peito: 0.32,
  coxa: 0.3,
  pe: 0.36,
  cabeca: 0.3,
  chapa: 0.5,
  shark: 1.15,
  pingo: 0.38,
  chute: 0.42,
  cabecada: 0.36,
  saque: 0.55,
  whiff: 0.3,
  peixinho: 1.05
};
var BLOCK_NET = 1.6;
var CONTACT_OFFSET = {
  peito: 0.22,
  coxa: 0.34,
  pe: 0.55,
  cabeca: 0.18,
  chapa: 0.5,
  shark: 0.45,
  pingo: 0.45,
  chute: 0.55,
  cabecada: 0.2,
  saque: 0.5,
  peixinho: 0.55
};
var ATTACK_INTENTS = /* @__PURE__ */ new Set(["chapa", "shark"]);
function aimTarget(side, aim, intent, fromZ = 0) {
  const f = clamp(aim.x || 0, -1, 1), l = clamp(aim.z || 0, -1, 1);
  if (intent === "pingo") return { x: -side * (1.25 + Math.max(0, f) * 1.1), z: clamp(fromZ * 0.4 + l * 3.4, -3.9, 3.9) };
  if (intent === "levantar") return null;
  const tx = f >= 0 ? 6 + f * 2.1 : 6 + f * 3.2;
  return { x: -side * tx, z: l * 3.7 };
}
function pickPart(intent, h) {
  if (intent === "shark") return "shark";
  if (intent === "chapa") return h >= 3.5 ? "cabecada" : h >= 1.15 ? "chapa" : "chute";
  if (intent === "pingo") return h >= 1.15 ? "pingo" : "pe";
  if (h < 0.5) return "pe";
  if (h < 1.1) return "coxa";
  if (h < 1.6) return "peito";
  return "cabeca";
}
function angDiff(a, b) {
  let d = (b - a) % (Math.PI * 2);
  if (d > Math.PI) d -= Math.PI * 2;
  if (d < -Math.PI) d += Math.PI * 2;
  return d;
}
var Athlete = class {
  constructor(id, team, idx) {
    this.id = id;
    this.team = team;
    this.idx = idx;
    this.side = team === 0 ? -1 : 1;
    this.x = this.side * 5;
    this.z = idx ? 2 : -2;
    this.y = 0;
    this.vy = 0;
    this.px = this.x;
    this.pz = this.z;
    this.py = 0;
    this.vx = 0;
    this.vz = 0;
    this.facing = this.side < 0 ? Math.PI / 2 : -Math.PI / 2;
    this.input = { x: 0, z: 0, sprint: false, jump: false };
    this.action = null;
    this.cooldown = 0;
    this.speedMul = 1;
    this.isHuman = false;
    this.reachBonus = 0.18;
    this.tired = 0;
    this.emote = null;
    this.emoteT = 0;
    this.lastLabel = null;
    this.stepDist = 0;
  }
  get grounded() {
    return this.y <= 1e-4;
  }
  place(x, z) {
    this.x = this.px = x;
    this.z = this.pz = z;
    this.y = this.py = 0;
    this.vy = 0;
    this.vx = this.vz = 0;
    this.action = null;
    this.cooldown = 0;
    this.facing = this.side < 0 ? Math.PI / 2 : -Math.PI / 2;
    this.emote = null;
  }
  // Procura na trajetoria prevista o melhor ponto de contato dentro da janela de tempo.
  findContact(ball, intent, window, pull = 0.6, dive = false) {
    if (!ball.live || !ball.pred) return null;
    const I = INTENTS[intent];
    const n = Math.min(ball.remaining(), Math.round(window / CFG.DT));
    const yMax = dive ? 1.15 : I.yMax + (this.grounded ? 0 : this.y * 0.8);
    const fast = clamp((Math.hypot(ball.vx, ball.vz) - 9) / 9, 0, 1);
    const reach = CFG.REACH - fast * 0.3 + (this.isHuman ? this.reachBonus : 0) + (dive ? 1.2 : 0);
    let best = null;
    for (let i = 0; i <= n; i++) {
      const p = ball.future(i);
      if (p.x * this.side < 0.03) continue;
      if (p.y < I.yMin || p.y > yMax) continue;
      const t = i * CFG.DT;
      const d = Math.hypot(p.x - this.x, p.z - this.z);
      if (d > reach + Math.min(pull, (dive ? 8 : 5.5) * t)) continue;
      const score = Math.abs(p.y - I.ideal) + 0.5 * Math.max(0, d - 0.6) + t * 0.4;
      if (!best || score < best.score) best = { i, t, x: p.x, y: p.y, z: p.z, d, score };
    }
    return best;
  }
  // Tenta iniciar um toque. opts: { window, quality, pull, aim, powerMul }
  tryHit(match, intent, opts = {}) {
    if (this.action || this.cooldown > 0 || match.phase !== "RALLY") return false;
    const ball = match.ball;
    const window = opts.window ?? 0.4;
    let c = this.findContact(ball, intent, window, opts.pull ?? 0.6);
    let dive = false;
    if (!c && opts.dive && this.grounded && (intent === "dominar" || intent === "levantar")) {
      c = this.findContact(ball, intent, Math.max(window, 0.5), 0.9, true);
      dive = !!c;
    }
    if (!c) {
      this.action = { aid: match.nextAid(), intent, part: "whiff", whiff: true, done: true, tick: 0, contactTick: 0, t: 0, tContact: 0, dur: RECOVERY.whiff, hop: 0 };
      match.emit({ type: "whiff", athlete: this });
      return false;
    }
    const I = INTENTS[intent];
    const part = dive ? "peixinho" : pickPart(intent, c.y - 0);
    let quality = opts.quality;
    let label = null;
    if (quality === void 0) {
      const shark = intent === "shark";
      const tPerfect = shark ? 0.16 : 0.22;
      const qT = c.t <= tPerfect ? 1 : 1 - 0.55 * clamp((c.t - tPerfect) / Math.max(0.05, window - tPerfect), 0, 1);
      const dy = Math.abs(c.y - I.ideal);
      const qH = 1 - (shark ? 0.7 : 0.45) * clamp(dy / (shark ? 0.7 : 1.2), 0, 1);
      const qD = 1 - 0.4 * clamp((c.d - 0.95) / 0.8, 0, 1);
      quality = clamp(qT * qH * qD, 0.05, 1);
      label = quality > 0.84 ? "PERFEITO!" : c.t > tPerfect ? "CEDO" : c.y < I.ideal - 0.55 ? "TARDE" : quality > 0.6 ? "BOM" : "RUIM";
    }
    if (dive) {
      quality = clamp(quality * 0.5, 0.05, 0.6);
      label = "PEIXINHO!";
    }
    const off = CONTACT_OFFSET[part] ?? 0.3;
    let sx = c.x + this.side * off, sz = c.z;
    if (dive) {
      const dx = c.x - this.x, dz = c.z - this.z, dl = Math.hypot(dx, dz) || 1;
      sx = c.x - dx / dl * off;
      sz = c.z - dz / dl * off;
    }
    if (sx * this.side < 0.3) sx = this.side * 0.3;
    const hopBase = part === "shark" ? 1.75 : part === "chapa" ? 1.85 : part === "cabecada" ? 1.95 : part === "pingo" ? 1.9 : 9;
    let hop = clamp(c.y - hopBase, 0, part === "shark" ? 1.35 : 1.15);
    if (part === "shark") hop = Math.max(hop, 0.45);
    if (part === "cabeca" && c.y > 1.9) hop = clamp(c.y - 1.85, 0, 0.4);
    this.action = {
      aid: match.nextAid(),
      intent,
      part,
      whiff: false,
      done: false,
      tick: 0,
      contactTick: c.i,
      t: 0,
      tContact: c.t,
      sx,
      sz,
      hop,
      y0: this.y,
      shotId: ball.shotId,
      quality,
      label,
      aim: opts.aim || null,
      powerMul: opts.powerMul ?? 1,
      dur: c.t + (RECOVERY[part] ?? 0.35),
      charge: opts.charge ?? 0,
      feint: false,
      tgt: null,
      dive,
      slide: opts.slide ?? 6.5
    };
    this.retarget(c.z);
    this.lastLabel = label;
    match.emit({ type: "swing", athlete: this, part, label });
    return true;
  }
  // atualiza o alvo do golpe armado (o humano pode mudar a mira ate o contato)
  retarget(fromZ = this.z) {
    const a = this.action;
    if (!a || a.whiff) return;
    const aim = a.aim || {};
    if (a.feint) a.tgt = aimTarget(this.side, aim, "pingo", fromZ);
    else if (aim.target) a.tgt = { x: aim.target.x, z: aim.target.z };
    else if (ATTACK_INTENTS.has(a.intent) || a.intent === "pingo") a.tgt = aimTarget(this.side, aim, a.intent, fromZ);
    else a.tgt = null;
  }
  // finta: durante a armacao de chapa/shark troca o golpe por uma deixadinha logo apos a rede
  feint(match) {
    const a = this.action;
    if (!a || a.done || a.whiff || a.feint || !ATTACK_INTENTS.has(a.intent)) return false;
    const left = a.contactTick - a.tick;
    if (left < 3) return false;
    a.feint = true;
    a.intent = "pingo";
    if (a.part === "shark" || a.part === "chapa") a.part = "pingo";
    a.quality = clamp(a.quality * (left < 6 ? 0.78 : 0.92), 0.05, 1);
    a.charge = 0;
    if (a.aim && a.aim.target) a.aim = { x: 0, z: 0 };
    this.retarget(this.z);
    match.emit({ type: "feint", athlete: this });
    return true;
  }
  // bloqueio: pulo perto da rede com o peito/cabeca acima da fita
  startBlock(match) {
    this.vy = CFG.JUMP_V;
    this.y = 1e-3;
    this.action = {
      aid: match.nextAid(),
      intent: "bloqueio",
      part: "bloqueio",
      block: true,
      blocked: false,
      whiff: false,
      done: true,
      tick: 0,
      contactTick: 26,
      t: 0,
      tContact: 0.44,
      hop: 0,
      y0: 0,
      quality: 1,
      dur: 0.9
    };
    match.emit({ type: "jump", athlete: this });
  }
  startServe(match, target, apex) {
    this.action = {
      aid: match.nextAid(),
      intent: "saque",
      part: "saque",
      serve: true,
      whiff: false,
      done: false,
      tick: 0,
      contactTick: Math.round(0.45 / CFG.DT),
      t: 0,
      tContact: 0.45,
      sx: this.x,
      sz: this.z,
      hop: 0,
      y0: 0,
      shotId: match.ball.shotId,
      quality: 1,
      target,
      apex,
      dur: 0.45 + RECOVERY.saque
    };
  }
  update(dt, match) {
    this.px = this.x;
    this.pz = this.z;
    this.py = this.y;
    if (this.cooldown > 0) this.cooldown -= dt;
    if (this.tired > 0) this.tired = Math.max(0, this.tired - dt * 0.012);
    if (this.emote) this.emoteT += dt;
    const a = this.action;
    const netFace = this.side < 0 ? Math.PI / 2 : -Math.PI / 2;
    if (a) {
      if (!a.done) {
        const ball2 = match.ball;
        if (!match.replica && !a.serve && (ball2.shotId !== a.shotId || !ball2.live)) {
          a.whiff = true;
          a.done = true;
          a.dur = a.t + 0.25;
        } else if (a.tick >= a.contactTick) {
          a.done = true;
          if (a.serve) match.resolveServe(this);
          else match.resolveContact(this);
        } else {
          const dx = a.sx - this.x, dz = a.sz - this.z;
          const d = Math.hypot(dx, dz);
          const stepMax = (a.dive ? 10.5 : a.slide ?? 6.5) * dt;
          if (d > 1e-4) {
            const k = Math.min(1, stepMax / d);
            this.x += dx * k;
            this.z += dz * k;
          }
          const u = (a.tick + 1) / Math.max(1, a.contactTick);
          this.y = a.y0 + (a.hop - a.y0) * smooth(u);
          this.vy = 0;
        }
        if (a.hop > 0 && a.contactTick === 0) {
          this.y = Math.max(this.y, a.hop * 0.6);
        }
      }
      this.vx *= 0.8;
      this.vz *= 0.8;
      this.x += this.vx * dt;
      this.z += this.vz * dt;
      if (a.done) this._gravity(dt, match);
      this.facing += angDiff(this.facing, netFace) * Math.min(1, 14 * dt);
      a.tick++;
      a.t += dt;
      if (a.done && a.t >= a.dur && this.grounded) {
        this.action = null;
        this.cooldown = 0.04;
      }
      this._clamp();
      return;
    }
    let ix = this.input.x, iz = this.input.z;
    const il = Math.hypot(ix, iz);
    if (il > 1) {
      ix /= il;
      iz /= il;
    }
    const max = (this.input.sprint ? CFG.ATHLETE_SPRINT : CFG.ATHLETE_SPEED) * this.speedMul * (this.grounded ? 1 : 0.85);
    const tvx = ix * max, tvz = iz * max;
    const ddx = tvx - this.vx, ddz = tvz - this.vz;
    const dl = Math.hypot(ddx, ddz);
    const acc = CFG.ATHLETE_ACCEL * dt;
    if (dl <= acc) {
      this.vx = tvx;
      this.vz = tvz;
    } else {
      this.vx += ddx / dl * acc;
      this.vz += ddz / dl * acc;
    }
    this.x += this.vx * dt;
    this.z += this.vz * dt;
    const sp = Math.hypot(this.vx, this.vz);
    if (this.grounded) {
      this.stepDist += sp * dt;
      if (this.stepDist > 0.95) {
        this.stepDist = 0;
        match.emit({ type: "step", athlete: this, speed: sp });
      }
    }
    if (this.input.jump && this.grounded) {
      if (match.phase === "RALLY" && Math.abs(this.x) < BLOCK_NET) {
        this.startBlock(match);
        this._clamp();
        return;
      }
      this.vy = CFG.JUMP_V;
      this.y = 1e-3;
      match.emit({ type: "jump", athlete: this });
    }
    this._gravity(dt, match);
    let target = netFace;
    const ball = match.ball;
    if (sp > 2.2) target = Math.atan2(this.vx, this.vz);
    else if (ball && !ball.held && match.phase === "RALLY") {
      const bx = ball.x - this.x, bz = ball.z - this.z;
      if (bx * bx + bz * bz > 0.25) target = Math.atan2(bx, bz);
    }
    this.facing += angDiff(this.facing, target) * Math.min(1, 11 * dt);
    this._clamp();
  }
  _gravity(dt, match) {
    if (this.y > 0 || this.vy > 0) {
      this.vy -= CFG.G * dt;
      this.y += this.vy * dt;
      if (this.y <= 0) {
        const impact = -this.vy;
        this.y = 0;
        this.vy = 0;
        if (impact > 1.2) match.emit({ type: "land", athlete: this, impact, part: this.action ? this.action.part : null });
      }
    }
  }
  _clamp() {
    if (this.x * this.side < 0.35) {
      this.x = this.side * 0.35;
      if (this.vx * this.side < 0) this.vx = 0;
    }
    this.x = clamp(this.x, -CFG.BOUNDS_X, CFG.BOUNDS_X);
    this.z = clamp(this.z, -CFG.BOUNDS_Z, CFG.BOUNDS_Z);
  }
};

// src/ai/AIController.js
function interceptFor(a, ball, intent, react = 0.1) {
  const I = INTENTS[intent];
  const n = ball.remaining();
  const speed = CFG.ATHLETE_SPRINT * a.speedMul;
  let best = null;
  for (let i = 0; i <= n; i++) {
    const p = ball.future(i);
    if (p.x * a.side < 0.05) continue;
    if (p.y < I.yMin + 0.1 || p.y > I.yMax - 0.05) continue;
    const t = i * CFG.DT;
    const d = Math.hypot(p.x - a.x, p.z - a.z);
    const need = react + 0.12 + Math.max(0, d - CFG.REACH * 0.6) / speed;
    const slack = t - need;
    const score = slack >= 0 ? 10 - Math.abs(p.y - I.ideal) - (p.vy > 0 ? 0.8 : 0) : slack;
    if (!best || score > best.score) best = { i, t, x: p.x, y: p.y, z: p.z, d, slack, score, ok: slack >= 0 };
  }
  return best;
}
function computeTeamPlan(match, team) {
  const ball = match.ball, rules = match.rules;
  const plan = { active: false, leave: false, shotId: ball.shotId, takerId: -1, n: 0, intent: "dominar", point: null, stand: null, aim: null, t0: match.time, quality: 1 };
  if (!ball.live || !ball.pred || match.phase !== "RALLY") return plan;
  const side = team === 0 ? -1 : 1;
  const land = ball.pred.land;
  if (land.x * side <= 0) return plan;
  const n = rules.nextTouch(team);
  plan.n = n;
  if (n > 3) return plan;
  const mates = match.athletes.filter((p) => p.team === team);
  if (rules.lastTeam !== team) {
    const outBy = Math.max(Math.abs(land.x) - CFG.COURT_HL, Math.abs(land.z) - CFG.COURT_HW);
    const ctl2 = match.controllers[mates[0].id] || match.controllers[mates[1].id];
    const smart = ctl2 && ctl2.diff ? ctl2.diff.smart : 0.7;
    const margin = 0.25 + (1 - smart) * 0.9;
    if (outBy > margin) {
      plan.leave = true;
      return plan;
    }
  }
  let cands = mates;
  if (rules.lastTeam === team) cands = mates.filter((p) => p.id !== rules.lastToucher);
  let intent = n === 1 ? "dominar" : n === 2 ? "levantar" : "chapa";
  let best = null;
  for (const c of cands) {
    const ctl2 = match.controllers[c.id];
    const react = rules.lastTeam === team ? 0.05 : (ctl2 && ctl2.diff ? ctl2.diff.react : 0.25) + (rules.rallyTouches === 1 ? (ball.serveForce || 0) * 0.3 : 0);
    const it2 = interceptFor(c, ball, intent, react);
    if (!it2) continue;
    const cost = (it2.ok ? 0 : 100 - it2.slack * 10) + it2.d;
    if (!best || cost < best.cost - (best.c.isHuman ? -0.4 : 0)) best = { c, it: it2, cost, react };
  }
  if (!best) return plan;
  const taker = best.c;
  plan.active = true;
  plan.takerId = taker.id;
  plan.readyAt = match.time + best.react;
  const ctl = match.controllers[taker.id];
  if (ctl && ctl.chooseIntent) {
    const ch = ctl.chooseIntent(match, n, best.it);
    intent = ch.intent;
    plan.aim = ch.aim;
    plan.quality = ch.quality;
    plan.charge = ch.charge || 0;
    plan.feint = !!ch.feint;
    if (intent !== (n === 1 ? "dominar" : n === 2 ? "levantar" : "chapa")) {
      const it2 = interceptFor(taker, ball, intent, best.react);
      if (it2 && it2.ok) best.it = it2;
      else if (intent === "shark") intent = "chapa";
    }
  }
  plan.intent = intent;
  const it = best.it;
  plan.point = { x: it.x, y: it.y, z: it.z, t: match.time + it.t };
  const off = CONTACT_OFFSET[pickPart(intent, it.y)] ?? 0.3;
  plan.stand = { x: Math.max(0.35, it.x * side + off) * side, z: it.z };
  const other = mates.find((p) => p.id !== taker.id);
  if (n === 1) plan.support = { id: other.id, x: side * 2.6, z: clamp(it.z * 0.45, -2.5, 2.5) };
  else if (n === 2) {
    let sz = clamp(other.z * 0.8, -3.2, 3.2);
    if (Math.abs(sz - it.z) < 1.6) sz = clamp(it.z + (other.z >= it.z ? 1.6 : -1.6), -3.6, 3.6);
    plan.support = { id: other.id, x: side * 3.3, z: sz };
  } else plan.support = { id: other.id, x: side * 5.2, z: clamp(-it.z * 0.5, -2.5, 2.5) };
  return plan;
}
var AIController = class {
  constructor(athlete, diff, rng) {
    this.a = athlete;
    this.diff = diff;
    this.rng = rng;
    this.serveAt = 0;
    athlete.speedMul = diff.speed;
  }
  rollQuality() {
    const d = this.diff;
    return clamp(d.skill + (this.rng() - 0.6) * d.skillVar * 2, 0.15, 1);
  }
  // Decide o tipo de toque e a mira quando este atleta e o responsavel pela bola.
  chooseIntent(match, n, it) {
    const d = this.diff, r = this.rng, a = this.a;
    const quality = this.rollQuality();
    if (n === 1) return { intent: "dominar", aim: null, quality };
    if (n === 2) return { intent: "levantar", aim: null, quality, charge: r() < (d.quick || 0) && quality > 0.6 ? 1 : 0 };
    const opp = match.athletes.filter((p) => p.team !== a.team);
    let intent = "chapa";
    const roll = r();
    const peakOk = it && it.y >= 2.5;
    if (roll < d.shark && peakOk) intent = "shark";
    else if (roll < d.shark + d.pingo) intent = "pingo";
    let target = null;
    const charge = intent === "pingo" ? 0 : r() < (d.force ?? 0.4) ? 0.6 + r() * 0.4 : r() * 0.12;
    const prec = 0.45 + 0.75 * charge;
    if (intent !== "pingo") {
      let bestScore = -1;
      const mine = this.lastTargets || (this.lastTargets = []);
      for (let k = 0; k < 14; k++) {
        const tx = -a.side * r.range(3, 8.2), tz = r.range(-3.9, 3.9);
        let dmin = 99;
        for (const o of opp) dmin = Math.min(dmin, Math.hypot(o.x - tx, o.z - tz));
        let rep = 0;
        for (const p of mine) rep += Math.max(0, 1.6 - Math.abs(p - tz)) / 1.6;
        const score = dmin * (0.4 + d.smart * 0.6) + r() * (1.6 - d.smart) - rep * (d.memory || 0) * 0.9;
        if (score > bestScore) {
          bestScore = score;
          target = { x: tx, z: tz };
        }
      }
      mine.push(target.z);
      if (mine.length > 4) mine.shift();
      target.x += r.gauss() * d.aimErr * 0.6 * prec;
      target.z += r.gauss() * d.aimErr * 0.6 * prec;
    } else {
      target = { x: -a.side * r.range(1.2, 2.4), z: clamp((it ? it.z : 0) + r.range(-2.5, 2.5), -3.8, 3.8) };
    }
    const feint = intent !== "pingo" && r() < (d.feint || 0);
    return { intent, aim: { target }, quality, charge, feint };
  }
  update(dt, match) {
    const a = this.a, inp = a.input;
    inp.x = 0;
    inp.z = 0;
    inp.sprint = false;
    inp.jump = false;
    const act = a.action;
    if (act && !act.done && !act.whiff && !act.feint && (act.intent === "chapa" || act.intent === "shark") && this.dodgeAid !== act.aid) {
      const wall = match.athletes.find((o) => o.team !== a.team && Math.abs(o.x) < 1.5 && Math.abs(o.z - a.z) < 1.1 && (o.action ? o.action.block : true));
      if (wall && act.contactTick - act.tick >= 4) {
        this.dodgeAid = act.aid;
        if (this.rng() < (this.diff.smart ?? 0.6) * 0.85) {
          if (this.rng() < 0.32) {
            a.feint(match);
            this.feintAid = null;
          } else {
            const away = wall.z >= a.z ? -1 : 1;
            const tz = clamp(a.z + away * (2.2 + this.rng() * 1.4), -3.8, 3.8);
            const tx = act.tgt ? act.tgt.x : -a.side * 6;
            act.aim = { target: { x: tx, z: tz } };
            a.retarget();
          }
        }
      }
    }
    if (act && this.feintAid === act.aid && !act.done) {
      const left = act.contactTick - act.tick;
      if (left <= 8 + (this.rng() * 3 | 0)) {
        a.feint(match);
        this.feintAid = null;
      }
    }
    if (match.phase === "SERVE") {
      if (match.server === a && !a.action) {
        if (!this.serveAt) this.serveAt = match.time + CFG.SERVE_DELAY + this.rng() * 0.6;
        if (match.time >= this.serveAt) {
          this.serveAt = 0;
          const d = this.diff, r = this.rng;
          const f = r() < (d.serveForce ?? 0.3) ? 0.6 + r() * 0.4 : 0.05 + r() * 0.3;
          const k = 0.5 + f * 0.9;
          const rec = match.athletes.filter((o) => o.team !== a.team);
          let best = null, bs = -1;
          for (let i = 0; i < 10; i++) {
            const c = { x: -a.side * r.range(f > 0.5 ? 5.5 : 4.5, 8.4), z: r.range(-3.8, 3.8) };
            let dmin = 99;
            for (const o of rec) dmin = Math.min(dmin, Math.hypot(o.x - c.x, o.z - c.z));
            const sc = dmin * (d.smart ?? 0.6) + r() * (1.2 - (d.smart ?? 0.6));
            if (sc > bs) {
              bs = sc;
              best = c;
            }
          }
          const target = { x: best.x - a.side * r.gauss() * d.serveErr * 0.6 * k, z: best.z + r.gauss() * d.serveErr * 0.45 * k };
          match.serve(a, target, 4.4 - f * 1.9 + r.range(-0.2, 0.2), f);
        }
      }
      return;
    }
    this.serveAt = 0;
    if (match.phase !== "RALLY") return;
    const ball = match.ball;
    const plan = match.plans[a.team];
    if (plan.active && plan.takerId === a.id) {
      if (match.time < plan.readyAt) return;
      this.moveTo(plan.stand.x, plan.stand.z, true);
      if (!a.action && a.cooldown <= 0) {
        const c = a.findContact(ball, plan.intent, 0.2, 0.3);
        if (!c && (plan.intent === "dominar" || plan.intent === "levantar") && ball.remaining() * CFG.DT < 0.42 && this.diveShot !== ball.shotId) {
          this.diveShot = ball.shotId;
          if (this.rng() < (this.diff.dive ?? 0.6) && a.findContact(ball, plan.intent, 0.5, 0.9, true)) {
            a.tryHit(match, plan.intent, { window: 0.5, pull: 0.9, quality: plan.quality, dive: true });
            return;
          }
        }
        if (c) {
          const urgent = ball.remaining() * CFG.DT < 0.2;
          const due = match.time >= plan.point.t - 0.24;
          if (due && c.t <= 0.17 || urgent) {
            a.tryHit(match, plan.intent, { window: 0.2, pull: 0.3, quality: plan.quality, aim: plan.aim, powerMul: this.diff.power, charge: plan.charge || 0 });
            this.feintAid = plan.feint && a.action ? a.action.aid : null;
          }
        }
      }
      return;
    }
    if (plan.leave) return;
    if (plan.active && plan.support && plan.support.id === a.id) {
      this.moveTo(plan.support.x, plan.support.z, false);
      return;
    }
    if (this.blockDuty(match)) return;
    let hx = a.side * 5.3;
    let hz = (a.idx ? 2.2 : -2.2) + clamp(ball.z, -4, 4) * 0.22;
    const memW = this.diff.memory || 0;
    const mem = match.memory ? match.memory[1 - a.team] : null;
    if (memW > 0 && mem && mem.length >= 3) {
      let mz = 0, sh = 0, hot = 0;
      for (const e of mem) {
        mz += e.z;
        sh += e.short + e.feint;
        hot += Math.max(e.shark, e.force > 0.5 ? 1 : 0);
      }
      mz /= mem.length;
      sh /= mem.length;
      hot /= mem.length;
      hz += clamp(mz, -3.5, 3.5) * 0.35 * memW;
      hx = a.side * (5.3 - clamp(sh, 0, 0.6) * 2.4 * memW + hot * 1.1 * memW);
    }
    const atk = match.athletes.find((o) => o.team !== a.team && o.action && !o.action.done && !o.action.whiff && o.action.tgt && (o.action.intent === "chapa" || o.action.intent === "shark"));
    if (atk) {
      const at = atk.action;
      if (this.readAid !== at.aid) {
        this.readAid = at.aid;
        this.reads = this.rng() < (this.diff.read ?? 0.4);
      }
      const mate = match.mate(a);
      const mine = Math.hypot(at.tgt.x - a.x, at.tgt.z - a.z) <= Math.hypot(at.tgt.x - mate.x, at.tgt.z - mate.z);
      if (this.reads && mine) {
        this.moveTo(clamp(at.tgt.x * a.side, 3.2, 8) * a.side, at.tgt.z, true);
        return;
      }
    }
    this.moveTo(hx, hz, false);
  }
  // decide (uma vez por toque adversario) quem do time sobe para bloquear; o outro cobre o fundo
  blockDuty(match) {
    const a = this.a, ball = match.ball, R = match.rules, opp = 1 - a.team;
    if (!ball.live || R.lastTeam !== opp || ball.x * a.side > 0.2) return false;
    const n = R.nextTouch(opp);
    if (n < 2 || n > 3) return false;
    const D = match.defense || (match.defense = [{}, {}]);
    const dd = D[a.team];
    const key = ball.shotId;
    if (dd.key !== key) {
      dd.key = key;
      dd.id = -1;
      const mates = match.athletes.filter((p) => p.team === a.team);
      const humans = mates.some((p) => p.isHuman);
      const ai = mates.filter((p) => !p.isHuman && match.controllers[p.id] && match.controllers[p.id].blockDuty);
      const mem = match.memory ? match.memory[opp] : [];
      let hot = 0;
      for (const e of mem) hot += Math.max(e.shark, e.force > 0.5 ? 1 : 0);
      hot = mem.length >= 3 ? hot / mem.length : 0;
      const pBlock = Math.min(0.85, (this.diff.block ?? 0.4) * (humans ? 0.7 : 1) * (1 + 2.2 * hot * (this.diff.memory || 0)));
      if (ai.length && this.rng() < pBlock) {
        ai.sort((p, q) => Math.abs(p.x) - Math.abs(q.x));
        dd.id = ai[0].id;
      }
    }
    if (dd.id < 0) return false;
    const blocker = match.athletes[dd.id];
    if (blocker !== a) {
      if (!a.action) this.moveTo(a.side * 5.8, clamp(-blocker.z * 0.5, -2.6, 2.6), false);
      return !a.isHuman;
    }
    const op = match.plans[opp];
    let tz = ball.z;
    if (op && op.active && op.point && op.n === 3) tz = op.point.z;
    const atk = match.athletes.find((o) => o.team === opp && o.action && !o.action.done && !o.action.whiff && (o.action.intent === "chapa" || o.action.intent === "shark"));
    if (atk) {
      if (this.blockReadAid !== atk.action.aid) {
        this.blockReadAid = atk.action.aid;
        this.blockReads = this.rng() < Math.min(0.95, (this.diff.read ?? 0.4) + (atk.action.intent === "shark" ? 0.25 : 0));
      }
      tz = atk.z;
      if (this.blockReads && atk.action.tgt) tz = clamp(atk.z * 0.65 + atk.action.tgt.z * 0.35, -3.8, 3.8);
    }
    if (!a.action) this.moveTo(a.side * 0.75, tz, true);
    if (atk && !a.action && a.grounded && this.jumpAid !== atk.action.aid) {
      const act = atk.action;
      const travel = Math.abs(atk.x) / 15;
      const tApex = CFG.JUMP_V / CFG.G;
      const left = (act.contactTick - act.tick) * CFG.DT + travel;
      this.jumpErr = this.jumpErr ?? 0;
      if (this.jumpAidPlanned !== act.aid) {
        this.jumpAidPlanned = act.aid;
        this.jumpErr = this.rng.gauss() * (this.diff.blockErr ?? 0.07);
      }
      if (left <= tApex + this.jumpErr && Math.abs(a.x) < 1.6) {
        this.jumpAid = act.aid;
        a.input.jump = true;
      }
    }
    return true;
  }
  moveTo(x, z, sprint) {
    const a = this.a;
    const dx = x - a.x, dz = z - a.z;
    const d = Math.hypot(dx, dz);
    if (d < 0.06) return;
    const k = Math.min(1, d / 0.45) / d;
    a.input.x = dx * k;
    a.input.z = dz * k;
    a.input.sprint = sprint && d > 1.2;
  }
};

// src/core/GameState.js
var PART_NOISE = { pe: 0.45, coxa: 0.22, peito: 0.1, cabeca: 0.25, peixinho: 0.75 };
var MOVE_NAMES = {
  peito: "PEITO",
  coxa: "COXA",
  pe: "P\xC9",
  cabeca: "CABE\xC7A",
  chapa: "CHAPA!",
  shark: "SHARK ATTACK!",
  pingo: "PINGO",
  chute: "CHUTE",
  cabecada: "CABE\xC7ADA!",
  saque: "SAQUE",
  levantar: "LEVANTADA",
  devolver: "DEVOLU\xC7\xC3O",
  finta: "FINTA!",
  rapida: "LEVANTADA R\xC1PIDA",
  bloqueio: "BLOQUEIO!",
  peixinho: "PEIXINHO!"
};
var Match = class {
  constructor(opts = {}) {
    this.rng = makeRng(opts.seed ?? 12345);
    this.rules = new Rules(opts.target ?? 12);
    this.ball = new Ball();
    this.athletes = [new Athlete(0, 0, 0), new Athlete(1, 0, 1), new Athlete(2, 1, 0), new Athlete(3, 1, 1)];
    this.controllers = [null, null, null, null];
    this.plans = [{ active: false }, { active: false }];
    this.planShot = -1;
    this.events = [];
    this.time = 0;
    this.replica = !!opts.replica;
    this.memory = [[], []];
    this.aidSeq = 0;
    this.phase = "SERVE";
    this.phaseT = 0;
    this.server = null;
    this.lastPoint = null;
    this.rules.servingTeam = opts.firstServe ?? (this.rng() < 0.5 ? 0 : 1);
    this.setupServe();
  }
  // replica so gera eventos cosmeticos locais (passos, pulo, queda); o resto vem do servidor
  emit(e) {
    if (this.replica && e.type !== "step" && e.type !== "jump" && e.type !== "land") return;
    this.events.push(e);
  }
  nextAid() {
    return this.replica ? --this.aidSeq : ++this.aidSeq;
  }
  drain() {
    const e = this.events;
    this.events = [];
    return e;
  }
  mate(a) {
    return this.athletes[a.team * 2 + (1 - a.idx)];
  }
  setupServe() {
    const R = this.rules;
    R.startRally();
    const st = R.servingTeam;
    for (const a of this.athletes) {
      const s = a.side;
      if (a.team === st) {
        if (a.idx === R.serverIdx[st]) {
          a.place(s * 10.3, a.idx ? 1.2 : -1.2);
          this.server = a;
        } else a.place(s * 3.6, a.idx ? 2 : -2);
      } else a.place(s * 5.6, a.idx ? 2.2 : -2.2);
    }
    const sv = this.server;
    this.ball.hold(sv.x - sv.side * 0.6, 0.24, sv.z);
    this.phase = "SERVE";
    this.phaseT = 0;
    this.plans = [{ active: false }, { active: false }];
    this.emit({ type: "serveSetup", team: st, server: sv });
  }
  // target: {x,z} na quadra adversaria
  // force 0..1: saque forte e mais baixo e rapido (dificil de receber), mas pode ir na rede ou fora
  serve(a, target, apex, force = 0) {
    if (this.phase !== "SERVE" || a !== this.server || a.action) return false;
    a.startServe(this, target, apex);
    a.action.force = force;
    this.emit({ type: "swing", athlete: a, part: "saque" });
    return true;
  }
  resolveServe(a) {
    if (this.replica) return;
    const act = a.action, ball = this.ball;
    const from = { x: ball.x, y: ball.y, z: ball.z };
    const to = { x: act.target.x, y: CFG.BALL_R, z: act.target.z };
    const f = act.force || 0;
    const v = f > 0.45 ? solveAttack(from, to, 12 + 10 * f, 0.1 - f * 0.17) : solveLob(from, to, act.apex, 0.12 - f * 0.2);
    if (f > 0.45) {
      const e = this.rng.gauss() * f * 0.55;
      v.vy += e;
      v.vx *= 1 + this.rng.gauss() * 0.035 * f;
      v.vz += this.rng.gauss() * 0.35 * f;
    }
    this.rules.touch(a.id, a.team, "saque");
    ball.launch(v, 0.5 + f * 0.3);
    ball.rough = 0;
    ball.serveForce = f;
    this.phase = "RALLY";
    this.phaseT = 0;
    this.emit({ type: "touch", athlete: a, part: "saque", name: MOVE_NAMES.saque, quality: 1, power: 0.5, pos: from, n: 0 });
  }
  resolveContact(a) {
    if (this.replica) return;
    const act = a.action, ball = this.ball, s = a.side, r = this.rng;
    const from = { x: ball.x, y: ball.y, z: ball.z };
    const speedIn = Math.hypot(ball.vx, ball.vy, ball.vz);
    const part = act.part;
    let intent = act.intent;
    const fault = this.rules.touch(a.id, a.team, intent);
    const n = this.rules.touches;
    if ((intent === "dominar" || intent === "levantar") && n >= 3) intent = "devolver";
    const rough = this.rules.lastTeam === a.team ? ball.rough : 0;
    let q = clamp(act.quality * (1 - 0.42 * rough), 0.05, 1);
    if (part === "shark") {
      q = clamp(q * (1 - 0.6 * a.tired), 0.05, 1);
      a.tired = Math.min(1, a.tired + 0.27 + 0.15 * (act.charge || 0));
    }
    const hard = Math.max(clamp((speedIn - 11) / 9, 0, 1), this.rules.rallyTouches === 2 ? (ball.serveForce || 0) * 0.85 : 0);
    const mate = this.mate(a);
    const aim = act.aim || {};
    const aimF = aim.x || 0, aimZ = aim.z || 0;
    const charge = act.charge > 0.15 ? clamp((act.charge - 0.15) / 0.85, 0, 1) : 0;
    const tgt = act.tgt;
    const jit = (sigma) => r.gauss() * sigma * 0.55;
    let v, power = 0.25, name = MOVE_NAMES[part];
    if (intent === "dominar" && this.rules.rallyTouches === 2 && (ball.serveForce || 0) > 0.45 && r() < hard * (1.15 - q) * 1.1) {
      v = { vx: ball.vx * r.range(0.15, 0.55), vy: r.range(1.2, 4.2), vz: ball.vz * 0.3 + (r() < 0.5 ? -1 : 1) * r.range(2.5, 5.5) };
      name = "ESPIRROU!";
    } else if (intent === "dominar") {
      const noise = 0.12 + (PART_NOISE[part] ?? 0.2) + (1 - q) * 1.5 + hard * 4.5 * (1.3 - q);
      let tx = clamp(Math.abs(from.x) * 0.35 + 1.9, 2.2, 4.2) + jit(noise);
      let tz = from.z * 0.45 + mate.z * 0.15 + aimZ * 1.2 + jit(noise);
      tx = Math.max(0.7, tx);
      const apex = Math.max(from.y + 1.6, 3.7 + jit(noise) * 0.4);
      v = solveApex(from, { x: s * tx, y: 1.25, z: tz }, apex);
    } else if (intent === "levantar") {
      const quick = charge > 0.5;
      const noise = (0.08 + (PART_NOISE[part] ?? 0.2) * 0.6 + (1 - q) * 1.25 + hard * 0.5) * (quick ? 1.3 : 1);
      const tx = Math.max(0.55, (quick ? 0.95 : 1.25) - aimF * 0.3 + jit(noise) * 0.7);
      const tz = clamp(lerp(from.z, mate.z, 0.55) + aimZ * 1.6 + jit(noise), -3.6, 3.6);
      const apex = quick ? Math.max(from.y + 0.9, 3.45 + jit(noise) * 0.3) : Math.max(from.y + 1.8, 4.7 + jit(noise) * 0.5);
      v = solveApex(from, { x: s * tx, y: 2.75, z: tz }, apex);
      name = quick ? MOVE_NAMES.rapida : MOVE_NAMES.levantar;
      power = 0.3;
    } else if (intent === "devolver") {
      const noise = 0.5 + (1 - q) * 1.4;
      const tx = r.range(4.2, 7.2) + aimF * 1.5 + jit(noise), tz = clamp(r.range(-3, 3) + aimZ * 2, -3.6, 3.6) + jit(noise);
      v = solveLob(from, { x: -s * tx, y: CFG.BALL_R, z: tz }, Math.max(from.y + 1.5, 4), 0.35);
      name = MOVE_NAMES.devolver;
      power = 0.35;
    } else if (intent === "pingo") {
      const noise = 0.2 + (1 - q) * 1;
      let tx, tz;
      if (tgt) {
        tx = Math.abs(tgt.x);
        tz = tgt.z;
      } else if (aim.target) {
        tx = Math.abs(aim.target.x);
        tz = aim.target.z;
      } else {
        tx = 1.5 + Math.max(0, aimF) * 1.3;
        tz = clamp(from.z + aimZ * 3.2, -3.9, 3.9);
      }
      tx = Math.max(0.5, tx + jit(noise));
      tz += jit(noise);
      const apex = Math.max(from.y + 0.06, CFG.NET_H + (act.feint ? 0.24 : 0.34)) + Math.abs(from.x) * 0.1;
      v = solveLob(from, { x: -s * tx, y: CFG.BALL_R, z: tz }, apex, 0.05 + q * 0.06);
      name = act.feint ? MOVE_NAMES.finta : MOVE_NAMES.pingo;
      power = 0.3;
    } else {
      let speed, noise, margin = 0.07;
      if (part === "shark") {
        speed = 12.5 + 9 * q;
        noise = (0.35 + (1 - q) * 3) * (1 + 0.45 * charge + 0.8 * charge * (1 - q));
        power = 1;
        if (q < 0.3) margin = -0.05;
      } else if (part === "chapa") {
        speed = 13 + 3.5 * q;
        noise = 0.3 + (1 - q) * 2.2;
        power = 0.8;
      } else if (part === "cabecada") {
        speed = 11 + 2 * q;
        noise = 0.4 + (1 - q) * 1.5;
        power = 0.55;
      } else {
        speed = 9.5 + 2 * q;
        noise = 0.6 + (1 - q) * 1.6;
        power = 0.45;
      }
      speed *= act.powerMul * clamp(1.18 - Math.abs(from.x) / 9 * 0.7, 0.55, 1);
      speed *= 0.96 + (part === "shark" ? 0.1 : 0.22) * charge;
      noise *= 0.55 + 1 * charge;
      margin -= 0.16 * charge * (1 - q);
      let tx, tz;
      if (tgt) {
        tx = Math.abs(tgt.x);
        tz = tgt.z;
      } else if (aim.target) {
        tx = Math.abs(aim.target.x);
        tz = aim.target.z;
      } else {
        tx = 5.8 + aimF * 2.3;
        tz = aimZ * 3.5;
      }
      tx = Math.max(0.8, tx + jit(noise));
      tz += jit(noise);
      v = solveAttack(from, { x: -s * tx, y: CFG.BALL_R, z: tz }, speed, margin);
    }
    ball.launch(v, power);
    ball.rough = intent === "dominar" || intent === "levantar" ? clamp((1 - q) * 1.15 + (act.dive ? 0.25 : 0) + hard * 0.2, 0, 1) : 0;
    if (act.dive) name = MOVE_NAMES.peixinho;
    if (n === 3 || intent === "chapa" || intent === "shark" || intent === "pingo") {
      const mem = this.memory[a.team];
      mem.push({ z: tgt ? tgt.z : 0, short: intent === "pingo" ? 1 : 0, feint: act.feint ? 1 : 0, force: charge, shark: part === "shark" ? 1 : 0 });
      if (mem.length > 10) mem.shift();
    }
    this.emit({ type: "touch", athlete: a, part, intent, name, quality: q, label: act.label, power, pos: from, n, speedIn, feint: !!act.feint, charge });
    if (fault) this.endPoint(1 - a.team, fault);
  }
  // bola atacada pelo adversario passando pelo corpo de quem pulou para bloquear
  checkBlocks() {
    const ball = this.ball, R = this.rules, r = this.rng;
    for (const a of this.athletes) {
      const act = a.action;
      if (!act || !act.block || act.blocked || a.team === R.lastTeam || R.lastTeam < 0) continue;
      if (ball.vx * a.side <= 0) continue;
      const bx = ball.x * a.side;
      if (bx < 0.01 || bx > 0.85) continue;
      const dz = Math.abs(ball.z - a.z);
      const lo = a.y + 1.3, hi = a.y + 2.25;
      if (dz > 0.46 || ball.y < lo || ball.y > hi) continue;
      act.blocked = true;
      const s = a.side, from = { x: ball.x, y: ball.y, z: ball.z };
      const speedIn = Math.hypot(ball.vx, ball.vy, ball.vz);
      const centered = 1 - dz / 0.46;
      const apexK = 1 - clamp(Math.abs(a.vy) / CFG.JUMP_V, 0, 1);
      const fast = clamp((speedIn - 14) / 8, 0, 1);
      const q = clamp(centered * 0.55 + apexK * 0.4 - fast * 0.1 + (R.lastMove === "shark" ? 0.12 : 0) + r.gauss() * 0.1, 0, 1);
      const fault = R.touch(a.id, a.team, "bloqueio");
      let v, result;
      if (q > 0.74) {
        result = "forte";
        v = solveAttack(from, { x: -s * r.range(1.3, 5.2), y: CFG.BALL_R, z: clamp(ball.z + r.range(-2.6, 2.6), -3.8, 3.8) }, clamp(speedIn * 0.42, 6.5, 10), 0.03);
      } else if (q > 0.55) {
        result = "amortece";
        v = solveApex(from, { x: s * r.range(1.6, 2.8), y: 1.2, z: clamp(a.z * 0.6 + r.range(-1, 1), -3.4, 3.4) }, Math.max(from.y + 0.9, 3.5));
      } else {
        result = "desvio";
        v = { vx: ball.vx * r.range(0.45, 0.75), vy: r.range(-1.5, 2.6), vz: ball.vz * 0.4 + r.range(-3, 3) };
      }
      ball.launch(v, result === "forte" ? 0.7 : 0.3);
      ball.rough = result === "forte" ? 0 : result === "amortece" ? 0.15 : 0.6;
      this.emit({ type: "touch", athlete: a, part: "bloqueio", intent: "bloqueio", name: MOVE_NAMES.bloqueio, quality: q, label: result === "forte" ? "BLOQUEIO!" : null, power: result === "forte" ? 0.7 : 0.3, pos: from, n: R.touches, speedIn, block: result });
      if (fault) this.endPoint(1 - a.team, fault);
      return;
    }
  }
  endPoint(winner, reason) {
    if (this.phase !== "RALLY") return;
    const R = this.rules;
    const move = R.lastMove;
    R.award(winner, reason);
    this.phase = "POINT";
    this.phaseT = 0;
    this.lastPoint = { winner, reason, move };
    for (const a of this.athletes) {
      a.emote = a.team === winner ? "comemora" : "lamenta";
      a.emoteT = 0;
      a.input.x = a.input.z = 0;
    }
    this.emit({ type: "point", winner, reason, move, scores: R.scores.slice() });
  }
  update(dt) {
    this.time += dt;
    this.phaseT += dt;
    const ball = this.ball;
    if (this.planShot !== ball.shotId || this.phase === "RALLY" && !this.plans[0].shotId) {
      this.planShot = ball.shotId;
      this.plans[0] = computeTeamPlan(this, 0);
      this.plans[1] = computeTeamPlan(this, 1);
    }
    if (this.phase === "SERVE" || this.phase === "RALLY") {
      for (const c of this.controllers) if (c) c.update(dt, this);
    } else for (const a of this.athletes) {
      a.input.x = 0;
      a.input.z = 0;
      a.input.jump = false;
    }
    for (const a of this.athletes) a.update(dt, this);
    for (let t = 0; t < 2; t++) {
      const p = this.athletes[t * 2], q = this.athletes[t * 2 + 1];
      const dx = q.x - p.x, dz = q.z - p.z, d = Math.hypot(dx, dz);
      if (d < 0.7 && d > 1e-4 && !p.action && !q.action) {
        const k = (0.7 - d) / d * 0.5;
        p.x -= dx * k;
        p.z -= dz * k;
        q.x += dx * k;
        q.z += dz * k;
      }
    }
    if (this.phase === "SERVE" && this.server && !this.server.action) {
      ball.hold(this.server.x - this.server.side * 0.6, 0.24, this.server.z);
    }
    if (this.phase === "RALLY" && ball.live && !this.replica) this.checkBlocks();
    const ev = ball.step(dt);
    if (ev === "net" || ev === "tape") this.emit({ type: ev, pos: { x: ball.x, y: ball.y, z: ball.z } });
    if (ev === "ground") {
      this.emit({ type: "ground", pos: { x: ball.x, y: ball.y, z: ball.z }, speed: Math.hypot(ball.vx, ball.vz) });
      if (this.phase === "RALLY" && !this.replica) {
        const res = this.rules.grounded(ball.x, ball.z);
        this.endPoint(res.winner, res.reason);
      }
    }
    if (!this.replica && this.phase === "POINT" && this.phaseT >= CFG.POINT_PAUSE) {
      if (this.rules.winner >= 0) {
        this.phase = "MATCH_END";
        this.phaseT = 0;
        this.emit({ type: "matchEnd", winner: this.rules.winner });
      } else this.setupServe();
    }
  }
};

// src/core/PlayerController.js
var ASSIST = {
  alta: { label: "Alta", pull: 0.75, slide: 6.5, reach: 0.18 },
  media: { label: "M\xE9dia", pull: 0.5, slide: 4.6, reach: 0.1 },
  off: { label: "Desligada", pull: 0.22, slide: 2.4, reach: 0 }
};
var PlayerController = class {
  constructor(athlete, input, rng) {
    this.a = athlete;
    this.input = input;
    this.rng = rng;
    athlete.isHuman = true;
    this.diff = { react: 0, smart: 1 };
    this.setAssist("alta");
    this.serve = { active: false, charging: false, power: 0, dir: 1, aimZ: 0 };
  }
  setAssist(k) {
    this.assist = ASSIST[k] || ASSIST.alta;
    this.a.reachBonus = this.assist.reach;
  }
  serveTarget(power = this.serve.power) {
    return { x: -this.a.side * (2.2 + power * 7.4), z: this.serve.aimZ };
  }
  update(dt, match) {
    const a = this.a, I = this.input, inp = a.input, s = this.serve;
    if (match.phase === "SERVE" && match.server !== a) {
      s.active = false;
      s.charging = false;
      inp.x = I.mx;
      inp.z = I.mz;
      inp.sprint = I.sprint;
      inp.jump = false;
      I.clearPressed();
      return;
    }
    if (match.phase === "SERVE") {
      inp.x = 0;
      inp.z = 0;
      inp.jump = false;
      inp.sprint = false;
      if (!a.action) {
        if (!s.active) {
          s.active = true;
          s.charging = false;
          s.power = 0;
          s.aimZ = clamp(s.aimZ, -3.6, 3.6);
          I.clearPressed();
          I.held.dominar = false;
        }
        s.aimZ = clamp(s.aimZ + I.mz * dt * 4.2, -4.2, 4.2);
        if (I.held.dominar) {
          if (!s.charging) {
            s.charging = true;
            s.power = 0;
            s.dir = 1;
          }
          s.power += s.dir * dt / 1;
          if (s.power >= 1) {
            s.power = 1;
            s.dir = -1;
          }
          if (s.power <= 0) {
            s.power = 0;
            s.dir = 1;
          }
        } else if (s.charging) {
          s.charging = false;
          const t = this.serveTarget();
          const force = clamp((s.power - 0.55) / 0.4, 0, 1);
          const noise = 0.22 + force * force * 0.55;
          t.x += this.rng.gauss() * noise;
          t.z += this.rng.gauss() * noise;
          match.serve(a, t, 4.6 - force * 1.9, force);
          s.active = false;
        }
      } else s.active = false;
      I.clearPressed();
      return;
    }
    s.active = false;
    s.charging = false;
    inp.x = I.mx;
    inp.z = I.mz;
    inp.sprint = I.sprint;
    inp.jump = I.consume("jump");
    if (match.phase !== "RALLY") return;
    const act = a.action;
    if (act && !act.done && !act.whiff) {
      act.aim = { x: I.mx * -a.side, z: I.mz };
      a.retarget(match.ball.z);
      const key = act.feint ? null : act.intent;
      if (key && I.held[key] && (key === "chapa" || key === "shark" || key === "levantar")) act.charge = Math.min(1, (act.charge || 0) + dt / 0.42);
      if (I.consume("pingo")) a.feint(match);
      I.clearPressed();
      return;
    }
    const R = match.rules;
    const wouldDouble = R.lastTeam === a.team && R.lastToucher === a.id;
    for (const intent of ["shark", "chapa", "pingo", "levantar", "dominar"]) {
      if (I.consume(intent)) {
        if (wouldDouble) break;
        a.tryHit(match, intent, { window: (intent === "shark" ? 0.34 : 0.42) + (this.lag || 0), pull: this.assist.pull, slide: this.assist.slide, aim: { x: I.mx * -a.side, z: I.mz }, dive: !!I.sprint });
        break;
      }
    }
  }
};

// src/ai/Difficulty.js
var DIFFICULTY = {
  facil: { label: "F\xE1cil", react: 0.42, skill: 0.6, skillVar: 0.3, aimErr: 1.5, power: 0.68, shark: 0.06, pingo: 0.12, speed: 0.9, smart: 0.35, serveErr: 1.4, force: 0.3, feint: 0.03, quick: 0.04, read: 0.15, block: 0.1, blockErr: 0.12, dive: 0.1, serveForce: 0.1 },
  medio: { label: "M\xE9dio", react: 0.29, skill: 0.76, skillVar: 0.22, aimErr: 0.9, power: 0.8, shark: 0.16, pingo: 0.15, speed: 0.97, smart: 0.65, serveErr: 1, force: 0.45, feint: 0.07, quick: 0.1, read: 0.4, block: 0.2, blockErr: 0.085, dive: 0.22, serveForce: 0.3 },
  dificil: { label: "Dif\xEDcil", react: 0.19, skill: 0.9, skillVar: 0.12, aimErr: 0.5, power: 0.95, shark: 0.28, pingo: 0.18, speed: 1.03, smart: 0.92, serveErr: 0.7, force: 0.55, feint: 0.09, quick: 0.12, read: 0.4, block: 0.2, blockErr: 0.065, dive: 0.2, serveForce: 0.45 }
};
var STYLES = {
  agressivo: { label: "Agressivo", mul: { shark: 1.8, pingo: 0.5, feint: 0.5, quick: 0.8, block: 0.8 }, add: { force: 0.3, serveForce: 0.25 } },
  colocador: { label: "Colocador", mul: { shark: 0.7, pingo: 1.7, feint: 1.1, aimErr: 0.55 }, add: { force: -0.15 } },
  fintador: { label: "Fintador", mul: { feint: 2.6, quick: 2.6, pingo: 1.3, shark: 0.9 }, add: { force: -0.1 } },
  muralha: { label: "Muralha", mul: { block: 1.9, read: 1.3, dive: 1.25, shark: 0.75 }, add: { force: -0.1 } }
};
var MEMORY = { facil: 0, medio: 0.2, dificil: 0.45 };
function cpuProfile(diffKey, styleKey) {
  const base = DIFFICULTY[diffKey] || DIFFICULTY.medio, st = STYLES[styleKey];
  const p = Object.assign({}, base, { memory: MEMORY[diffKey] ?? 0.35, style: styleKey || null, styleLabel: st ? st.label : "" });
  if (st) {
    for (const k in st.mul) p[k] = (p[k] ?? 0) * st.mul[k];
    for (const k in st.add) p[k] = Math.min(0.95, Math.max(0.02, (p[k] ?? 0) + st.add[k]));
    for (const k of ["shark", "pingo", "feint", "quick", "block", "read", "dive"]) p[k] = Math.min(0.95, p[k]);
  }
  return p;
}
function pickStyles(rng) {
  const ks = Object.keys(STYLES);
  const a = ks[Math.floor(rng() * ks.length)];
  let b = a;
  while (b === a) b = ks[Math.floor(rng() * ks.length)];
  return [a, b];
}
var PARTNER = { label: "Parceiro", react: 0.2, skill: 0.9, skillVar: 0.1, aimErr: 0.7, power: 0.88, shark: 0.14, pingo: 0.12, speed: 1, smart: 0.8, serveErr: 0.8, force: 0.45, feint: 0.08, quick: 0.08, read: 0.5, block: 0.3, blockErr: 0.06, dive: 0.5, serveForce: 0.25 };

// src/net/protocol.js
var PROTO = 3;
var HELD_BITS = { dominar: 1, levantar: 2, chapa: 4, shark: 8 };
var PHASES = ["SERVE", "RALLY", "POINT", "MATCH_END"];
var CHAT = ["Boa!", "Minha!", "Sua!", "De novo!"];
var r3 = (v) => Math.round(v * 1e3) / 1e3;
function packAction(a) {
  if (!a) return 0;
  return [
    a.aid,
    a.intent,
    a.part,
    a.whiff ? 1 : 0,
    a.done ? 1 : 0,
    a.tick,
    a.contactTick,
    r3(a.tContact),
    r3(a.sx || 0),
    r3(a.sz || 0),
    r3(a.hop || 0),
    r3(a.y0 || 0),
    a.shotId || 0,
    r3(a.quality || 0),
    a.label || 0,
    r3(a.dur),
    a.serve ? 1 : 0,
    r3(a.charge || 0),
    a.feint ? 1 : 0,
    a.tgt ? r3(a.tgt.x) : 0,
    a.tgt ? r3(a.tgt.z) : 0,
    a.tgt ? 1 : 0,
    a.block ? 1 : 0,
    a.dive ? 1 : 0
  ];
}
function packState(m) {
  const R = m.rules, b = m.ball;
  const sc = m.server && m.controllers[m.server.id] && m.controllers[m.server.id].serve;
  return {
    ph: PHASES.indexOf(m.phase),
    pt: r3(m.phaseT),
    sv: m.server ? m.server.id : -1,
    r: [R.scores[0], R.scores[1], R.servingTeam, R.serverIdx[0], R.serverIdx[1], R.touches, R.lastToucher, R.lastTeam, R.lastMove || 0, R.rallyTouches, R.winner],
    b: [r3(b.x), r3(b.y), r3(b.z), r3(b.vx), r3(b.vy), r3(b.vz), b.live ? 1 : 0, b.held ? 1 : 0, b.shotId, r3(b.power), b.predStep],
    p: m.athletes.map((a) => [
      r3(a.x),
      r3(a.z),
      r3(a.y),
      r3(a.vx),
      r3(a.vz),
      r3(a.vy),
      r3(a.facing),
      r3(a.input.x),
      r3(a.input.z),
      a.input.sprint ? 1 : 0,
      r3(Math.max(0, a.cooldown)),
      a.emote || 0,
      packAction(a.action)
    ]),
    sc: sc && sc.active ? [sc.charging ? 1 : 0, r3(sc.power), r3(sc.aimZ)] : 0
  };
}
function packEvent(e) {
  const o = {};
  for (const k in e) {
    const v = e[k];
    if (k === "athlete" || k === "server") o[k] = v ? v.id : -1;
    else if (k === "pos") o.pos = { x: r3(v.x), y: r3(v.y), z: r3(v.z) };
    else if (typeof v === "number") o[k] = r3(v);
    else o[k] = v;
  }
  return o;
}
var cleanName = (s) => String(s || "").replace(/[^\p{L}\p{N} _.\-]/gu, "").trim().slice(0, 12) || "Jogador";
var CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
var cleanCode = (s) => String(s || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 4);

// server/rooms.js
var ACTIONS = ["dominar", "levantar", "chapa", "shark", "pingo", "jump"];
var LOCAL_EVENTS = /* @__PURE__ */ new Set(["step", "jump", "land"]);
var LIMITS = { rooms: 300, perRoom: 4, msgsPerSec: 240, offLobbyMs: 2e4, offPlayMs: 15e3, emptyMs: 3e4, snapEvery: 2 };
var num = (v, lo, hi) => typeof v === "number" && isFinite(v) ? clamp(v, lo, hi) : 0;
var NetSource = class {
  constructor() {
    this.reset();
    this.lag = 0;
  }
  reset() {
    this.mx = 0;
    this.mz = 0;
    this.sprint = false;
    this.held = {};
    this.pressed = {};
  }
  consume(a) {
    if (this.pressed[a]) {
      this.pressed[a] = false;
      return true;
    }
    return false;
  }
  clearPressed() {
    this.pressed = {};
  }
};
var Player = class {
  constructor(id, name) {
    this.id = id;
    this.name = name;
    this.team = 0;
    this.ready = false;
    this.ping = 0;
    this.token = Math.random().toString(36).slice(2, 12) + Math.random().toString(36).slice(2, 8);
    this.conn = null;
    this.offAt = 0;
    this.source = new NetSource();
    this.ack = 0;
    this.athlete = null;
    this.cpu = false;
    this.chatAt = -1e9;
  }
  get on() {
    return !!this.conn;
  }
  send(str) {
    if (this.conn) this.conn.send(str);
  }
};
var Room = class {
  constructor(hub2, code) {
    this.hub = hub2;
    this.code = code;
    this.players = [];
    this.nextId = 1;
    this.hostId = 0;
    this.opts = { points: 12, diff: "medio" };
    this.state = "lobby";
    this.match = null;
    this.tick = 0;
    this.emptyAt = 0;
    this.endAt = 0;
    this.pending = [];
  }
  humans(team) {
    return this.players.filter((p) => p.team === team);
  }
  broadcast(obj) {
    const s = JSON.stringify(obj);
    for (const p of this.players) p.send(s);
  }
  lobbyMsg() {
    return {
      t: "lobby",
      code: this.code,
      host: this.hostId,
      state: this.state,
      opts: this.opts,
      players: this.players.map((p) => ({ id: p.id, name: p.name, team: p.team, ready: p.ready, ping: p.ping, on: p.on }))
    };
  }
  sendLobby() {
    this.broadcast(this.lobbyMsg());
  }
  fixHost() {
    const h = this.players.find((p) => p.id === this.hostId);
    if (!h || !h.on) {
      const n = this.players.find((p) => p.on);
      if (n) this.hostId = n.id;
    }
  }
  add(name) {
    const p = new Player(this.nextId++, name);
    p.team = this.humans(0).length <= this.humans(1).length ? 0 : 1;
    this.players.push(p);
    if (!this.hostId) this.hostId = p.id;
    return p;
  }
  remove(p) {
    if (this.state === "play" && p.athlete) this.toCpu(p);
    this.players = this.players.filter((q) => q !== p);
    this.fixHost();
    if (this.players.length) this.sendLobby();
  }
  // ---------- partida ----------
  startMsg(p) {
    return {
      t: "start",
      k: this.tick,
      target: this.opts.points,
      you: p.athlete ? p.athlete.id : -1,
      slots: this.players.filter((q) => q.athlete && !q.cpu).map((q) => ({ a: q.athlete.id, pid: q.id, name: q.name, team: q.team }))
    };
  }
  start() {
    const seed = Date.now() ^ Math.random() * 1e9 | 0;
    const m = this.match = new Match({ seed, target: this.opts.points });
    this.rng = makeRng(seed + 5);
    const diff = DIFFICULTY[this.opts.diff] || DIFFICULTY.medio;
    for (const p of this.players) {
      p.athlete = null;
      p.cpu = false;
      p.ack = 0;
      p.source.reset();
    }
    for (let team = 0; team < 2; team++) {
      const hs = this.humans(team).slice(0, 2);
      hs.forEach((p, idx) => {
        const a = m.athletes[team * 2 + idx];
        p.athlete = a;
        m.controllers[a.id] = new PlayerController(a, p.source, this.rng);
        m.controllers[a.id].setAssist("media");
      });
      const styles = pickStyles(this.rng);
      for (let idx = hs.length; idx < 2; idx++) {
        const a = m.athletes[team * 2 + idx];
        m.controllers[a.id] = new AIController(a, hs.length ? PARTNER : cpuProfile(this.opts.diff, styles[idx]), this.rng);
      }
    }
    this.state = "play";
    this.tick = 0;
    this.pending = [];
    for (const p of this.players) p.send(JSON.stringify(this.startMsg(p)));
    this.sendLobby();
    this.snapshot();
  }
  toCpu(p) {
    const m = this.match, a = p.athlete;
    if (!m || !a || p.cpu) return;
    p.cpu = true;
    const diff = DIFFICULTY[this.opts.diff] || DIFFICULTY.medio;
    const mate = m.mate(a);
    const mateHuman = this.players.some((q) => q !== p && q.athlete === mate && !q.cpu);
    a.isHuman = false;
    a.input.x = a.input.z = 0;
    m.controllers[a.id] = new AIController(a, mateHuman ? PARTNER : diff, this.rng);
    if (!mateHuman) m.controllers[mate.id] = new AIController(mate, diff, this.rng);
    this.broadcast({ t: "cpu", a: a.id, name: p.name });
  }
  toHuman(p) {
    const m = this.match, a = p.athlete;
    if (!m || !a) return;
    p.cpu = false;
    p.source.reset();
    m.controllers[a.id] = new PlayerController(a, p.source, this.rng);
    m.controllers[a.id].setAssist("media");
    const mate = m.mate(a);
    if (!this.players.some((q) => q !== p && q.athlete === mate && !q.cpu)) m.controllers[mate.id] = new AIController(mate, PARTNER, this.rng);
  }
  snapshot() {
    const st = JSON.stringify(packState(this.match)), ev = JSON.stringify(this.pending);
    this.pending = [];
    for (const p of this.players) if (p.conn) p.conn.send(`{"t":"s","k":${this.tick},"a":${p.ack},"s":${st},"e":${ev}}`);
  }
  step() {
    const m = this.match;
    for (const p of this.players) if (p.athlete && !p.cpu) m.controllers[p.athlete.id].lag = 0;
    m.update(CFG.DT);
    this.tick++;
    let ended = false;
    for (const e of m.drain()) {
      if (LOCAL_EVENTS.has(e.type)) continue;
      const o = packEvent(e);
      if (e.type === "matchEnd") {
        o.stats = m.rules.stats;
        o.scores = m.rules.scores.slice();
        ended = true;
      }
      this.pending.push(o);
    }
    if (this.pending.length || this.tick % LIMITS.snapEvery === 0) this.snapshot();
    if (ended) {
      this.state = "lobby";
      this.match = null;
      this.players = this.players.filter((p) => p.on);
      for (const p of this.players) {
        p.ready = false;
        p.athlete = null;
        p.cpu = false;
      }
      this.fixHost();
      this.sendLobby();
    }
  }
  housekeeping(now) {
    for (const p of this.players.slice()) {
      if (p.on) continue;
      const age = now - p.offAt;
      if (this.state === "lobby" && age > LIMITS.offLobbyMs) this.remove(p);
      else if (this.state === "play" && age > LIMITS.offPlayMs && !p.cpu) this.toCpu(p);
    }
    if (this.players.some((p) => p.on)) this.emptyAt = 0;
    else if (!this.emptyAt) this.emptyAt = now;
    return this.emptyAt && now - this.emptyAt > LIMITS.emptyMs;
  }
};
var Hub = class {
  constructor() {
    this.rooms = /* @__PURE__ */ new Map();
    this.acc = 0;
    this.last = 0;
    this.hk = 0;
    this.clock = () => Date.now();
  }
  newCode() {
    for (let i = 0; i < 50; i++) {
      let c = "";
      for (let j = 0; j < 4; j++) c += CODE_CHARS[Math.random() * CODE_CHARS.length | 0];
      if (!this.rooms.has(c)) return c;
    }
    return null;
  }
  err(conn, msg) {
    conn.send(JSON.stringify({ t: "err", msg }));
  }
  attach(conn, room, p) {
    if (p.conn && p.conn !== conn) {
      const old = p.conn;
      old.player = null;
      old.room = null;
      try {
        old.close();
      } catch (e) {
      }
    }
    p.conn = conn;
    p.offAt = 0;
    conn.player = p;
    conn.room = room;
    conn.send(JSON.stringify({ t: "joined", v: PROTO, code: room.code, you: p.id, token: p.token }));
    room.fixHost();
    if (room.state === "play" && p.athlete) {
      if (p.cpu) room.toHuman(p);
      p.source.reset();
      conn.send(JSON.stringify(room.startMsg(p)));
    }
    room.sendLobby();
  }
  // mensagem recebida de uma conexao (string JSON)
  message(conn, raw) {
    const now = this.clock();
    if (conn.winAt === void 0 || now - conn.winAt > 1e3) {
      conn.winAt = now;
      conn.winN = 0;
    }
    if (++conn.winN > LIMITS.msgsPerSec) {
      try {
        conn.close();
      } catch (e) {
      }
      return;
    }
    if (typeof raw !== "string" || raw.length > 600) return;
    let d;
    try {
      d = JSON.parse(raw);
    } catch (e) {
      return;
    }
    if (!d || typeof d !== "object") return;
    const room = conn.room, p = conn.player;
    switch (d.t) {
      case "ping":
        conn.send(`{"t":"pong","c":${num(d.c, 0, 1e15)}}`);
        if (p) p.ping = Math.round(num(d.rtt, 0, 2e3));
        break;
      case "in": {
        if (!room || !p || room.state !== "play" || !p.athlete || p.cpu) break;
        const s = p.source;
        s.mx = num(d.x, -1, 1);
        s.mz = num(d.z, -1, 1);
        s.sprint = !!d.s;
        const hm = d.h | 0;
        for (const k in HELD_BITS) s.held[k] = !!(hm & HELD_BITS[k]);
        if (Array.isArray(d.p)) {
          for (const a of d.p.slice(0, 6)) if (ACTIONS.includes(a)) s.pressed[a] = true;
        }
        p.ack = num(d.q, 0, 1e12) | 0;
        break;
      }
      case "create": {
        if (room) break;
        if (this.rooms.size >= LIMITS.rooms) {
          this.err(conn, "Servidor cheio. Tente de novo em alguns minutos.");
          break;
        }
        const code = this.newCode();
        if (!code) {
          this.err(conn, "N\xE3o foi poss\xEDvel criar a sala.");
          break;
        }
        const r = new Room(this, code);
        this.rooms.set(code, r);
        if (d.points === 18) r.opts.points = 18;
        if (DIFFICULTY[d.diff]) r.opts.diff = d.diff;
        this.attach(conn, r, r.add(cleanName(d.name)));
        break;
      }
      case "join": {
        if (room) break;
        const r = this.rooms.get(cleanCode(d.code));
        if (!r) {
          this.err(conn, "Sala n\xE3o encontrada. Confira o c\xF3digo.");
          break;
        }
        const old = typeof d.token === "string" && r.players.find((q) => q.token === d.token);
        if (old) {
          this.attach(conn, r, old);
          break;
        }
        if (r.state === "play") {
          this.err(conn, "Partida em andamento nessa sala. Tente quando ela terminar.");
          break;
        }
        if (r.players.length >= LIMITS.perRoom) {
          this.err(conn, "Sala cheia (4 jogadores).");
          break;
        }
        this.attach(conn, r, r.add(cleanName(d.name)));
        break;
      }
      case "team": {
        if (!room || room.state !== "lobby") break;
        const t = d.team === 1 ? 1 : 0;
        if (t !== p.team && room.humans(t).length < 2) {
          p.team = t;
          p.ready = false;
          room.sendLobby();
        }
        break;
      }
      case "ready":
        if (room && room.state === "lobby") {
          p.ready = !!d.v;
          room.sendLobby();
        }
        break;
      case "opts": {
        if (!room || room.state !== "lobby" || p.id !== room.hostId) break;
        if (d.points === 12 || d.points === 18) room.opts.points = d.points;
        if (DIFFICULTY[d.diff]) room.opts.diff = d.diff;
        room.sendLobby();
        break;
      }
      case "start":
        if (room && room.state === "lobby" && p.id === room.hostId) room.start();
        break;
      case "chat": {
        if (!room || !p) break;
        const i = d.i | 0;
        if (i < 0 || i >= CHAT.length || now - p.chatAt < 700) break;
        p.chatAt = now;
        room.broadcast({ t: "chat", from: p.id, name: p.name, a: p.athlete ? p.athlete.id : -1, i });
        break;
      }
      case "leave":
        if (room && p) {
          conn.player = null;
          conn.room = null;
          p.conn = null;
          room.remove(p);
          if (!room.players.length) this.rooms.delete(room.code);
        }
        break;
    }
  }
  closed(conn) {
    const p = conn.player, room = conn.room;
    if (!p || !room || p.conn !== conn) return;
    p.conn = null;
    p.offAt = this.clock();
    p.ready = false;
    p.source.reset();
    room.fixHost();
    if (room.players.some((q) => q.on)) room.sendLobby();
  }
  // avanca todas as partidas; chame com frequencia (ex.: a cada 8 ms)
  pump(now = this.clock()) {
    if (!this.last) this.last = now;
    this.acc += Math.min(250, now - this.last);
    this.last = now;
    const stepMs = 1e3 * CFG.DT;
    let n = 0;
    while (this.acc >= stepMs && n < 5) {
      this.acc -= stepMs;
      n++;
      for (const r of this.rooms.values()) if (r.state === "play") r.step();
    }
    if (n === 5) this.acc = 0;
    if (now - this.hk > 1e3) {
      this.hk = now;
      for (const r of [...this.rooms.values()]) if (r.housekeeping(now)) this.rooms.delete(r.code);
    }
  }
  stats() {
    let pl = 0, on = 0;
    for (const r of this.rooms.values()) {
      pl += r.players.length;
      if (r.state === "play") on++;
    }
    return { rooms: this.rooms.size, playing: on, players: pl };
  }
};

// server/main.js
var PORT = process.env.PORT || 3e3;
var ROOT = import_node_path.default.join(process.cwd(), "public");
var TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".png": "image/png", ".jpg": "image/jpeg", ".glb": "model/gltf-binary", ".json": "application/json", ".svg": "image/svg+xml", ".ico": "image/x-icon" };
var cache = /* @__PURE__ */ new Map();
var hub = new Hub();
var onlineDirty = false;
function load(file) {
  if (!cache.has(file)) {
    const raw = import_node_fs.default.readFileSync(file);
    cache.set(file, { raw, gz: import_node_zlib.default.gzipSync(raw, { level: 9 }), br: import_node_zlib.default.brotliCompressSync(raw) });
  }
  return cache.get(file);
}
var server = import_node_http.default.createServer((req, res) => {
  let url = "/";
  try {
    url = decodeURIComponent((req.url || "/").split("?")[0]);
  } catch (e) {
  }
  if (url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    return res.end(JSON.stringify({ ok: true, online: wss.clients.size, ...hub.stats() }));
  }
  let file = import_node_path.default.normalize(import_node_path.default.join(ROOT, url === "/" ? "index.html" : url));
  if (!file.startsWith(ROOT) || !import_node_fs.default.existsSync(file) || import_node_fs.default.statSync(file).isDirectory()) file = import_node_path.default.join(ROOT, "index.html");
  const body = load(file);
  const ae = req.headers["accept-encoding"] || "";
  const headers = { "Content-Type": TYPES[import_node_path.default.extname(file)] || "application/octet-stream", "Cache-Control": file.endsWith(".html") ? "no-cache" : "public, max-age=86400", Vary: "Accept-Encoding" };
  let data = body.raw;
  if (/\bbr\b/.test(ae)) {
    headers["Content-Encoding"] = "br";
    data = body.br;
  } else if (/\bgzip\b/.test(ae)) {
    headers["Content-Encoding"] = "gzip";
    data = body.gz;
  }
  headers["Content-Length"] = data.length;
  res.writeHead(200, headers);
  res.end(req.method === "HEAD" ? void 0 : data);
});
var wss = new import_websocket_server.default({ server, path: "/ws", maxPayload: 2048, perMessageDeflate: false });
wss.on("connection", (ws) => {
  if (wss.clients.size > 1500) {
    ws.close();
    return;
  }
  const conn = { send: (s) => {
    if (ws.readyState === 1 && ws.bufferedAmount < 262144) ws.send(s);
  }, close: () => ws.close(), player: null, room: null };
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });
  ws.on("message", (data, isBinary) => {
    if (!isBinary) {
      try {
        hub.message(conn, data.toString());
      } catch (e) {
        console.error("msg", e);
      }
    }
  });
  ws.on("close", () => {
    hub.closed(conn);
    onlineDirty = true;
  });
  onlineDirty = true;
  conn.send(JSON.stringify({ t: "online", n: wss.clients.size }));
  ws.on("error", () => {
  });
});
setInterval(() => {
  try {
    hub.pump();
  } catch (e) {
    console.error("pump", e);
  }
}, 8);
setInterval(() => {
  if (!onlineDirty) return;
  onlineDirty = false;
  const msg = JSON.stringify({ t: "online", n: wss.clients.size });
  for (const ws of wss.clients) if (ws.readyState === 1 && ws.bufferedAmount < 65536) ws.send(msg);
}, 3e3);
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (e) {
    }
  }
}, 15e3);
server.listen(PORT, () => console.log(`FUTEVOLEI.GG no ar na porta ${PORT} (online em /ws)`));
