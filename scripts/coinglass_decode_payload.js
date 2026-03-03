#!/usr/bin/env node
"use strict";

const fs = require("fs");
const vm = require("vm");

function parseArgs(argv) {
  const parsed = {
    bundle: "",
    ciphertextB64: "",
    key: "",
  };

  for (let idx = 0; idx < argv.length; idx += 1) {
    const current = argv[idx];
    const next = argv[idx + 1];
    if (!next) {
      break;
    }
    if (current === "--bundle") {
      parsed.bundle = next;
      idx += 1;
      continue;
    }
    if (current === "--ciphertext-b64") {
      parsed.ciphertextB64 = next;
      idx += 1;
      continue;
    }
    if (current === "--key") {
      parsed.key = next;
      idx += 1;
    }
  }

  if (!parsed.bundle || !parsed.ciphertextB64 || !parsed.key) {
    throw new Error(
      "Usage: coinglass_decode_payload.js --bundle <_app.js> --ciphertext-b64 <base64> --key <key>"
    );
  }

  return parsed;
}

function createWebpackRequire(bundlePath) {
  const bundleCode = fs.readFileSync(bundlePath, "utf8");
  const moduleMap = {};

  const previousSelf = globalThis.self;
  const previousChunk = globalThis.webpackChunk_N_E;

  globalThis.self = globalThis;
  globalThis.webpackChunk_N_E = [];
  globalThis.webpackChunk_N_E.push = function pushChunk(chunk) {
    Object.assign(moduleMap, chunk[1] || {});
  };

  try {
    vm.runInThisContext(bundleCode, { filename: bundlePath });
  } finally {
    globalThis.self = previousSelf;
    globalThis.webpackChunk_N_E = previousChunk;
  }

  const moduleCache = {};

  function webpackRequire(moduleId) {
    if (moduleCache[moduleId]) {
      return moduleCache[moduleId].exports;
    }

    const moduleFactory = moduleMap[moduleId];
    if (!moduleFactory) {
      throw new Error(`Missing webpack module: ${moduleId}`);
    }

    const moduleRecord = { exports: {} };
    moduleCache[moduleId] = moduleRecord;
    moduleFactory(moduleRecord, moduleRecord.exports, webpackRequire);
    return moduleRecord.exports;
  }

  webpackRequire.d = (exportsObject, definition) => {
    for (const key of Object.keys(definition)) {
      if (!webpackRequire.o(definition, key) || webpackRequire.o(exportsObject, key)) {
        continue;
      }
      Object.defineProperty(exportsObject, key, {
        enumerable: true,
        get: definition[key],
      });
    }
  };

  webpackRequire.o = (target, key) => Object.prototype.hasOwnProperty.call(target, key);

  webpackRequire.r = (exportsObject) => {
    if (typeof Symbol !== "undefined" && Symbol.toStringTag) {
      Object.defineProperty(exportsObject, Symbol.toStringTag, { value: "Module" });
    }
    Object.defineProperty(exportsObject, "__esModule", { value: true });
  };

  webpackRequire.n = (moduleValue) => {
    const getter =
      moduleValue && moduleValue.__esModule
        ? () => moduleValue.default
        : () => moduleValue;
    webpackRequire.d(getter, { a: getter });
    return getter;
  };

  webpackRequire.g = globalThis;
  return webpackRequire;
}

function decodePayload(bundlePath, ciphertext, key) {
  const webpackRequire = createWebpackRequire(bundlePath);
  const cryptoJs = webpackRequire.n(webpackRequire(81354))();
  const pako = webpackRequire(27885).ZP;

  const decryptedHex = cryptoJs.AES.decrypt(
    ciphertext,
    cryptoJs.enc.Utf8.parse(key),
    {
      mode: cryptoJs.mode.ECB,
      padding: cryptoJs.pad.Pkcs7,
    }
  ).toString(cryptoJs.enc.Hex);

  if (!decryptedHex) {
    throw new Error("Coinglass decrypt produced an empty payload.");
  }

  const hexPairs = decryptedHex.match(/[\da-f]{2}/gi) || [];
  const compressed = new Uint8Array(hexPairs.map((pair) => parseInt(pair, 16)));
  const inflated = pako.inflate(compressed);

  let text = Buffer.from(inflated).toString("utf8");
  if (text.startsWith("\"")) {
    text = text.slice(1);
  }
  if (text.endsWith("\"")) {
    text = text.slice(0, -1);
  }

  return text;
}

function main() {
  try {
    const args = parseArgs(process.argv.slice(2));
    const ciphertext = Buffer.from(args.ciphertextB64, "base64").toString("utf8");
    const text = decodePayload(args.bundle, ciphertext, args.key);
    process.stdout.write(text);
    return 0;
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    return 1;
  }
}

const exitCode = main();
if (exitCode !== 0) {
  process.exitCode = exitCode;
}
