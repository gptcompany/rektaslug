# Provider API Reverse Engineering Handoff

## Date: 2026-03-03

## Objective

Reverse engineer the CoinGlass LiquidationMap frontend to replace the fragile
UI dropdown approach with a more robust, programmatic API capture strategy.

## Key Findings

### 1. CoinGlass `data` Query Parameter (FULLY DECODED)

The `data` parameter on `api/index/5/liqMap` (and other liqMap/liqHeatMap endpoints)
is generated client-side by a function in `_app-*.js`:

```javascript
function a() {
  var t = parseInt((new Date).getTime() / 1e3);   // Unix timestamp (seconds)
  var e = t + "," + authenticator.generate(
    "I65VU7K5ZQL7WB4E",                            // TOTP secret (base32)
    {time: t, step: 30}                             // 30-second TOTP window
  );
  return CryptoJS.AES.encrypt(
    e,
    CryptoJS.enc.Utf8.parse("1f68efd73f8d4921acc0dead41dd39bc"),  // AES-128 key
    {mode: CryptoJS.mode.ECB, padding: CryptoJS.pad.Pkcs7}
  ).toString();  // base64 output
}
```

**Python implementation** (now in `scripts/capture_provider_api.py`):

```python
import base64, time, pyotp
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

def generate_coinglass_data_param():
    ts = int(time.time())
    totp = pyotp.TOTP("I65VU7K5ZQL7WB4E", interval=30)
    otp_code = totp.at(ts)
    plaintext = f"{ts},{otp_code}"
    cipher = AES.new("1f68efd73f8d4921acc0dead41dd39bc".encode(), AES.MODE_ECB)
    encrypted = cipher.encrypt(pad(plaintext.encode(), AES.block_size))
    return base64.b64encode(encrypted).decode()
```

These are **public client-side constants** from the minified JS bundle.
They are anti-scraping measures, not secrets.

### 2. Timeframe to API Parameter Mapping (COMPLETE)

Source: `LiquidationMap-*.js` chunk, `onChangeTime` handler.

| UI Label   | Dropdown Value | `interval` | `limit` |
|-----------|---------------|-----------|---------|
| 1 day     | 1             | `1`       | 1500    |
| 7 day     | 7             | `5`       | 2000    |
| 30 day    | 30            | `30`      | 1440    |
| 90 day    | 90            | `90d`     | 1440    |
| 180 day   | 180           | `180d`    | 1440    |
| 1 year    | 365           | `365d`    | 1440    |

### 3. Authentication Mechanism (FULLY DECODED)

- Auth is via a **custom header `obe`** containing the session token.
- Login endpoint: `POST capi.coinglass.com/coin-community/api/user/login`
  with `mailAddress` + `password` (form-urlencoded).
- The response returns `accessToken` (format: `s_<32hex>`), which is:
  - Set as a cookie `obe` on `.coinglass.com` (httpOnly=false)
  - Read by the axios request interceptor and attached as header `obe: <token>`
- Token TTL: ~27 hours (`accessTokenExpireIn` in response).
- Also available: `refreshToken` (format: `l_<32hex>`) for session renewal.
- Cross-origin requests from `www.coinglass.com` to `capi.coinglass.com` work because
  the server returns `access-control-allow-headers: *`.
- **Direct programmatic API calls now work** via `coinglass_rest_login()` +
  `coinglass_rest_fetch_liqmap()` - no browser needed.

### 4. Response Decryption

The response `data` field (when `encryption` header is present) is:

1. AES-ECB decrypted using the `user` response header as key
2. Pako-inflated (zlib decompressed)
3. Parsed as JSON

The existing `scripts/coinglass_decode_payload.js` handles this by loading
CryptoJS and pako from the saved `_app` bundle.

### 5. Can We Bypass the Browser?

**YES, FULLY.** Two approaches are now available:

**Approach A: Playwright route interception** (original, still works)
1. Login via Playwright browser
2. Navigate to LiquidationMap page
3. Intercept the page's own `liqMap` request via `page.route()`
4. Rewrite the `interval`, `limit`, and `data` parameters
5. Page reload triggers a fresh authenticated request with desired params

**Approach B: Pure REST replay** (new, no browser needed)
1. `POST` to login endpoint with email/password → get `accessToken`
2. `GET` liqMap with `Obe: <token>` header + TOTP+AES `data` param
3. Response comes back with encryption headers for decoding

Approach B is **preferred** for automation/CI because:
- No Playwright dependency for the CoinGlass path
- Faster (~2 seconds vs ~30 seconds with browser)
- No headless detection risks
- Simpler error handling

## Implementation Changes

### Modified Files

1. **`scripts/capture_provider_api.py`**
   - Added `generate_coinglass_data_param()` - Python TOTP+AES generation
   - Added `resolve_coinglass_interval_limit()` - timeframe to API param mapping
   - Added `coinglass_direct_fetch()` - route interception for param rewriting
   - New deps: `pyotp`, `pycryptodome`

2. **`pyproject.toml`** (via `uv add`)
   - Added `pyotp` and `pycryptodome` dependencies

3. **`scripts/coinglass_decode_payload.js`**
   - Updated decoding logic to handle new response format

4. **`scripts/compare_provider_liquidations.py`**
   - Major expansion: added cross-provider comparison logic, SQL reporting,
     and CoinGlass decoded payload integration

## Verified Captures

### CoinGlass 1w (route interception)
- Manifest: `data/validation/raw_provider_api/20260303T165247Z/manifest.json`
- Login: success
- Timeframe applied: true
- Captures: 2 liqMap requests (default interval=1 + rewritten interval=5)
- Both returned `success=true` with encrypted data

### CoinGlass 1d (route interception)
- Manifest: `data/validation/raw_provider_api/20260303T165345Z/manifest.json`
- Login: success
- Timeframe applied: true
- Captures: 1 liqMap request (interval=1, same as default)
- Returned `success=true` with encrypted data

## URL Reference

### CoinGlass
- Login: `https://www.coinglass.com/login?act=liqmap`
- Liquidation Map (liq-map equivalent): `https://www.coinglass.com/pro/futures/LiquidationMap`
- Liquidation HeatMap (heatmap): `https://www.coinglass.com/pro/futures/LiquidationHeatMapNew`
- Public Liquidity Heatmap: `https://www.coinglass.com/LiquidityHeatmap`

### CoinAnk
- Liq map: `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/{timeframe}`
- Liq heatmap: `https://coinank.com/chart/derivatives/liq-heat-map/binance/btcusdt/{timeframe}`

### API Endpoints
- CoinGlass liqMap: `https://capi.coinglass.com/api/index/5/liqMap?merge=true&symbol=Binance_BTCUSDT&interval={interval}&limit={limit}&data={data}`
- CoinGlass exLiqMap: `https://capi.coinglass.com/api/index/2/exLiqMap?merge=true&symbol=BTC&interval={interval}&limit={limit}`
- CoinAnk getLiqMap: `https://api.coinank.com/api/liqMap/getLiqMap?exchange=Binance&symbol=BTCUSDT&interval={timeframe}`

## Frontend Bundle References

| File | Hash | Purpose |
|------|------|---------|
| `_app-*.js` | `94ee9e72c1d2190a` | Main bundle with API functions and interceptors |
| `LiquidationMap-*.js` | `e61d9cea2e1db745` | Page component with onChangeTime handler |
| Chunk 33638 | `b2f137eab4511f73` | Exchange-specific liquidation map chart |
| Chunk 74267 | `5c8fab292b36814d` | Aggregate liquidation map chart |

### 6. Browserless REST API Replay (FULLY WORKING)

The full browser flow can now be replaced by two plain HTTP calls:

1. **Login**: `POST capi.coinglass.com/coin-community/api/user/login`
   - Content-Type: `application/x-www-form-urlencoded`
   - Body: `mailAddress=<email>&password=<password>`
   - Required headers: `Language: en`, `Encryption: true`, `Cache-Ts-V2: <now_ms>`
   - Returns JSON with `accessToken` (= `obe` header), ~27h TTL

2. **Fetch liqMap**: `GET capi.coinglass.com/api/index/5/liqMap?...`
   - Headers: `Obe: <accessToken>`, `Language: en`, `Encryption: true`,
     `Cache-Ts-V2: <now_ms>`, `Referer: https://www.coinglass.com/`
   - Query: `merge=true&symbol=Binance_BTCUSDT&interval=<X>&limit=<Y>&data=<totp_aes>`
   - Returns encrypted response with `encryption`/`user`/`v`/`time` headers for decoding

Python functions: `coinglass_rest_login()` and `coinglass_rest_fetch_liqmap()` in
`scripts/capture_provider_api.py`. All 6 timeframes verified browserless (2026-03-03).

## Next Steps

1. **Monitor bundle hash changes** - If CoinGlass updates the frontend, the TOTP
   secret and AES key may change. The bundle URL hash will change when this happens.
2. **Investigate ~14x volume difference** - CoinAnk `getLiqMap` vs CoinGlass
   `liqMapV2` total volumes differ by ~14x. Likely leverage tier inclusion and
   aggregation methodology differences.

## Commit History

- `28ba2ea` Add end-to-end provider comparison workflow
- `60bbd39` Decode Coinglass payloads and add SQL reporting
- `cae0e8f` Reverse engineer CoinGlass data param and add route interception
