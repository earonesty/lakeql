# Browser demo fixture serving

The playground sample is published to the `lakeql-public` R2 bucket under a
SHA-256-addressed object key. Vite derives the default URL from the checked-in
`site/public/sales.parquet` bytes, so the UI and uploader use the same identity.
The Pages deployment publishes the object before building the site and checks
actual start, middle, and footer ranges, including browser CORS visibility.
Without upload credentials it still requires the exact object to be available.

Objects use `Cache-Control: public, max-age=31536000, immutable, no-transform`.
Parquet's internal compression is independent of HTTP compression. HTTP byte
ranges must refer to the exact stored representation consumed by the reader.
An uncompressed-size hint cannot repair offsets into a compressed representation.

## Why the playground moved off the website origin

On November 16, 2026, `https://lakeql.com/sales.parquet` returned a valid 2,820-byte
file for a full GET. Through Cloudflare, a range probe advertised
`Content-Range: bytes 0-1/1330` with an empty body; the claimed footer range
`1322-1329` returned eight bytes without a Parquet footer. Neither response
exposed a Content-Encoding explaining the different representation. Requesting
`Accept-Encoding: identity` did not correct the proxied response.

A direct request to GitHub Pages at `185.199.108.153`, preserving the lakeql.com
Host header (with TLS verification disabled for that diagnostic), returned
`Content-Range: bytes 0-1/2820` and the correct `PA` bytes. Asking that origin
for `Accept-Encoding: gzip` instead returned `Content-Encoding: gzip`,
`Content-Range: bytes 0-1/1330`, and the two gzip signature bytes `1f8b`.
The proxied response is consistent with Cloudflare decompressing a partial gzip
representation while retaining its compressed offsets. This isolates the
inconsistent representation to the Cloudflare-proxied path; it does not establish
which Cloudflare configuration or deployment first introduced it. The playground
had used a website-relative sample since its original commit (`7402210`), rather
than the R2 path used by the comparison fixtures. The sample and HTTP adapter
were unchanged between the pre-performance baseline and merged PR #23.

The replacement R2 object passes byte-for-byte start, middle, and footer checks.
Chromium verified SQL, JS, and JSON using the default URL and the empty-source
fallback; all returned the same 12 rows. A custom same-origin sample also passed
all three modes on a local server. This change does not make claims about total
browser heap usage or change how arbitrary user-supplied HTTP servers behave.
