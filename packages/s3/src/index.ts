import {
  LaQLError,
  type ListOptions,
  type ObjectHead,
  type ObjectInfo,
  type ObjectStore,
  type PutOptions,
} from "@laql/core";

export const PACKAGE = "@laql/s3" as const;

export interface S3StoreOptions {
  endpoint: string;
  bucket: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  fetch?: typeof fetch;
  now?: () => Date;
}

export function s3Store(options: S3StoreOptions): ObjectStore {
  return new S3ObjectStore(options);
}

export class S3ObjectStore implements ObjectStore {
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => Date;

  constructor(private readonly options: S3StoreOptions) {
    this.fetchImpl = options.fetch ?? fetch;
    this.now = options.now ?? (() => new Date());
  }

  async get(path: string): Promise<Uint8Array | null> {
    const response = await this.request("GET", path);
    if (response.status === 404) return null;
    assertOk(response, path);
    return new Uint8Array(await response.arrayBuffer());
  }

  async getRange(path: string, range: { offset: number; length: number }): Promise<Uint8Array> {
    const response = await this.request("GET", path, {
      Range: `bytes=${range.offset}-${range.offset + range.length - 1}`,
    });
    assertOk(response, path);
    return new Uint8Array(await response.arrayBuffer());
  }

  async put(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options?: PutOptions,
  ): Promise<void> {
    const headers: Record<string, string> = {};
    if (options?.contentType) headers["content-type"] = options.contentType;
    const response = await this.request("PUT", path, headers, body);
    assertOk(response, path);
  }

  async delete(path: string): Promise<void> {
    const response = await this.request("DELETE", path);
    assertOk(response, path);
  }

  async *list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo> {
    const query = new URLSearchParams({ "list-type": "2", prefix });
    if (options?.limit !== undefined) query.set("max-keys", String(options.limit));
    const response = await this.request("GET", "", {}, undefined, query);
    assertOk(response, prefix);
    for (const object of parseListObjectsV2(await response.text())) yield object;
  }

  async head(path: string): Promise<ObjectHead | null> {
    const response = await this.request("HEAD", path);
    if (response.status === 404) return null;
    assertOk(response, path);
    const contentLength = response.headers.get("content-length");
    if (contentLength === null) {
      throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `Missing S3 content-length for ${path}`, {
        path,
      });
    }
    const head: ObjectHead = { size: Number(contentLength) };
    const etag = response.headers.get("etag");
    const contentType = response.headers.get("content-type");
    const lastModified = response.headers.get("last-modified");
    if (etag !== null) head.etag = etag;
    if (contentType !== null) head.contentType = contentType;
    if (lastModified !== null) head.lastModified = new Date(lastModified);
    return head;
  }

  private async request(
    method: string,
    path: string,
    headers: Record<string, string> = {},
    body?: Uint8Array | ReadableStream<Uint8Array>,
    query = new URLSearchParams(),
  ): Promise<Response> {
    const url = new URL(
      `${encodeURIComponent(this.options.bucket)}/${path}`,
      ensureSlash(this.options.endpoint),
    );
    url.search = query.toString();
    const signRequest: SignRequest = {
      method,
      url,
      headers,
      region: this.options.region,
      accessKeyId: this.options.accessKeyId,
      secretAccessKey: this.options.secretAccessKey,
      now: this.now(),
    };
    if (this.options.sessionToken !== undefined)
      signRequest.sessionToken = this.options.sessionToken;
    const signed = await signS3Request(signRequest);
    const init: RequestInit = { method, headers: signed };
    if (body !== undefined) init.body = bodyInit(body);
    return this.fetchImpl(url, init);
  }
}

interface SignRequest {
  method: string;
  url: URL;
  headers: Record<string, string>;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  now: Date;
}

export async function signS3Request(request: SignRequest): Promise<Headers> {
  const headers = new Headers(request.headers);
  const amzDate = timestamp(request.now);
  const date = amzDate.slice(0, 8);
  headers.set("host", request.url.host);
  headers.set("x-amz-date", amzDate);
  headers.set("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
  if (request.sessionToken) headers.set("x-amz-security-token", request.sessionToken);

  const signedHeaders = [...headers.keys()].map((key) => key.toLowerCase()).sort();
  const canonicalHeaders = signedHeaders
    .map((key) => `${key}:${headers.get(key)?.trim().replace(/\s+/gu, " ") ?? ""}\n`)
    .join("");
  const credentialScope = `${date}/${request.region}/s3/aws4_request`;
  const canonicalRequest = [
    request.method,
    request.url.pathname,
    canonicalQuery(request.url.searchParams),
    canonicalHeaders,
    signedHeaders.join(";"),
    "UNSIGNED-PAYLOAD",
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    await sha256Hex(canonicalRequest),
  ].join("\n");
  const signingKey = await hmac(
    await hmac(
      await hmac(await hmac(`AWS4${request.secretAccessKey}`, date), request.region),
      "s3",
    ),
    "aws4_request",
  );
  const signature = await hmacHex(signingKey, stringToSign);
  headers.set(
    "authorization",
    `AWS4-HMAC-SHA256 Credential=${request.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders.join(";")}, Signature=${signature}`,
  );
  return headers;
}

function parseListObjectsV2(xml: string): ObjectInfo[] {
  const objects: ObjectInfo[] = [];
  for (const match of xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/gu)) {
    const block = match[1] ?? "";
    const key = text(block, "Key");
    const size = Number(text(block, "Size"));
    if (!key || !Number.isFinite(size)) continue;
    const info: ObjectInfo = { path: decodeXml(key), size };
    const etag = text(block, "ETag");
    const lastModified = text(block, "LastModified");
    if (etag) info.etag = decodeXml(etag);
    if (lastModified) info.lastModified = new Date(lastModified);
    objects.push(info);
  }
  return objects;
}

function text(block: string, tag: string): string {
  return block.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "u"))?.[1] ?? "";
}

function decodeXml(value: string): string {
  return value
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&quot;", '"')
    .replaceAll("&apos;", "'")
    .replaceAll("&amp;", "&");
}

function ensureSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

function assertOk(response: Response, path: string): void {
  if (response.ok || response.status === 206) return;
  if (response.status === 404) {
    throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
  }
  throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `S3 request failed for ${path}`, {
    path,
    status: response.status,
  });
}

function canonicalQuery(params: URLSearchParams): string {
  return [...params.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join("&");
}

function timestamp(date: Date): string {
  return date.toISOString().replace(/[:-]|\.\d{3}/gu, "");
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return hex(new Uint8Array(digest));
}

async function hmac(key: string | Uint8Array, value: string): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    typeof key === "string" ? new TextEncoder().encode(key) : arrayBuffer(key),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return new Uint8Array(
    await crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(value)),
  );
}

function bodyInit(body: Uint8Array | ReadableStream<Uint8Array>): BodyInit {
  if (body instanceof Uint8Array) return arrayBuffer(body);
  return body;
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}

async function hmacHex(key: Uint8Array, value: string): Promise<string> {
  return hex(await hmac(key, value));
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
