import {
  type ConditionalObjectStore,
  type ConditionalPutOptions,
  LaQLError,
  type ListOptions,
  type ObjectHead,
  type ObjectInfo,
  type PutOptions,
} from "@laql/core";
import { AwsClient } from "aws4fetch";
import { XMLParser } from "fast-xml-parser";

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

export function s3Store(options: S3StoreOptions): ConditionalObjectStore {
  return new S3ObjectStore(options);
}

export class S3ObjectStore implements ConditionalObjectStore {
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => Date;
  private readonly aws: AwsClient;

  constructor(private readonly options: S3StoreOptions) {
    this.fetchImpl = options.fetch ?? fetch;
    this.now = options.now ?? (() => new Date());
    const awsOptions: ConstructorParameters<typeof AwsClient>[0] = {
      accessKeyId: options.accessKeyId,
      secretAccessKey: options.secretAccessKey,
      region: options.region,
      service: "s3",
      retries: 0,
    };
    if (options.sessionToken !== undefined) awsOptions.sessionToken = options.sessionToken;
    this.aws = new AwsClient(awsOptions);
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

  async conditionalPut(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options: ConditionalPutOptions,
  ): Promise<boolean> {
    const headers: Record<string, string> = {};
    if (options.contentType) headers["content-type"] = options.contentType;
    if (options.expectedEtag === null) headers["if-none-match"] = "*";
    else headers["if-match"] = options.expectedEtag;
    const response = await this.request("PUT", path, headers, body);
    if (response.status === 409 || response.status === 412) return false;
    assertOk(response, path);
    return true;
  }

  async delete(path: string): Promise<void> {
    const response = await this.request("DELETE", path);
    assertOk(response, path);
  }

  async *list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo> {
    let continuationToken: string | undefined;
    let emitted = 0;
    do {
      const query = new URLSearchParams({ "list-type": "2", prefix });
      if (options?.limit !== undefined) query.set("max-keys", String(options.limit - emitted));
      if (options?.delimiter !== undefined) query.set("delimiter", options.delimiter);
      if (continuationToken !== undefined) query.set("continuation-token", continuationToken);
      const response = await this.request("GET", "", {}, undefined, query);
      assertOk(response, prefix);
      const result = parseListObjectsV2(await response.text());
      for (const object of result.objects) {
        if (options?.limit !== undefined && emitted >= options.limit) return;
        yield object;
        emitted += 1;
      }
      continuationToken = result.nextContinuationToken;
    } while (
      continuationToken !== undefined &&
      (options?.limit === undefined || emitted < options.limit)
    );
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
      `${encodeURIComponent(this.options.bucket)}/${encodeObjectPath(path)}`,
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
    const requestBody = body === undefined ? undefined : bodyInit(body);
    const signed = await signS3Request(signRequest, this.aws, requestBody);
    const init: RequestInit = { method, headers: signed };
    if (requestBody !== undefined) init.body = requestBody;
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

export async function signS3Request(
  request: SignRequest,
  client = s3SignerClient(request),
  body?: BodyInit,
): Promise<Headers> {
  const headers = new Headers(request.headers);
  if (!headers.has("x-amz-content-sha256")) {
    headers.set("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
  }
  const init: Parameters<AwsClient["sign"]>[1] = {
    method: request.method,
    headers,
    aws: {
      datetime: timestamp(request.now),
    },
  };
  if (body !== undefined) init.body = body;
  const signed = await client.sign(request.url, init);
  return signed.headers;
}

function s3SignerClient(request: SignRequest): AwsClient {
  const options: ConstructorParameters<typeof AwsClient>[0] = {
    accessKeyId: request.accessKeyId,
    secretAccessKey: request.secretAccessKey,
    region: request.region,
    service: "s3",
    retries: 0,
  };
  if (request.sessionToken !== undefined) options.sessionToken = request.sessionToken;
  return new AwsClient(options);
}

const listObjectsParser = new XMLParser({
  ignoreAttributes: true,
  parseTagValue: false,
  trimValues: true,
});

export function parseListObjectsV2(xml: string): {
  objects: ObjectInfo[];
  nextContinuationToken?: string;
} {
  const parsed = listObjectsParser.parse(xml) as {
    ListBucketResult?: {
      Contents?: unknown;
      IsTruncated?: unknown;
      NextContinuationToken?: unknown;
    };
  };
  const result = parsed.ListBucketResult ?? {};
  const objects: ObjectInfo[] = [];
  for (const content of arrayOf<ListObjectContent>(result.Contents)) {
    const key = stringValue(content.Key);
    const size = Number(stringValue(content.Size));
    if (!key || !Number.isFinite(size)) continue;
    const info: ObjectInfo = { path: key, size };
    const etag = stringValue(content.ETag);
    const lastModified = stringValue(content.LastModified);
    if (etag) info.etag = etag;
    if (lastModified) info.lastModified = new Date(lastModified);
    objects.push(info);
  }
  const nextContinuationToken = isTrue(result.IsTruncated)
    ? stringValue(result.NextContinuationToken)
    : "";
  return {
    objects,
    ...(nextContinuationToken ? { nextContinuationToken } : {}),
  };
}

interface ListObjectContent {
  Key?: unknown;
  Size?: unknown;
  ETag?: unknown;
  LastModified?: unknown;
}

function arrayOf<T>(value: unknown): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? (value as T[]) : [value as T];
}

function stringValue(value: unknown): string {
  if (value === undefined || value === null) return "";
  return String(value);
}

function isTrue(value: unknown): boolean {
  return value === true || String(value).toLowerCase() === "true";
}

function ensureSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

function encodeObjectPath(path: string): string {
  if (/^(?:[a-z][a-z0-9+.-]*:)?\/\//iu.test(path) || path.startsWith("/")) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", `Object path must be relative: ${path}`, {
      path,
    });
  }
  if (path === "") return "";
  return path
    .split("/")
    .map((segment) => {
      let decoded: string;
      try {
        decoded = decodeURIComponent(segment);
      } catch {
        throw new LaQLError("LAQL_VALIDATION_ERROR", `Object path has invalid encoding: ${path}`, {
          path,
        });
      }
      if (decoded === "." || decoded === "..") {
        throw new LaQLError("LAQL_VALIDATION_ERROR", `Object path contains traversal: ${path}`, {
          path,
        });
      }
      return encodeURIComponent(segment);
    })
    .join("/");
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

function timestamp(date: Date): string {
  return date.toISOString().replace(/[:-]|\.\d{3}/gu, "");
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
