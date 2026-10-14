"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const ghost_storage_base_1 = __importDefault(require("ghost-storage-base"));
const client_s3_1 = require("@aws-sdk/client-s3");
const fs_1 = require("fs");
const path_1 = require("path");

const MIME_MAP = {
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
    '.gif': 'image/gif', '.webp': 'image/webp', '.svg': 'image/svg+xml',
    '.avif': 'image/avif', '.ico': 'image/x-icon',
    '.mp4': 'video/mp4', '.webm': 'video/webm', '.ogv': 'video/ogg',
    '.mp3': 'audio/mpeg', '.wav': 'audio/wav', '.ogg': 'audio/ogg', '.m4a': 'audio/mp4',
    '.pdf': 'application/pdf', '.zip': 'application/zip', '.json': 'application/json',
    '.webmanifest': 'application/manifest+json'
};

function getContentType(filePath, explicitType) {
    if (explicitType) return explicitType;
    if (filePath && filePath.includes('/format/webp/')) return 'image/webp';
    if (filePath && filePath.includes('/format/avif/')) return 'image/avif';
    const ext = (0, path_1.extname)(filePath || '').toLowerCase();
    return MIME_MAP[ext] || 'application/octet-stream';
}

const stripLeadingAndTrailingSlashes = (str) => str.replace(/^\/+|\/+$/g, '');
const stripEndingSlash = (str) => str.replace(/\/+$/, '');

function isNotFoundError(err) {
    if (!err) return false;
    const meta = err.$metadata;
    return Boolean(
        err.name === 'NoSuchKey' ||
        err.name === 'NotFound' ||
        err.Code === 'NoSuchKey' ||
        (meta && meta.httpStatusCode === 404)
    );
}

class S3Storage extends ghost_storage_base_1.default {
    constructor(config) {
        var _a;
        super();
        this.client = new client_s3_1.S3Client({
            region: config.region || 'auto',
            credentials: config.accessKeyId
                ? {
                    accessKeyId: config.accessKeyId,
                    secretAccessKey: config.secretAccessKey,
                }
                : undefined,
            endpoint: config.endpoint,
            forcePathStyle: (_a = config.forcePathStyle) !== null && _a !== void 0 ? _a : Boolean(config.endpoint),
        });
        this.bucket = config.bucket;
        this.pathPrefix = stripLeadingAndTrailingSlashes(config.pathPrefix || '');
        const defaultHost = `https://s3.${config.region ? `${config.region}.` : ''}amazonaws.com/${config.bucket}`;
        this.host = stripEndingSlash(config.assetHost || defaultHost);
        this.serveMode = config.serveMode || 'direct';
    }

    urlToPath(url) {
        if (url && url.startsWith(this.host)) {
            return url.substring(this.host.length);
        }
        try {
            return new URL(url).pathname;
        }
        catch (e) {
            return url;
        }
    }

    resolveKey(fileName, targetDir) {
        const joined = targetDir
            ? (0, path_1.posix.join)(targetDir, fileName || '')
            : (fileName || '');
        const cleaned = stripLeadingAndTrailingSlashes(joined);
        if (!this.pathPrefix) return cleaned;
        if (cleaned === this.pathPrefix || cleaned.startsWith(this.pathPrefix + '/')) {
            return cleaned;
        }
        return stripLeadingAndTrailingSlashes((0, path_1.posix.join)(this.pathPrefix, cleaned));
    }

    async delete(fileName, targetDir) {
        const target = this.resolveKey(fileName, targetDir || this.getTargetDir(this.pathPrefix));
        try {
            await this.client.send(new client_s3_1.DeleteObjectCommand({
                Bucket: this.bucket,
                Key: target,
            }));
        }
        catch (e) {
            return false;
        }
        return true;
    }

    async exists(fileName, targetDir) {
        const key = this.resolveKey(fileName, targetDir);
        try {
            await this.client.send(new client_s3_1.HeadObjectCommand({
                Bucket: this.bucket,
                Key: key,
            }));
        }
        catch (e) {
            return false;
        }
        return true;
    }

    async save(image, targetDir) {
        const dir = targetDir || this.getTargetDir(this.pathPrefix);
        const targetRelative = await this.getUniqueFileName(image, dir);
        const key = this.resolveKey(targetRelative);
        const file = (0, fs_1.createReadStream)(image.path);
        await this.client.send(new client_s3_1.PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: file,
            ContentType: getContentType(image.name || image.path, image.type),
            CacheControl: 'public, max-age=31536000, immutable',
        }));
        return `${this.host}/${key}`;
    }

    async saveRaw(buffer, targetPath) {
        const key = this.resolveKey(targetPath);
        await this.client.send(new client_s3_1.PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: buffer,
            ContentType: getContentType(targetPath),
            CacheControl: 'public, max-age=31536000, immutable',
        }));
        return `${this.host}/${key}`;
    }

    serve() {
        return async (req, res, next) => {
            const key = this.resolveKey(decodeURIComponent(req.path));
            if (this.serveMode === 'redirect') {
                res.redirect(301, `${this.host}/${key}`);
                return;
            }
            try {
                const params = {
                    Bucket: this.bucket,
                    Key: key,
                };
                if (req.headers.range) {
                    params.Range = req.headers.range;
                }
                const out = await this.client.send(new client_s3_1.GetObjectCommand(params));
                const headers = {
                    'Cache-Control': 'public, max-age=31536000, immutable',
                    'Content-Type': out.ContentType || getContentType(key),
                    'Accept-Ranges': out.AcceptRanges || 'bytes',
                };
                if (out.ContentLength !== undefined) headers['Content-Length'] = String(out.ContentLength);
                if (out.ContentRange) headers['Content-Range'] = out.ContentRange;
                if (out.ETag) headers['ETag'] = out.ETag;

                res.status(out.ContentRange ? 206 : 200).set(headers);

                req.on('close', () => {
                    if (out.Body && typeof out.Body.destroy === 'function') {
                        out.Body.destroy();
                    }
                });
                out.Body.pipe(res);
            }
            catch (e) {
                if (isNotFoundError(e)) {
                    res.status(404).end();
                    return;
                }
                next(e);
            }
        };
    }

    async read(options = {}) {
        let path = (options.path || '').replace(/\/+$/, '');
        if (path.startsWith(this.host)) {
            path = path.substring(this.host.length);
        }
        const key = this.resolveKey(path);
        try {
            const out = await this.client.send(new client_s3_1.GetObjectCommand({
                Bucket: this.bucket,
                Key: key,
            }));
            const data = await out.Body.transformToByteArray();
            return Buffer.from(data);
        }
        catch (err) {
            if (isNotFoundError(err)) {
                err.errorType = 'NotFoundError';
                err.statusCode = 404;
            }
            throw err;
        }
    }
}
exports.default = S3Storage;
module.exports = S3Storage;
