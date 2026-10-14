import BaseAdapter, { Image, ReadOptions } from 'ghost-storage-base'
import {
  DeleteObjectCommand,
  GetObjectCommand,
  GetObjectCommandInput,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3'
import { createReadStream } from 'fs'
import { extname, posix } from 'path'
import type { Handler } from 'express'
import type { Readable } from 'stream'

type Config = {
  accessKeyId?: string
  assetHost?: string
  bucket?: string
  pathPrefix?: string
  region?: string
  secretAccessKey?: string
  endpoint?: string
  forcePathStyle?: boolean
  serveMode?: 'direct' | 'redirect'
}

const MIME_MAP: Record<string, string> = {
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.png': 'image/png',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml',
  '.avif': 'image/avif',
  '.ico': 'image/x-icon',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm',
  '.ogv': 'video/ogg',
  '.mp3': 'audio/mpeg',
  '.wav': 'audio/wav',
  '.ogg': 'audio/ogg',
  '.m4a': 'audio/mp4',
  '.pdf': 'application/pdf',
  '.zip': 'application/zip',
  '.json': 'application/json',
  '.webmanifest': 'application/manifest+json',
}

function getContentType(filePath?: string, explicitType?: string): string {
  if (explicitType) return explicitType
  if (filePath && filePath.includes('/format/webp/')) return 'image/webp'
  if (filePath && filePath.includes('/format/avif/')) return 'image/avif'
  const ext = extname(filePath || '').toLowerCase()
  return MIME_MAP[ext] || 'application/octet-stream'
}

const stripLeadingAndTrailingSlashes = (str: string) =>
  str.replace(/^\/+|\/+$/g, '')
const stripEndingSlash = (str: string) => str.replace(/\/+$/, '')

function isNotFoundError(err: any): boolean {
  if (!err) return false
  return Boolean(
    err.name === 'NoSuchKey' ||
      err.name === 'NotFound' ||
      err.Code === 'NoSuchKey' ||
      (err.$metadata && err.$metadata.httpStatusCode === 404)
  )
}

export default class S3Storage extends BaseAdapter {
  private client: S3Client
  private bucket: string
  private host: string
  private serveMode: 'direct' | 'redirect'

  constructor(config: Config) {
    super()
    this.client = new S3Client({
      region: config.region || 'auto',
      credentials:
        config.accessKeyId && config.secretAccessKey
          ? {
              accessKeyId: config.accessKeyId,
              secretAccessKey: config.secretAccessKey,
            }
          : undefined,
      endpoint: config.endpoint,
      forcePathStyle: config.forcePathStyle ?? Boolean(config.endpoint),
    })
    if (!config.bucket) throw new Error('S3 bucket not specified')
    this.bucket = config.bucket
    this.pathPrefix = stripLeadingAndTrailingSlashes(config.pathPrefix || '')
    const defaultHost = `https://s3.${config.region ? `${config.region}.` : ''}amazonaws.com/${config.bucket}`
    this.host = stripEndingSlash(config.assetHost || defaultHost)
    this.serveMode = config.serveMode || 'direct'
  }

  urlToPath(url: string): string {
    if (url && url.startsWith(this.host)) {
      return url.substring(this.host.length)
    }
    try {
      return new URL(url).pathname
    } catch {
      return url
    }
  }

  resolveKey(fileName?: string, targetDir?: string): string {
    const joined = targetDir
      ? posix.join(targetDir, fileName || '')
      : fileName || ''
    const cleaned = stripLeadingAndTrailingSlashes(joined)
    if (!this.pathPrefix) return cleaned
    if (
      cleaned === this.pathPrefix ||
      cleaned.startsWith(this.pathPrefix + '/')
    ) {
      return cleaned
    }
    return stripLeadingAndTrailingSlashes(posix.join(this.pathPrefix, cleaned))
  }

  async delete(fileName: string, targetDir?: string): Promise<boolean> {
    const target = this.resolveKey(
      fileName,
      targetDir || this.getTargetDir(this.pathPrefix)
    )
    try {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: this.bucket,
          Key: target,
        })
      )
    } catch {
      return false
    }
    return true
  }

  async exists(fileName: string, targetDir?: string): Promise<boolean> {
    const key = this.resolveKey(fileName, targetDir)
    try {
      await this.client.send(
        new HeadObjectCommand({
          Bucket: this.bucket,
          Key: key,
        })
      )
    } catch {
      return false
    }
    return true
  }

  async save(image: Image, targetDir?: string): Promise<string> {
    const dir = targetDir || this.getTargetDir(this.pathPrefix)
    const targetRelative = await this.getUniqueFileName(image, dir)
    const key = this.resolveKey(targetRelative)
    const file = createReadStream(image.path)
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: file,
        ContentType: getContentType(image.name || image.path, image.type),
        CacheControl: 'public, max-age=31536000, immutable',
      })
    )
    return `${this.host}/${key}`
  }

  async saveRaw(buffer: Buffer, targetPath: string): Promise<string> {
    const key = this.resolveKey(targetPath)
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: buffer,
        ContentType: getContentType(targetPath),
        CacheControl: 'public, max-age=31536000, immutable',
      })
    )
    return `${this.host}/${key}`
  }

  serve(): Handler {
    return async (req, res, next) => {
      const key = this.resolveKey(decodeURIComponent(req.path))
      if (this.serveMode === 'redirect') {
        res.redirect(301, `${this.host}/${key}`)
        return
      }
      try {
        const params: GetObjectCommandInput = {
          Bucket: this.bucket,
          Key: key,
        }
        if (req.headers.range) {
          params.Range = req.headers.range
        }
        const out = await this.client.send(new GetObjectCommand(params))
        const headers: Record<string, string> = {
          'Cache-Control': 'public, max-age=31536000, immutable',
          'Content-Type': out.ContentType || getContentType(key),
          'Accept-Ranges': out.AcceptRanges || 'bytes',
        }
        if (out.ContentLength !== undefined)
          headers['Content-Length'] = String(out.ContentLength)
        if (out.ContentRange) headers['Content-Range'] = out.ContentRange
        if (out.ETag) headers['ETag'] = out.ETag

        res.status(out.ContentRange ? 206 : 200).set(headers)

        const stream = out.Body as Readable
        req.on('close', () => {
          if (stream && typeof stream.destroy === 'function') {
            stream.destroy()
          }
        })
        stream.pipe(res)
      } catch (e) {
        if (isNotFoundError(e)) {
          res.status(404).end()
          return
        }
        next(e)
      }
    }
  }

  async read(options: ReadOptions = { path: '' }): Promise<Buffer> {
    let path = (options.path || '').replace(/\/+$/, '')
    if (path.startsWith(this.host)) {
      path = path.substring(this.host.length)
    }
    const key = this.resolveKey(path)
    try {
      const out = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
        })
      )
      const data = await out.Body?.transformToByteArray()
      return Buffer.from(data || [])
    } catch (err: any) {
      if (isNotFoundError(err)) {
        err.errorType = 'NotFoundError'
        err.statusCode = 404
      }
      throw err
    }
  }
}

module.exports = S3Storage
