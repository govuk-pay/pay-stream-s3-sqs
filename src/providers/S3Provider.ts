import { Readable } from 'stream'
import logger from './../utils/logger'

import {
	S3,
	S3ServiceException,
	SelectObjectContentCommand,
	SelectObjectContentCommandInput,
	SelectObjectContentCommandOutput,
} from '@aws-sdk/client-s3'

import { Provider, Message, ProgressPage } from './Provider'

const s3 = new S3()

export default class S3Provider implements Provider {
	bucketName: string
	sourceFile: string

	constructor(bucketName: string, sourceFile: string) {
		if (!bucketName || !sourceFile) {
			throw new Error('Invalid S3 provider params')
		}

		logger.info('Provider: using AWS S3', { 'bucket_name': bucketName, 'source_file': sourceFile })
		this.bucketName = bucketName
		this.sourceFile = sourceFile
	}

	stream(): Readable {
		const readableStream = new Readable({ objectMode: true, read(): void {} })

		this.getSourceFileContentLength()
			.then((totalFileSizeInBytes) => this.fetch(readableStream, totalFileSizeInBytes))
			.catch((error: S3ServiceException) => {
				logger.error('Provider: failed to provide messages from S3', {
					'name': error.name,
					'status_code': error.$metadata.httpStatusCode,
					'request_id': error.$metadata.requestId,
					'aws_message': error.message
				})
			})
		return readableStream
	}

	async getSourceFileContentLength(): Promise<number> {
		const targetFileHeaderObject = await s3.headObject({ Bucket: this.bucketName, Key: this.sourceFile})

		logger.info('Provider: fetched source file header info', {
			'content_length': targetFileHeaderObject.ContentLength,
			'content_type': targetFileHeaderObject.ContentType,
			'last_modified': targetFileHeaderObject.LastModified,
			'accept_range': targetFileHeaderObject.AcceptRanges
		})
		return targetFileHeaderObject.ContentLength
	}

	async fetch(stream: Readable, totalFileSizeInBytes: number): Promise<void> {
		const PAGE_SIZE_BYTES = 1000
		let bytesFrom = 0

		do {
			const targetBytes = bytesFrom + PAGE_SIZE_BYTES
			const messages = await this.fetchPage(bytesFrom, targetBytes)
			bytesFrom = targetBytes
			stream.push({
				messages,
				progress: bytesFrom
			} as ProgressPage)
		} while (bytesFrom < totalFileSizeInBytes)
		logger.info('Provider: completed fetching all rows from source file')
	}

	async fetchPage(bytesFrom = 0, targetBytes: number): Promise<Message[]> {
		const filePageRequestParams: SelectObjectContentCommandInput = {
			Bucket: this.bucketName,
			Key: this.sourceFile,
			InputSerialization: {
				CSV: {
					FileHeaderInfo: 'USE',
					RecordDelimiter: '\n',
					FieldDelimiter: ','
				},
			},
			OutputSerialization: {
				JSON: {
					RecordDelimiter: ','
				}
			},
			ScanRange: {
				Start: bytesFrom,
				End: targetBytes
			},
			Expression: 'SELECT * FROM S3Object',
			ExpressionType: 'SQL'
		}

		try {
			const response = await s3.send(new SelectObjectContentCommand(filePageRequestParams))
			return await this.processResponse(response)
		} catch (error) {
			logger.error('Provider: failed to fetch page', {
				'bytes_from': bytesFrom,
				'bytes_to': targetBytes,
				'source_file': this.sourceFile
			})

			throw error
		}
	}

	async processResponse(response: SelectObjectContentCommandOutput): Promise<Message[]> {
		const data: Message[] = []
		for await (const event of response.Payload) {
			if (event.Records?.Payload) {
				const text = new TextDecoder('utf-8').decode(event.Records.Payload)
				const messages = this.parseS3SelectBytes(text)
				data.push(...messages)
			}
		}

		return data
	}

	parseS3SelectBytes(selectedContent: string): Message[] {
		const result = selectedContent.replace(/(,$)/g, '')
		return JSON.parse(`[${result}]`)
	}
}
