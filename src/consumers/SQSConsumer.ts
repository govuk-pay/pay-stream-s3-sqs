import { Writable } from 'stream'
import crypto from 'crypto'
import { SendMessageBatchCommandInput, SQS, SQSServiceException } from '@aws-sdk/client-sqs';
import DefaultSQSMessage from './../transformers/DefaultSQSMessage'
import Consumer from './Consumer'
import { Message, ProgressPage } from './../providers/Provider'
import Transformer from './../transformers/Transformer'
import logger from '../utils/logger'

const sqs = new SQS()

export default class SQSConsumer implements Consumer {
	queueUrl: string
	messageBuilder: Transformer

	constructor(queueUrl: string, messageBuilder?: Transformer) {
		if (!queueUrl) {
			throw new Error('Invalid SQS consumer params')
		}
		this.queueUrl = queueUrl
		this.messageBuilder = messageBuilder || new DefaultSQSMessage()
	}

	writer(): Writable {
		logger.info('Consumer: using AWS SQS', { 'queue_url': this.queueUrl })
		return new Writable({ objectMode: true, write: this.receiveMessageBatchFromStream.bind(this) })
	}

	receiveMessageBatchFromStream(page: ProgressPage, encoding: string, done: (error?: Error) => void): void {
		this.sendMessageBatch(page.messages, page.progress)
		done()
	}

	sendMessageBatch(messages: Message[], currentProgress: number): void {
		const batchId = crypto.randomBytes(6).toString('hex')
		const batches = this.splitBatches(messages, 10)

		logger.info('Consumer: received message stream', {
			'batch_id': batchId,
			'total_batch_size': messages.length
		})

		// limit messages that will be sent to SQS in each batch; AWS limits 10 messages per request
		for (const batch of batches) {
			const params: SendMessageBatchCommandInput = {
				QueueUrl: this.queueUrl,
				Entries: [ ...batch ].map(this.messageBuilder.transform)
			}
			sqs.sendMessageBatch(params, (error: SQSServiceException, data) => {
				if (error) {
					logger.error('Consumer: failed to emit message batch', {
						'progress': currentProgress,
						'batch_id': batchId,
						'message': error.message,
						'name': error.name,
						'request_id': error.$metadata.requestId,
						'region': sqs.config.region
					})
					throw new Error(`SQSConsumer failed to process at [progress=${currentProgress}]`)
				}
				logger.info('Consumer: emitted message batch', {
					'batch_segment_size': batch.length,
					'batch_id': batchId,
					'messages': batch.map((entry, index) => ({
						'id': params.Entries[index].Id,
					})),
					'failed': data.Failed.map((entry) => ({
						'id': entry.Id,
						'code': entry.Code,
						'message': entry.Message
					})),
					'successful': data.Successful.map((entry) => ({
						'id': entry.Id,
						'sqs_message_id': entry.MessageId
					}))
				})
			})
		}
	}

	splitBatches(messages: Message[], chunkSize: number): Message[][] {
		const batches: Message[][] = []
		for (let i = 0; i < messages.length; i += chunkSize) {
			batches.push(messages.slice(i, i + chunkSize))
		}
		return batches
	}
}
