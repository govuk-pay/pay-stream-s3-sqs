import crypto from 'crypto'
import { SendMessageBatchRequestEntry } from '@aws-sdk/client-sqs';
import Transformer from './Transformer'
import { Message } from './../providers/Provider'

export default class DefaultSQSMessage implements Transformer {
	transform(message: Message): SendMessageBatchRequestEntry {
		return {
			Id: crypto.randomBytes(16).toString('hex'),
			MessageBody: JSON.stringify(message)
		}
	}
}
