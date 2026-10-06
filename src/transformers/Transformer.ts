import { SendMessageBatchRequestEntry } from '@aws-sdk/client-sqs';
export default interface Transformer {
	transform(message: { [key: string]: string }): SendMessageBatchRequestEntry;
}
