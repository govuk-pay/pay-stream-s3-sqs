/* eslint-disable @typescript-eslint/ban-ts-comment */
// using `esModuleInterop` (import SQS as) causes jest mocks to fail; force compatability with common modules

import { SQS } from '@aws-sdk/client-sqs';

import { Message, ProgressPage } from '../providers/Provider'

const SQSMock = SQS as unknown as jest.Mock<SQS>

const sendMessageBatch = jest.fn()
// @ts-ignore
SQSMock.mockImplementation(() => ({ sendMessageBatch }))

import SQSConsumer from './SQSConsumer'

jest.mock('@aws-sdk/client-sqs')

beforeEach(() => {
	sendMessageBatch.mockClear()
})

describe('splitting batches utility', () => {
	test('returns correct number of batches', () => {
		const sqs = new SQSConsumer('some-queue-url')

		const someMessages: Message[] = [
			{ 'id': '1' },
			{ 'id': '2' },
			{ 'id': '3' },
			{ 'id': '4' },
			{ 'id': '5' }
		]
		const batches = sqs.splitBatches(someMessages, 2)
		expect(SQSMock).toHaveBeenCalledTimes(1)
		expect(batches).toHaveLength(3)
		expect(batches[0]).toHaveLength(2)
		expect(batches[1]).toHaveLength(2)
		expect(batches[2]).toHaveLength(1)
		expect(batches[0][0].id).toBe('1')
		expect(batches[2][0].id).toBe('5')
	})

	test('directly returns batch with less messages than chunk size', () => {
		const sqs = new SQSConsumer('some-queue-url')

		const someMessages: Message[] = [
			{ 'id': '1' }
		]
		const batches = sqs.splitBatches(someMessages, 2)
		expect(SQSMock).toHaveBeenCalledTimes(1)
		expect(batches[0]).toStrictEqual(someMessages)
	})
})

test('responding to page sends multiple batches of messages', () => {
	const sqs = new SQSConsumer('some-queue-url')

	const progressPage: ProgressPage = {
		progress: 100,
		messages: [
			{ 'id': '1' },
			{ 'id': '2' },
			{ 'id': '3' },
			{ 'id': '4' },
			{ 'id': '5' },
			{ 'id': '6' },
			{ 'id': '7' },
			{ 'id': '8' },
			{ 'id': '9' },
			{ 'id': '10' },
			{ 'id': '11' },
			{ 'id': '12' },
			{ 'id': '13' }
		]
	}

	sqs.receiveMessageBatchFromStream(progressPage, 'utf-8', () => {})
	expect(sendMessageBatch).toHaveBeenCalledTimes(2)
})

test('rejects consumer without valid queue url', () => {
	const invalidInitialise = () => new SQSConsumer(null)
	expect(invalidInitialise).toThrowError('Invalid SQS consumer params')
})
