import {
	S3,
	SelectObjectContentCommand,
} from '@aws-sdk/client-s3'

import S3Provider from './S3Provider'

const s3Provider = new S3Provider('some-bucket', 'some-path')

describe('S3 content string parser', () => {
	test('strips always appended commas', () => {
		const exampleResult = '{"id": "5"},'
		const messages = s3Provider.parseS3SelectBytes(exampleResult)
		expect(messages).toHaveLength(1)
		expect(messages[0].id).toBe('5')
	})

	test('returns correct number of entries from content', () => {
		const exampleResult = '{"id": "5"},{"id": "6"},{"id": "7"},{"id": "8"}'
		const messages = s3Provider.parseS3SelectBytes(exampleResult)
		expect(messages).toHaveLength(4)
	})
})

describe('S3 page fetcher', () => {
	let sendSpy: jest.SpyInstance

	beforeEach(() => {
		sendSpy = jest.spyOn(S3.prototype, 'send')
	})

	afterEach(() => {
		sendSpy.mockRestore()
	})

	async function* fakePayload(chunks: string[]) {
		for (const chunk of chunks) {
			yield {
				Records: {
					Payload: new TextEncoder().encode(chunk)
				}
			}
		}
	}

	test('translates Uint8Array payloads into messages', async () => {
		sendSpy.mockResolvedValueOnce({
			Payload: fakePayload(['{"id":"1"},', '{"id":"2"},'])
		})

		const page = new S3Provider('some-bucket', 'some-file')
		const messages = await page.fetchPage(0, 1000)

		expect(messages).toEqual([{ id: '1' }, { id: '2' }])
	})


	test('errors are propagated by send()', async () => {
		sendSpy.mockRejectedValueOnce(new Error('network error'))

		const page = new S3Provider('some-bucket', 'some-file')

		await expect(page.fetchPage(0, 1000)).rejects.toThrow('network error')
	})
})
