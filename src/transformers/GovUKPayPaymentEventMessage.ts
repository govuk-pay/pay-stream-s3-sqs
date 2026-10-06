import crypto from 'crypto'
import { SendMessageBatchRequestEntry } from '@aws-sdk/client-sqs';
import Transformer from './Transformer'
import { Message, MessageValue } from './../providers/Provider'

type PaymentEventMessageValue = string | boolean
type PaymentEventDetails = { [key: string]: PaymentEventMessageValue | { [key: string]: PaymentEventMessageValue } }

const DEBUG: boolean = process.env['DEBUG'] === 'true' || false

interface PaymentEventMessage {
	event_details: PaymentEventDetails,
	resource_type?: string;
	resource_external_id?: string;
	event_date?: string;
	event_type?: string;
	parent_resource_external_id?: string;
	reproject_domain_object?: boolean;
	service_id?: string;
	live?: string;
	[key: string]: PaymentEventMessageValue | PaymentEventDetails
}

interface ReservedKeys {
	key: string,
	target: string,
	targetBoolean?: boolean
}

// we can gaurantee the existence of required fields as anything with permissions
// to this resource will be validating data entry
function formatPaymentEventMessage(message: Message): PaymentEventMessage {
	if (DEBUG) {
		console.log(`Transformer incoming message: ${JSON.stringify(message)}`)
	}

	const reservedKeys: ReservedKeys[] = [
		{ key: 'transaction_id', target: 'resource_external_id' },
		{ key: 'parent_transaction_id', target: 'parent_resource_external_id' },
		{ key: 'transaction_type', target: 'resource_type' },
		{ key: 'event_date', target: 'timestamp' },
		{ key: 'event_name', target: 'event_type' },
		{ key: 'reproject_domain_object', target: 'reproject_domain_object', targetBoolean: true },
		{ key: 'service_id', target: 'service_id' },
		{ key: 'live', target:'live', targetBoolean: true}
	]
	const formatted: PaymentEventMessage = { event_details: {} }

	// initially extract the reserved properties
	for (const reserved of reservedKeys) {
		if (!Object.hasOwn(message, reserved.key)) {
			continue
		}

		const reservedEntry: MessageValue = message[reserved.key].trim()

		if (!reservedEntry) {
			continue
		}

		if (reserved.targetBoolean) {
			formatted[reserved.target] = reservedEntry.toLocaleLowerCase() === 'true'
		} else {
			formatted[reserved.target] = reservedEntry
		}

		delete message[reserved.key]
	}

	// any remaining properties will override attributes of the transaction itself
	// put these in `event_data`
	for (const [ eventMessageKey, eventMessageValue ] of Object.entries(message)) {
		const trimmedMessageValue: MessageValue = eventMessageValue.trim()

		if (trimmedMessageValue.length === 0) {
			continue
		}

		// support only 1 level of nesting for second level attributes
		if (eventMessageKey.includes('.')) {
			const [ topLevelKey, nestedKey ] = eventMessageKey.split('.')
			formatted.event_details[topLevelKey] = {
				[nestedKey]: parseMessageValue(trimmedMessageValue)
			}
		} else {
			formatted.event_details[eventMessageKey] = parseMessageValue(trimmedMessageValue)
		}
	}

	if (DEBUG) {
		console.log(`Transformer outgoing formatted message: ${JSON.stringify(formatted)}`)
	}
	return formatted
}

function parseMessageValue(messageValue: MessageValue): PaymentEventMessageValue {
	if (messageValue.toLocaleLowerCase() === 'true' || messageValue.toLocaleLowerCase() === 'false') {
		return messageValue.toLocaleLowerCase() === 'true'
	}

	return messageValue
}

export default class GovUkPayPaymentEventMessage implements Transformer {
	transform(message: Message): SendMessageBatchRequestEntry {
		return {
			Id: crypto.randomBytes(16).toString('hex'),
			MessageBody: JSON.stringify(
				formatPaymentEventMessage(message)
			)
		}
	}
}
