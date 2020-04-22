import net = require('net')
import ByteBuffer = require('bytebuffer')
import { MessageCodes } from './messageCodes'
import { encode } from './antidote'

const counters: { [counterId: string]: number } = {}

export class MultidoteServer {
	private server: net.Server | undefined
	private buffer: ByteBuffer = new ByteBuffer()

	constructor(port: number) {
		this.server = net
			.createServer(socket => {
				socket.on('data', data => this.onData(data, socket))
			})
			.listen(port)
	}

	public close() {
		if (this.server) {
			this.server.close()
			this.server = undefined
		}
	}

	private onData(data: Buffer, socket: net.Socket) {
		this.buffer.append(data)
		this.buffer.flip()
		this.readMessagesFromBuffer(socket)
	}

	private readMessagesFromBuffer(socket: net.Socket) {
		let buffer = this.buffer
		while (buffer.remaining() >= 4) {
			buffer.mark()
			let messageLength = buffer.readUint32()

			// See if we have the complete message
			if (buffer.remaining() < messageLength) {
				// rewind the offset
				buffer.reset()
				break
			}
			// We have a complete message from riak
			let slice = buffer.slice(undefined, buffer.offset + messageLength)
			let code = slice.readUint8()
			let decoded: any = null
			if (messageLength > 1) {
				let ResponseProto = MessageCodes.messageCodeToProto(code)
				// GH issue #45
				// Must use 'true' as argument to force copy of data
				// otherwise, subsequent fetches will clobber data
				decoded = ResponseProto.decode(slice.toBuffer(true))
			}
			this.handleRequest(code, decoded, socket)

			// skip past message in buffer
			buffer.skip(messageLength)
			// repeat until we are out of messages
		}

		// ByteBuffer's 'flip()' effectively clears the buffer which we don't
		// want. We want to flip while preserving anything in the buffer and
		// compact if necessary.

		let newOffset = buffer.remaining()
		// Compact if necessary
		if (newOffset > 0 && buffer.offset !== 0) {
			buffer.copyTo(buffer, 0)
		}
		buffer.offset = newOffset
		buffer.limit = buffer.capacity()
	}

	private handleRequest(code: number, decoded: any, socket: net.Socket) {
		if (code === MessageCodes.apbStaticUpdateObjects) {
			if (counters[decoded.updates[0].boundobject.key.toUTF8()] === undefined)
				counters[decoded.updates[0].boundobject.key.toUTF8()] = 0
			counters[decoded.updates[0].boundobject.key.toUTF8()] +=
				decoded.updates[0].operation.counterop.inc.low

			let apbStartTransactionResp =
				MessageCodes.antidotePb.ApbStartTransactionResp
			let message: AntidotePB.ApbStartTransactionMessage = new apbStartTransactionResp(
				{ success: true }
			)
			this.sendResponse(
				MessageCodes.apbStartTransactionResp,
				encode(message),
				socket
			)
		} else console.log(code)
	}

	private sendResponse(
		messageCode: number,
		encodedMessage: ArrayBuffer,
		socket: net.Socket
	): void {
		// For now, ignore disconnected clients
		// if (!this.socket) {
		// 	// try to reconnect:
		// 	this.reconnect()
		// }
		if (!this.server) {
			console.log('Could not access socket.')
			return
		}

		let header = Buffer.alloc(5)
		header.writeInt32BE(encodedMessage.byteLength + 1, 0)
		header.writeUInt8(messageCode, 4)

		let msg = Buffer.concat([header, Buffer.from(encodedMessage)])
		socket.write(msg)
	}
}
