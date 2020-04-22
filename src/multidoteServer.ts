import net = require('net')
import ByteBuffer = require('bytebuffer')
import { MessageCodes } from './messageCodes'

const counters: { [counterId: string]: number } = {}

export class MultidoteServer {
	private socket: net.Socket | undefined
	private buffer: ByteBuffer = new ByteBuffer()
	private port: number

	constructor(port: number) {
		this.port = port

		net
			.createServer(socket => {
				socket.on('data', data => this.onData(data))
			})
			.listen(port)
	}

	public close() {
		if (this.socket) {
			this.socket.destroy()
			this.socket = undefined
		}
	}

	private onData(data: Buffer) {
		this.buffer.append(data)
		this.buffer.flip()
		this.readMessagesFromBuffer()
	}

	private readMessagesFromBuffer() {
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
			this.handleRequest(code, decoded)

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

	private handleRequest(code: number, decoded: any) {
		if (code === MessageCodes.apbStaticUpdateObjects) {
			if (counters[decoded.updates[0].boundobject.key.toUTF8()] === undefined)
				counters[decoded.updates[0].boundobject.key.toUTF8()] = 0
			counters[decoded.updates[0].boundobject.key.toUTF8()] +=
				decoded.updates[0].operation.counterop.inc.low
		} else console.log(code)
	}
}
