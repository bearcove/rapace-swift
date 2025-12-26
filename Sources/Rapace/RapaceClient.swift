import Foundation
import Network
import Postcard

/// Errors that can occur during RPC
public enum RapaceError: Error, CustomStringConvertible {
    case connectionFailed(String)
    case sendFailed(String)
    case receiveFailed(String)
    case invalidResponse(String)
    case serverError(String)
    case timeout

    public var description: String {
        switch self {
        case .connectionFailed(let msg): return "Connection failed: \(msg)"
        case .sendFailed(let msg): return "Send failed: \(msg)"
        case .receiveFailed(let msg): return "Receive failed: \(msg)"
        case .invalidResponse(let msg): return "Invalid response: \(msg)"
        case .serverError(let msg): return "Server error: \(msg)"
        case .timeout: return "Request timed out"
        }
    }
}

/// A rapace RPC client over TCP
public actor RapaceClient {
    private let connection: TCPConnection
    private var nextMsgId: UInt64 = 1
    private var nextChannelId: UInt32 = 1

    /// Serial queue to ensure only one RPC call is in-flight at a time.
    /// This prevents actor reentrancy from causing request/response mismatches.
    private var callInProgress: Bool = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    public init(host: String, port: UInt16) async throws {
        self.connection = TCPConnection()
        try await connection.connect(host: host, port: port)
    }

    /// Acquire the call lock - waits if another call is in progress
    private func acquireCallLock() async {
        if callInProgress {
            await withCheckedContinuation { continuation in
                waiters.append(continuation)
            }
        }
        callInProgress = true
    }

    /// Release the call lock - resumes the next waiting caller if any
    private func releaseCallLock() {
        if let next = waiters.first {
            waiters.removeFirst()
            next.resume()
        } else {
            callInProgress = false
        }
    }

    /// Close the connection
    public func close() async {
        await connection.close()
    }

    /// Call an RPC method with raw request bytes, returning raw response bytes
    public func call(methodId: UInt32, requestPayload: [UInt8]) async throws -> [UInt8] {
        // Acquire lock to prevent actor reentrancy from interleaving requests/responses
        await acquireCallLock()
        defer { releaseCallLock() }

        // Debug: log the call
        let reqHex = requestPayload.map { String(format: "%02x", $0) }.joined(separator: " ")
        print("[RapaceClient] call methodId=0x\(String(methodId, radix: 16)), request=[\(reqHex)]")
        // Allocate message and channel IDs
        let msgId = nextMsgId
        nextMsgId += 1
        let channelId = nextChannelId
        nextChannelId += 1

        // Build the request descriptor
        var desc = MsgDescHot()
        desc.msgId = msgId
        desc.channelId = channelId
        desc.methodId = methodId
        desc.flags = FrameFlags.data  // DATA flag

        // Set payload length - payload is ALWAYS sent after descriptor on the wire
        desc.payloadSlot = inlinePayloadSlot
        desc.payloadLen = UInt32(requestPayload.count)

        // Serialize the frame
        // Wire format: [4-byte frame_len][64-byte desc][payload bytes]
        // frame_len = 64 + payload.len() (payload is always external on wire)
        let descData = desc.serialize()
        let frameLen = UInt32(64 + requestPayload.count)

        // Build the full frame: [4-byte length][64-byte desc][payload]
        var frame = Data()
        var len = frameLen.littleEndian
        frame.append(Data(bytes: &len, count: 4))
        frame.append(descData)
        if !requestPayload.isEmpty {
            frame.append(contentsOf: requestPayload)
        }

        // Send the frame
        try await connection.send(frame)

        // Read response: first 4 bytes for length
        let respLenData = try await connection.receive(exactly: 4)
        let respLen = respLenData.withUnsafeBytes { $0.load(as: UInt32.self).littleEndian }

        // Read the rest of the frame
        let respFrame = try await connection.receive(exactly: Int(respLen))

        // Parse response descriptor (first 64 bytes)
        guard respFrame.count >= 64 else {
            throw RapaceError.invalidResponse("Response frame too short: \(respFrame.count) bytes")
        }

        let respDesc = try MsgDescHot(from: respFrame.prefix(64))

        // Check for error flag
        if respDesc.flags.contains(.error) {
            throw RapaceError.serverError("Server returned error flag")
        }

        // Extract payload
        let payloadLen = Int(respDesc.payloadLen)
        if payloadLen == 0 {
            print("[RapaceClient] response payload empty")
            return []
        }

        let result: [UInt8]
        if respDesc.isInline {
            // Payload is in inline_payload field
            result = Array(respDesc.inlinePayloadData.prefix(payloadLen))
        } else {
            // Payload is after the 64-byte descriptor
            let payloadStart = 64
            let payloadEnd = payloadStart + payloadLen
            guard respFrame.count >= payloadEnd else {
                throw RapaceError.invalidResponse("Response payload truncated")
            }
            result = Array(respFrame[payloadStart..<payloadEnd])
        }

        // Debug: log the response
        let respHex = result.map { String(format: "%02x", $0) }.joined(separator: " ")
        print("[RapaceClient] response payload (\(result.count) bytes): [\(respHex)]")
        return result
    }
}

// MARK: - Helper to compute method IDs (FNV-1a)

/// Compute a rapace method ID from service and method names
public func computeMethodId(service: String, method: String) -> UInt32 {
    let fullName = "\(service).\(method)"

    // FNV-1a 64-bit
    var hash: UInt64 = 0xcbf29ce484222325
    let prime: UInt64 = 0x100000001b3

    for byte in fullName.utf8 {
        hash ^= UInt64(byte)
        hash = hash &* prime
    }

    // Fold to 32 bits
    return UInt32(truncatingIfNeeded: (hash >> 32) ^ hash)
}
