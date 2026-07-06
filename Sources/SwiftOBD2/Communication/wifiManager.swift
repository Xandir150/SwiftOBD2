//
//  wifiManager.swift
//
//
//  Created by kemo konteh on 2/26/24.
//

import CoreBluetooth
import Foundation
import Network
import OSLog

protocol CommProtocol {
    func sendCommand(_ command: String, retries: Int) async throws -> [String]
    func disconnectPeripheral()
    func connectAsync(timeout: TimeInterval, peripheral: CBPeripheral?) async throws
    func scanForPeripherals() async throws
    var connectionStatePublisher: Published<ConnectionState>.Publisher { get }
    var obdDelegate: OBDServiceDelegate? { get set }
}

enum CommunicationError: Error {
    case invalidData
    case errorOccurred(Error)
}

class WifiManager: CommProtocol {
    @Published var connectionState: ConnectionState = .disconnected

    let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "com.example.app", category: "wifiManager")

    var obdDelegate: OBDServiceDelegate?

    var connectionStatePublisher: Published<ConnectionState>.Publisher { $connectionState }

    var tcp: NWConnection?

    func connectAsync(timeout _: TimeInterval, peripheral _: CBPeripheral? = nil) async throws {
        let host = NWEndpoint.Host("192.168.0.10")
        guard let port = NWEndpoint.Port("35000") else {
            throw CommunicationError.invalidData
        }
        tcp = NWConnection(host: host, port: port, using: .tcp)

        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            tcp?.stateUpdateHandler = { [weak self] newState in
                guard let self = self else { return }
                switch newState {
                case .ready:
                    self.logger.info("Connected to \(host.debugDescription):\(port.debugDescription)")
                    self.connectionState = .connectedToAdapter
                    continuation.resume(returning: ())
                case let .waiting(error):
                    self.logger.warning("Connection waiting: \(error.localizedDescription)")
                case let .failed(error):
                    self.logger.error("Connection failed: \(error.localizedDescription)")
                    self.connectionState = .disconnected
                    continuation.resume(throwing: CommunicationError.errorOccurred(error))
                default:
                    break
                }
            }
            tcp?.start(queue: .main)
        }
    }

    func sendCommand(_ command: String, retries: Int) async throws -> [String] {
        guard let data = "\(command)\r".data(using: .ascii) else {
            throw CommunicationError.invalidData
        }
        logger.info("Sending: \(command)")
        return try await sendCommandInternal(data: data, retries: retries)
    }

    private func sendCommandInternal(data: Data, retries: Int) async throws -> [String] {
        for attempt in 1 ... retries {
            do {
                let response = try await sendAndReceiveData(data)
                if let lines = processResponse(response) {
                    return lines
                } else if attempt < retries {
                    logger.info("No data received, retrying attempt \(attempt + 1) of \(retries)...")
                    try await Task.sleep(nanoseconds: 100_000_000) // 0.5 seconds delay
                }
            } catch {
                if attempt == retries {
                    throw error
                }
                logger.warning("Attempt \(attempt) failed, retrying: \(error.localizedDescription)")
            }
        }
        throw CommunicationError.invalidData
    }

    private func sendAndReceiveData(_ data: Data) async throws -> String {
        guard let tcpConnection = tcp else {
             throw CommunicationError.invalidData
         }
        let logger = self.logger // Avoid capturing `self` directly

        // ELM327 terminates every response with a '>' prompt. A single receive() returns
        // after the FIRST TCP segment, which on many WiFi clones contains only the command
        // echo — the actual payload (and the "OK" the init sequence looks for) arrives in
        // subsequent segments. Accumulate until the prompt shows up (or a deadline passes),
        // otherwise responses interleave: each read returns the previous command's tail.
        return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<String, Error>) in
            var resumed = false
            var buffer = ""
            let deadline = DispatchTime.now() + .seconds(5)

            func resumeOnce(_ result: Result<String, Error>) {
                guard !resumed else { return }
                resumed = true
                switch result {
                case .success(let value): continuation.resume(returning: value)
                case .failure(let error): continuation.resume(throwing: error)
                }
            }

            // Overall deadline: a clone that never sends '>' shouldn't hang the caller.
            // Connection runs on the main queue (see connectAsync), so this and the
            // receive callbacks are serialized — the plain `resumed` flag is safe.
            DispatchQueue.main.asyncAfter(deadline: deadline) {
                if buffer.isEmpty {
                    logger.warning("Receive timed out with no data")
                    resumeOnce(.failure(CommunicationError.invalidData))
                } else {
                    logger.warning("Receive timed out without '>' prompt, returning partial buffer")
                    resumeOnce(.success(buffer))
                }
            }

            func receiveChunk() {
                tcpConnection.receive(minimumIncompleteLength: 1, maximumLength: 500) { data, _, isComplete, error in
                    if let error = error {
                        logger.error("Error receiving data: \(error.localizedDescription)")
                        resumeOnce(.failure(CommunicationError.errorOccurred(error)))
                        return
                    }
                    if let data, let chunk = String(data: data, encoding: .utf8) {
                        buffer += chunk
                    }
                    if buffer.contains(">") {
                        resumeOnce(.success(buffer))
                    } else if isComplete {
                        // Peer closed the connection — return whatever arrived.
                        if buffer.isEmpty {
                            resumeOnce(.failure(CommunicationError.invalidData))
                        } else {
                            resumeOnce(.success(buffer))
                        }
                    } else if !resumed {
                        receiveChunk()
                    }
                }
            }

            tcpConnection.send(content: data, completion: .contentProcessed { error in
                if let error = error {
                    logger.error("Error sending data: \(error.localizedDescription)")
                    resumeOnce(.failure(CommunicationError.errorOccurred(error)))
                    return
                }
                receiveChunk()
            })
        }
    }

    private func processResponse(_ response: String) -> [String]? {
        logger.info("Processing response: \(response)")
        var lines = response.components(separatedBy: .newlines).filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }

        guard !lines.isEmpty else {
            logger.warning("Empty response lines")
            return nil
        }

        if lines.last?.contains(">") == true {
            lines.removeLast()
        }

        if lines.first?.lowercased() == "no data" {
            return nil
        }

        return lines
    }

    func disconnectPeripheral() {
        tcp?.cancel()
    }

    func scanForPeripherals() async throws {}
}
