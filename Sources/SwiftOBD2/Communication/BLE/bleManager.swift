// MARK: - BLEManager Class Documentation

/// The BLEManager class is a wrapper around the CoreBluetooth framework. It is responsible for managing the connection to the OBD2 adapter,
/// scanning for peripherals, and handling the communication with the adapter.
///
/// **Key Responsibilities:**
/// - Scanning for peripherals
/// - Connecting to peripherals
/// - Managing the connection state
/// - Handling the communication with the adapter
/// - Processing the characteristics of the adapter
/// - Sending messages to the adapter
/// - Receiving messages from the adapter
/// - Parsing the received messages
/// - Handling errors

import Combine
import CoreBluetooth
import Foundation

public enum ConnectionState: Sendable, Equatable {
    case disconnected
    case connecting
    case connectedToAdapter
    case connectedToVehicle
    case error

    public var description: String {
        switch self {
        case .disconnected: return "Disconnected"
        case .connecting: return "Connecting"
        case .connectedToAdapter: return "Connected to Adapter"
        case .connectedToVehicle: return "Connected to Vehicle"
        case .error: return "Error"
        }
    }

    public var isConnected: Bool {
        switch self {
        case .connectedToAdapter, .connectedToVehicle:
            return true
        default:
            return false
        }
    }
}

// MARK: - Constants
enum BLEConstants {
    static let defaultTimeout: TimeInterval = 3.0
    static let scanDuration: TimeInterval = 10.0
    static let connectionTimeout: TimeInterval = 10.0
    static let retryDelay: TimeInterval = 0.5
    static let maxBufferSize = 1024
    static let bluetoothPowerOnTimeout: TimeInterval = 30.0
    static let pollingInterval: UInt64 = 100_000_000 // 100ms in nanoseconds
}

class BLEManager: NSObject, CommProtocol, BLEPeripheralManagerDelegate {
    private let peripheralSubject = PassthroughSubject<CBPeripheral, Never>()
    // Replaced with centralized logging - see connectionStateDidChange for usage

    // MARK: Properties

    @Published var connectionState: ConnectionState = .disconnected

    var connectionStatePublisher: Published<ConnectionState>.Publisher { $connectionState }


    public weak var obdDelegate: OBDServiceDelegate?

    // Focused components
    private var centralManager: CBCentralManager!
    private var messageProcessor: BLEMessageProcessor!
    private var characteristicHandler: BLECharacteristicHandler!
    private var peripheralManager: BLEPeripheralManager!
    private var peripheralScanner: BLEPeripheralScanner!

    private var cancellables = Set<AnyCancellable>()

    // The peripheral a centralManager.connect() is in flight for, before
    // didConnect hands it to peripheralManager. Without it, a disconnect
    // during the connecting window has nothing to cancel: CoreBluetooth keeps
    // the attempt alive forever and the state machine stays .connecting,
    // failing every retry with .connectionInProgress.
    private var pendingConnectPeripheral: CBPeripheral?

    deinit {
        // Clean up resources
        cancellables.removeAll()
        disconnectPeripheral()
        obdDebug("BLEManager deinitialized", category: .bluetooth)
    }

    // MARK: - Initialization

    override init() {
        super.init()
        // Use background queue for better performance, but dispatch UI updates to main queue
        let bleQueue = DispatchQueue(label: "com.swiftobd2.ble", qos: .userInitiated)
        
        centralManager = CBCentralManager(
            delegate: self,
            queue: bleQueue,
            options: [
                CBCentralManagerOptionShowPowerAlertKey: true,
            ]
        )

        messageProcessor = BLEMessageProcessor()
        characteristicHandler = BLECharacteristicHandler(messageProcessor: messageProcessor)
        peripheralManager = BLEPeripheralManager(characteristicHandler: characteristicHandler)
        peripheralScanner = BLEPeripheralScanner()

        characteristicHandler.onDeviceInfoUpdated = { [weak self] info in
            DispatchQueue.main.async {
                self?.obdDelegate?.adapterInfoUpdated(info)
            }
        }
    }

    // MARK: - Central Manager Control Methods

    func startScanning(_ serviceUUIDs: [CBUUID]?) {
        guard centralManager.state == .poweredOn else { 
            obdWarning("Cannot start scanning - Bluetooth not powered on", category: .bluetooth)
            return 
        }
        
        obdDebug("Starting BLE scan for services: \(serviceUUIDs?.map { $0.uuidString } ?? ["All"])", category: .bluetooth)
        
        // Use allowDuplicates: false for better performance - we don't need duplicate discovery events
        let scanOptions = [CBCentralManagerScanOptionAllowDuplicatesKey: false]
        centralManager.scanForPeripherals(withServices: serviceUUIDs, options: scanOptions)
    }

    func stopScan() {
        if centralManager.isScanning {
            obdDebug("Stopping BLE scan", category: .bluetooth)
            centralManager.stopScan()
        }
    }

    func disconnectPeripheral() {
        stopScan()
        // Cancel a pending attempt too: during the connecting window the
        // connected slot is still empty, and skipping the cancel here is what
        // used to wedge the manager in .connecting after a Stop mid-connect.
        let target = peripheralManager.connectedPeripheral ?? pendingConnectPeripheral
        if let target {
            centralManager.cancelPeripheralConnection(target)
        }
        // Cancelling a never-connected attempt produces no didDisconnect
        // callback, so land the state machine ourselves. resetConfigure also
        // resumes any scan/characteristics waiters and is idempotent — a real
        // link's later didDisconnect just runs it again as a no-op.
        resetConfigure()
    }

    // MARK: - Central Manager Delegate Methods

    func didUpdateState(_ central: CBCentralManager) {
        switch central.state {
        case .poweredOn:
            centralManagerDidPowerOn()
        case .poweredOff:
            obdWarning("Bluetooth powered off", category: .bluetooth)
            // Full teardown, not just dropping the peripheral: resumes any
            // scan/characteristics waiters and emits .disconnected so the
            // consumer's disconnect cleanup runs.
            resetConfigure()
        case .unsupported:
            obdError("Device does not support Bluetooth Low Energy", category: .bluetooth)
        case .unauthorized:
            obdError("App not authorized to use Bluetooth Low Energy", category: .bluetooth)
        case .resetting:
            obdWarning("Bluetooth is resetting", category: .bluetooth)
        default:
            // .unknown can fire transiently at startup; setting .error here
            // would stick (nothing transitions it back) and mask the real state.
            obdError("Bluetooth in unexpected state: \(central.state.rawValue)", category: .bluetooth)
        }
    }

    func centralManagerDidPowerOn() {
        // Scanning is initiated explicitly by the caller (Dongle tab / scanForDevices).
    }

    func didDiscover(_: CBCentralManager, peripheral: CBPeripheral, advertisementData: [String: Any], rssi: NSNumber) {
        peripheralScanner.addDiscoveredPeripheral(peripheral, advertisementData: advertisementData, rssi: rssi)
        // Snapshot on the BLE queue (where the scanner mutates the array) so the
        // main-queue delegate call doesn't read it mid-mutation.
        let found = peripheralScanner.foundPeripherals
        DispatchQueue.main.async {
            self.obdDelegate?.peripheralsUpdated(found)
        }
    }

    func connect(to peripheral: CBPeripheral) {
        let peripheralName = peripheral.name ?? "Unnamed"
        obdInfo("Attempting connection to peripheral: \(peripheralName)", category: .bluetooth)
        
        let oldState = connectionState
        connectionState = .connecting
        OBDLogger.shared.logConnectionChange(from: oldState, to: connectionState)

        pendingConnectPeripheral = peripheral
        centralManager.connect(peripheral, options: [CBConnectPeripheralOptionNotifyOnDisconnectionKey: true])
        if centralManager.isScanning {
            centralManager.stopScan()
        }
    }

    func didConnect(_: CBCentralManager, peripheral: CBPeripheral) {
        obdInfo("Connected to peripheral: \(peripheral.name ?? "Unnamed")", category: .bluetooth)
        pendingConnectPeripheral = nil
        peripheralManager.setPeripheral(peripheral)
        // Note: connectionState will be set to .connectedToAdapter in peripheralManager delegate
    }

    func didFailToConnect(_: CBCentralManager, peripheral: CBPeripheral, error: Error?) {
        let peripheralName = peripheral.name ?? "Unnamed"
        let errorMsg = error?.localizedDescription ?? "Unknown error"
        obdError("Connection failed to peripheral: \(peripheralName) - \(errorMsg)", category: .bluetooth)

        // Clean up peripheral state so a retry can proceed from a fresh baseline.
        pendingConnectPeripheral = nil
        peripheralManager.reset()

        let oldState = connectionState
        connectionState = .error
        OBDLogger.shared.logConnectionChange(from: oldState, to: connectionState)
    }

    func didDisconnect(_: CBCentralManager, peripheral: CBPeripheral, error: Error?) {
        let peripheralName = peripheral.name ?? "Unnamed"
        if let error = error {
            obdWarning("Unexpected disconnection from \(peripheralName): \(error.localizedDescription)", category: .bluetooth)
        } else {
            obdInfo("Disconnected from peripheral: \(peripheralName)", category: .bluetooth)
        }
        resetConfigure()
    }

    func connectionEventDidOccur(_: CBCentralManager, event: CBConnectionEvent, peripheral _: CBPeripheral) {
        obdError("Unexpected connection event: \(event.rawValue)", category: .bluetooth)
    }

    // MARK: - Async Methods

    func connectAsync(timeout: TimeInterval, peripheral: CBPeripheral? = nil) async throws {
        try await waitForPoweredOn()

        switch connectionState {
        case .connectedToAdapter, .connectedToVehicle:
            obdInfo("Already connected to peripheral", category: .bluetooth)
            return
        case .connecting:
            // Another connection attempt is genuinely in flight — don't stack on top.
            obdWarning("Cannot connect - already connecting", category: .bluetooth)
            throw BLEManagerError.connectionInProgress
        default:
            // .disconnected and .error are both recoverable starting points.
            break
        }

        let targetPeripheral: CBPeripheral
        if let peripheral = peripheral {
            targetPeripheral = peripheral
        } else {
            // Pending-connect mode (timeout: .infinity) requires a known
            // peripheral — never scan forever.
            guard timeout.isFinite else {
                throw BLEManagerError.peripheralNotFound
            }
            startScanning(BLEPeripheralScanner.supportedServices)
            do {
                targetPeripheral = try await peripheralScanner.waitForFirstPeripheral(timeout: timeout)
            } catch {
                // Without this the radio keeps scanning and the scanner's
                // waiter slot stays armed after a scan timeout.
                stopScan()
                peripheralScanner.reset()
                throw error
            }
        }

        connect(to: targetPeripheral)

        do {
            try await peripheralManager.waitForCharacteristicsSetup(timeout: timeout)
        } catch {
            // CoreBluetooth's connect never times out on its own, so without this the
            // manager stays .connecting forever and every retry throws
            // connectionInProgress. Clear peripheral state (which also resumes the
            // pending setup continuation) and cancel the half-open connection.
            pendingConnectPeripheral = nil
            peripheralManager.reset()
            centralManager.cancelPeripheralConnection(targetPeripheral)
            // A cancelled attempt (caller tore the task down deliberately) lands
            // .disconnected; a genuine failure lands .error — both recoverable
            // starting points. Never stomp a .disconnected another path already
            // reached (e.g. disconnectPeripheral during this attempt).
            let oldState = connectionState
            let newState: ConnectionState = error is CancellationError ? .disconnected : .error
            if oldState != .disconnected, oldState != newState {
                connectionState = newState
                OBDLogger.shared.logConnectionChange(from: oldState, to: newState)
            }
            throw error
        }
    }

    func peripheralManager(_ manager: BLEPeripheralManager, didSetupCharacteristics peripheral: CBPeripheral) {
        let oldState = connectionState
        connectionState = .connectedToAdapter
        OBDLogger.shared.logConnectionChange(from: oldState, to: connectionState)
        obdInfo("Characteristics setup complete, connected to adapter", category: .bluetooth)
    }

    func waitForPoweredOn() async throws {
        let maxWaitTime = BLEConstants.bluetoothPowerOnTimeout
        let startTime = CFAbsoluteTimeGetCurrent()
        
        while centralManager.state != .poweredOn {
            // Check for timeout
            if CFAbsoluteTimeGetCurrent() - startTime > maxWaitTime {
                obdError("Bluetooth failed to power on within \(maxWaitTime) seconds", category: .bluetooth)
                throw BLEManagerError.timeout
            }
            
            // Check for terminal states
            switch centralManager.state {
            case .unsupported:
                throw BLEManagerError.unsupported
            case .unauthorized:
                throw BLEManagerError.unauthorized
            case .poweredOff:
                obdWarning("Bluetooth is powered off - waiting...", category: .bluetooth)
            case .resetting:
                obdDebug("Bluetooth is resetting - waiting...", category: .bluetooth)
            default:
                break
            }
            
            try await Task.sleep(nanoseconds: BLEConstants.pollingInterval)
        }
        
        obdDebug("Bluetooth powered on successfully", category: .bluetooth)
    }


    /// Sends a message to the connected peripheral and returns the response.
    /// - Parameter message: The message to send.
    /// - Returns: The response from the peripheral.
    /// - Throws:
    ///     `BLEManagerError.sendingMessagesInProgress` if a message is already being sent.
    ///     `BLEManagerError.missingPeripheralOrCharacteristic` if the peripheral or ecu characteristic is missing.
    ///     `BLEManagerError.incorrectDataConversion` if the data cannot be converted to ASCII.
    ///     `BLEManagerError.peripheralNotConnected` if the peripheral is not connected.
    ///     `BLEManagerError.timeout` if the operation times out.
    ///     `BLEManagerError.unknownError` if an unknown error occurs.
    func sendCommand(_ command: String, retries: Int = 3) async throws -> [String] {
        guard let peripheral = peripheralManager.connectedPeripheral else {
            obdError("Missing peripheral or ECU characteristic", category: .bluetooth)
            throw BLEManagerError.missingPeripheralOrCharacteristic
        }

        obdDebug("Sending command: \(command)", category: .communication)

        // The `retries` parameter used to be declared and silently ignored (`retries _:`),
        // making every BLE command a single 3-second attempt. Two real consequences:
        // one dropped BLE notification failed the whole read instead of re-asking, and
        // K-line protocol detection (ISO 9141 / KWP 5-baud init takes 5-10 s inside the
        // ELM327 while it prints "SEARCHING...") could never fit one 3 s window — the
        // WiFi transport honors retries, so the same vehicle behaved differently per
        // transport. Re-sending the same command after a timeout is safe: the pending
        // completion was already taken by the timeout path, and a late reply to attempt
        // N carries the same payload attempt N+1 is waiting for.
        let attempts = max(1, retries)
        var lastError: Error?
        for attempt in 1 ... attempts {
            do {
                try characteristicHandler.writeCommand(command, to: peripheral)
                let response = try await messageProcessor.waitForResponse(timeout: BLEConstants.defaultTimeout)
                obdDebug("Command response: \(response.joined(separator: " | "))", category: .communication)
                return response
            } catch BLEManagerError.noData {
                // NO DATA is a routine reply (module asleep, unsupported PID) — not a
                // transport failure worth retrying (a parked car polling its ignition
                // probe would otherwise burn the whole retry budget every cycle), and
                // not worth logging above debug severity either — a real comm failure
                // still gets a warning per attempt below.
                obdDebug("No data: \(command)", category: .communication)
                throw BLEManagerError.noData
            } catch {
                lastError = error
                obdWarning("Command \(command) attempt \(attempt)/\(attempts) failed: \(error.localizedDescription)", category: .communication)
                if attempt < attempts {
                    try? await Task.sleep(nanoseconds: 150_000_000) // let the adapter settle before re-sending
                }
            }
        }
        obdError("Command failed after \(attempts) attempts: \(command)", category: .communication)
        throw lastError ?? BLEManagerError.timeout
    }

    func sendMonitorCommand(_ command: String, duration: TimeInterval) async throws -> [String] {
        guard let peripheral = peripheralManager.connectedPeripheral else {
            throw BLEManagerError.missingPeripheralOrCharacteristic
        }
        messageProcessor.monitorMode = true
        // Always reset monitorMode when this call returns, whether via timeout or a
        // normal response (e.g. the adapter replies "?" immediately with a ">").
        defer { messageProcessor.monitorMode = false }
        try characteristicHandler.writeCommand(command, to: peripheral)
        let frames = try await messageProcessor.waitForResponse(timeout: duration)
        // Send a bare CR to stop ELM327 monitoring mode, then drain the resulting
        // "STOPPED\r>" acknowledgment. Without this drain, STOPPED can arrive after
        // we return and corrupt the next command's waitForResponse.
        try? characteristicHandler.writeCommand("", to: peripheral)
        _ = try? await messageProcessor.waitForResponse(timeout: 1.0)
        return frames
    }


    func scanForPeripherals() async throws {
        startScanning(nil)
        try await Task.sleep(nanoseconds: UInt64(BLEConstants.scanDuration * 1_000_000_000))
        stopScan()
    }

    /// Looks up a previously connected peripheral by its system identifier so
    /// the consumer can issue a pending connect without scanning. Returns nil
    /// when Bluetooth never powers on or the system no longer knows the UUID.
    func retrievePeripheral(withIdentifier identifier: UUID) async -> CBPeripheral? {
        // Retrieval before the central reaches .poweredOn always returns [].
        try? await waitForPoweredOn()
        return centralManager.retrievePeripherals(withIdentifiers: [identifier]).first
    }

    private func resetConfigure() {
        pendingConnectPeripheral = nil
        characteristicHandler.reset()
        messageProcessor.reset()
        peripheralManager.reset()
        peripheralScanner.reset()

        let oldState = connectionState
        connectionState = .disconnected
        if oldState != connectionState {
            OBDLogger.shared.logConnectionChange(from: oldState, to: connectionState)
            obdDelegate?.peripheralsUpdated([])
        }
    }

    /// Fully resets BLEManager state for clean reconnection.
    /// Captures the peripheral reference before clearing state so that
    /// cancelPeripheralConnection is called with a valid reference, and the
    /// subsequent didDisconnect callback is a safe no-op (all handlers already nil'd).
    public func reset() {
        let target = peripheralManager.connectedPeripheral ?? pendingConnectPeripheral
        stopScan()
        resetConfigure()
        if let target {
            centralManager.cancelPeripheralConnection(target)
        }
    }
}

// MARK: - CBCentralManagerDelegate, CBPeripheralDelegate

/// Extension to conform to CBCentralManagerDelegate and CBPeripheralDelegate
/// and handle the delegate methods.
extension BLEManager: CBCentralManagerDelegate {

    func centralManager(_ central: CBCentralManager, didDiscover peripheral: CBPeripheral, advertisementData: [String: Any], rssi RSSI: NSNumber) {
        didDiscover(central, peripheral: peripheral, advertisementData: advertisementData, rssi: RSSI)
    }

    func centralManager(_ central: CBCentralManager, didConnect peripheral: CBPeripheral) {
        didConnect(central, peripheral: peripheral)
    }

    func centralManagerDidUpdateState(_ central: CBCentralManager) {
        didUpdateState(central)
    }

    func centralManager(_ central: CBCentralManager, didFailToConnect peripheral: CBPeripheral, error: Error?) {
        didFailToConnect(central, peripheral: peripheral, error: error)
    }

    func centralManager(_ central: CBCentralManager, didDisconnectPeripheral peripheral: CBPeripheral, error: Error?) {
        didDisconnect(central, peripheral: peripheral, error: error)
    }
}

enum BLEManagerError: Error, CustomStringConvertible, LocalizedError {
    case missingPeripheralOrCharacteristic
    case unknownCharacteristic
    case scanTimeout
    case sendMessageTimeout
    case stringConversionFailed
    case noData
    case incorrectDataConversion
    case peripheralNotConnected
    case sendingMessagesInProgress
    case timeout
    case peripheralNotFound
    case unknownError
    case unsupported
    case unauthorized
    case connectionInProgress

    public var description: String {
        switch self {
        case .missingPeripheralOrCharacteristic:
            return "Error: Device not connected. Make sure the device is correctly connected."
        case .scanTimeout:
            return "Error: Scan timed out. Please try to scan again or check the device's Bluetooth connection."
        case .sendMessageTimeout:
            return "Error: Send message timed out. Please try to send the message again or check the device's Bluetooth connection."
        case .stringConversionFailed:
            return "Error: Failed to convert string. Please make sure the string is in the correct format."
        case .noData:
            return "Error: No Data"
        case .unknownCharacteristic:
            return "Error: Unknown characteristic"
        case .incorrectDataConversion:
            return "Error: Incorrect data conversion"
        case .peripheralNotConnected:
            return "Error: Peripheral not connected"
        case .sendingMessagesInProgress:
            return "Error: Sending messages in progress"
        case .timeout:
            return "Error: Timeout"
        case .peripheralNotFound:
            return "Error: Peripheral not found"
        case .unknownError:
            return "Unknown Error"
        case .unsupported:
            return "Error: Device does not support Bluetooth Low Energy"
        case .unauthorized:
            return "Error: App not authorized to use Bluetooth Low Energy"
        case .connectionInProgress:
            return "Error: Connection already active or in progress. Please disconnect before attempting a new connection."
        }
    }

    // `CustomStringConvertible.description` alone isn't picked up by `Error.localizedDescription` —
    // without this, every one of the messages above was unreachable through normal error handling
    // and callers saw the generic "BLEManagerError error N." instead.
    public var errorDescription: String? { description }
}
