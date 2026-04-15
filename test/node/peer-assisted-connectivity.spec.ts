// @ts-nocheck
import assert from 'node:assert/strict'
import test from 'node:test'
import {encrypt, genKey} from '../../packages/core/src/crypto.ts'
import createStrategy from '../../packages/core/src/strategy.ts'
import {PeerAssistedConnectivity} from '../../packages/core/src/strategies/peer-assisted-connectivity.ts'
import createRoom from '../../packages/core/src/room.ts'
import {
  MockDataChannel,
  MockPeer as BaseMockPeer,
  MockRTCPeerConnection as BaseMockRTCPeerConnection,
  type Subscriber,
  wait,
  waitFor as sharedWaitFor
} from './mocks.ts'

const peerConnectionRegistry = new Map()

class MockRTCPeerConnection extends BaseMockRTCPeerConnection {
  remoteDescription = null
  onicecandidate = null
  _paired = null
  _initiatorDataChannel = null

  createDataChannel() {
    const channel = new MockDataChannel()
    this._initiatorDataChannel = channel
    return channel
  }

  addEventListener(event, fn) {
    ;(this.listeners[event] ??= new Set()).add(fn)
  }

  removeEventListener(event, fn) {
    this.listeners[event]?.delete(fn)
  }

  restartIce() {}

  async createOffer() {
    return {type: 'offer', sdp: `mock-offer-${Math.random()}`}
  }

  async createAnswer() {
    return {type: 'answer', sdp: `mock-answer-${Math.random()}`}
  }

  async setLocalDescription(description) {
    if (description?.type === 'rollback') {
      this.signalingState = 'stable'
      return
    }

    const nextDescription = description ??
      (this.signalingState === 'have-remote-offer'
        ? await this.createAnswer()
        : await this.createOffer())

    this.localDescription = nextDescription
    this.signalingState =
      nextDescription.type === 'offer' ? 'have-local-offer' : 'stable'

    if (nextDescription.type === 'offer') {
      peerConnectionRegistry.set(nextDescription.sdp, this)
    }

    this.listeners['icegatheringstatechange']?.forEach(listener => listener())
  }

  async setRemoteDescription(desc) {
    this.remoteDescription = desc

    if (desc?.type === 'offer') {
      this.signalingState = 'have-remote-offer'

      const initiator = peerConnectionRegistry.get(desc.sdp)

      if (initiator) {
        this._paired = initiator
        initiator._paired = this
        peerConnectionRegistry.delete(desc.sdp)
      }
    } else {
      this.signalingState = 'stable'

      if (desc?.type === 'answer' && this._paired) {
        this._simulateConnection()
      }
    }
  }

  _simulateConnection() {
    const initiator = this
    const responder = this._paired

    const initiatorChannel = initiator._initiatorDataChannel
    const responderChannel = new MockDataChannel()

    initiatorChannel.send = data => {
      setTimeout(() => responderChannel.onmessage?.({data}), 1)
    }

    responderChannel.send = data => {
      setTimeout(() => initiatorChannel.onmessage?.({data}), 1)
    }

    responder.ondatachannel?.({channel: responderChannel})

    initiator.connectionState = 'connected'
    responder.connectionState = 'connected'
    initiator.iceConnectionState = 'connected'
    responder.iceConnectionState = 'connected'

    setTimeout(() => {
      initiatorChannel.readyState = 'open'
      responderChannel.readyState = 'open'
      initiatorChannel.onopen?.()
      responderChannel.onopen?.()
    }, 5)
  }

  async addIceCandidate() {}

  close() {
    peerConnectionRegistry.forEach((pc, key) => {
      if (pc === this) peerConnectionRegistry.delete(key)
    })
    super.close()
  }
}

class MockPeer extends BaseMockPeer {
  _linked = null

  sendData(data) {
    if (this._linked) {
      const remote = this._linked
      const buf =
        data instanceof Uint8Array
          ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
          : data instanceof ArrayBuffer
            ? data
            : data.buffer
      setTimeout(() => remote.handlers.data?.(buf), 1)
    }
  }
}

const linkPeers = (a, b) => {
  a._linked = b
  b._linked = a
}

const waitFor = (
  check: () => boolean,
  timeoutMs = 5_000
): Promise<void> => sharedWaitFor(check, timeoutMs)

const makeEncryptedAnswer = (appId: string, roomId: string) =>
  encrypt(genKey('', appId, roomId), 'answer-sdp')

const connectMockPeerPair = async (
  subLocal: Subscriber,
  subRemote: Subscriber,
  localPeerId: string,
  remotePeerId: string,
  appId: string,
  roomId: string
): Promise<{localPeer: MockPeer; remotePeer: MockPeer}> => {
  const localPeer = new MockPeer()
  const remotePeer = new MockPeer()
  linkPeers(localPeer, remotePeer)

  const encLocal = await makeEncryptedAnswer(appId, roomId)
  const encRemote = await makeEncryptedAnswer(appId, roomId)

  await subLocal.onMessage(
    subLocal.rootTopic,
    {peerId: remotePeerId, answer: encLocal, peer: localPeer},
    () => {}
  )

  await subRemote.onMessage(
    subRemote.rootTopic,
    {peerId: localPeerId, answer: encRemote, peer: remotePeer},
    () => {}
  )

  return {localPeer, remotePeer}
}

void test(
  'PeerAssistedConnectivity: does not interfere with normal two-peer connectivity',
  {timeout: 10_000},
  async () => {
    const subsA: Subscriber[] = []
    const subsB: Subscriber[] = []
    const appId = `pac-smoke-${Date.now()}`
    const roomId = 'room-smoke'

    const joinA = createStrategy({
      init: () => ({}),
      subscribe: async (_relay, rootTopic, selfTopic, onMessage) => {
        subsA.push({rootTopic, selfTopic, onMessage})
        return () => {}
      },
      announce: () => {}
    })

    const joinB = createStrategy({
      init: () => ({}),
      subscribe: async (_relay, rootTopic, selfTopic, onMessage) => {
        subsB.push({rootTopic, selfTopic, onMessage})
        return () => {}
      },
      announce: () => {}
    })

    const pac = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})
    const config = {
      appId,
      rtcPolyfill: MockRTCPeerConnection,
      strategies: [pac]
    }

    const roomA = joinA(config, roomId)
    const roomB = joinB(config, roomId)

    try {
      await waitFor(() => subsA.length >= 1 && subsB.length >= 1)

      await connectMockPeerPair(
        subsA[0], subsB[0], 'peer-a', 'peer-b', appId, roomId
      )

      await wait(300)

      assert.ok('peer-b' in roomA.getPeers(), 'roomA should see peer-b')
      assert.ok('peer-a' in roomB.getPeers(), 'roomB should see peer-a')
    } finally {
      await roomA.leave().catch(() => {})
      await roomB.leave().catch(() => {})
    }
  }
)

void test(
  'PeerAssistedConnectivity: bridges two peers through a mutual peer via gossip',
  {timeout: 15_000},
  async () => {
    peerConnectionRegistry.clear()
    const config = {appId: `pac-bridge-${Date.now()}`, rtcPolyfill: MockRTCPeerConnection}

    const roomAlice = createRoom(() => {}, () => {}, () => {})
    const roomBob = createRoom(() => {}, () => {}, () => {})
    const roomCharlie = createRoom(() => {}, () => {}, () => {})

    const pacAlice = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})
    const pacBob = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})
    const pacCharlie = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})

    const roomId = 'bridge-room'
    const cleanupAlice = pacAlice.init(roomAlice, 'alice', config, roomId)
    const cleanupBob = pacBob.init(roomBob, 'bob', config, roomId)
    const cleanupCharlie = pacCharlie.init(roomCharlie, 'charlie', config, roomId)

    try {
      const peerAB = new MockPeer()
      const peerBA = new MockPeer()
      linkPeers(peerAB, peerBA)

      roomAlice._injectPeer(peerAB, 'bob')
      roomBob._injectPeer(peerBA, 'alice')

      await waitFor(
        () => 'bob' in roomAlice.getPeers() && 'alice' in roomBob.getPeers()
      )

      const peerAC = new MockPeer()
      const peerCA = new MockPeer()
      linkPeers(peerAC, peerCA)

      roomAlice._injectPeer(peerAC, 'charlie')
      roomCharlie._injectPeer(peerCA, 'alice')

      await waitFor(
        () =>
          'charlie' in roomAlice.getPeers() &&
          'alice' in roomCharlie.getPeers()
      )

      await waitFor(
        () =>
          'charlie' in roomBob.getPeers() && 'bob' in roomCharlie.getPeers(),
        10_000
      )

      assert.ok(
        'charlie' in roomBob.getPeers(),
        'bob should see charlie via PAC bridge through alice'
      )
      assert.ok(
        'bob' in roomCharlie.getPeers(),
        'charlie should see bob via PAC bridge through alice'
      )
    } finally {
      cleanupAlice()
      cleanupBob()
      cleanupCharlie()
      await roomAlice.leave().catch(() => {})
      await roomBob.leave().catch(() => {})
      await roomCharlie.leave().catch(() => {})
    }
  }
)

void test(
  'PeerAssistedConnectivity: onPeerJoin/onPeerLeave chaining allows user and strategy listeners',
  {timeout: 10_000},
  async () => {
    const subsA: Subscriber[] = []
    const subsB: Subscriber[] = []
    const appId = `pac-chain-${Date.now()}`
    const roomId = 'room-chain'

    const joinRoomA = createStrategy({
      init: () => ({}),
      subscribe: async (_relay, rootTopic, selfTopic, onMessage) => {
        subsA.push({rootTopic, selfTopic, onMessage})
        return () => {}
      },
      announce: () => {}
    })

    const joinRoomB = createStrategy({
      init: () => ({}),
      subscribe: async (_relay, rootTopic, selfTopic, onMessage) => {
        subsB.push({rootTopic, selfTopic, onMessage})
        return () => {}
      },
      announce: () => {}
    })

    const pac = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})
    const config = {
      appId,
      rtcPolyfill: MockRTCPeerConnection,
      strategies: [pac]
    }

    const roomA = joinRoomA(config, roomId)
    const roomB = joinRoomB(config, roomId)

    const userJoinedA: string[] = []
    const userLeftA: string[] = []

    roomA.onPeerJoin(id => userJoinedA.push(id))
    roomA.onPeerLeave(id => userLeftA.push(id))

    try {
      await waitFor(() => subsA.length >= 1 && subsB.length >= 1)

      const {localPeer} = await connectMockPeerPair(
        subsA[0],
        subsB[0],
        'self-a',
        'peer-b',
        appId,
        roomId
      )

      await wait(300)

      assert.ok(
        userJoinedA.includes('peer-b'),
        'user onPeerJoin should fire for peer-b (chaining works)'
      )

      localPeer.destroy()
      await wait(100)

      assert.ok(
        userLeftA.includes('peer-b'),
        'user onPeerLeave should fire for peer-b (chaining works)'
      )
    } finally {
      await roomA.leave().catch(() => {})
      await roomB.leave().catch(() => {})
    }
  }
)

void test(
  'PeerAssistedConnectivity: cleanup runs on leave and destroys shared peer after idle',
  {timeout: 10_000},
  async () => {
    const subsA: Subscriber[] = []
    const subsB: Subscriber[] = []
    const appId = `pac-cleanup-${Date.now()}`
    const roomId = 'room-cleanup'

    const joinRoomA = createStrategy({
      init: () => ({}),
      subscribe: async (_relay, rootTopic, selfTopic, onMessage) => {
        subsA.push({rootTopic, selfTopic, onMessage})
        return () => {}
      },
      announce: () => {}
    })

    const joinRoomB = createStrategy({
      init: () => ({}),
      subscribe: async (_relay, rootTopic, selfTopic, onMessage) => {
        subsB.push({rootTopic, selfTopic, onMessage})
        return () => {}
      },
      announce: () => {}
    })

    const pac = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})
    const config = {
      appId,
      rtcPolyfill: MockRTCPeerConnection,
      strategies: [pac],
      _test_only_sharedPeerIdleMs: 60
    }

    const roomA = joinRoomA(config, roomId)
    const roomB = joinRoomB(config, roomId)

    try {
      await waitFor(() => subsA.length >= 1 && subsB.length >= 1)

      const {localPeer} = await connectMockPeerPair(
        subsA[0],
        subsB[0],
        'self-a',
        'peer-b',
        appId,
        roomId
      )

      await wait(300)

      assert.ok(
        'peer-b' in roomA.getPeers(),
        'peer-b should be connected before leave'
      )

      await roomA.leave()

      assert.equal(
        localPeer.destroyCount,
        0,
        'shared peer should not be destroyed immediately (idle timeout pending)'
      )

      await wait(150)

      assert.equal(
        localPeer.destroyCount,
        1,
        'shared peer should be destroyed after idle timeout'
      )
    } finally {
      await roomA.leave().catch(() => {})
      await roomB.leave().catch(() => {})
    }
  }
)

void test(
  'PeerAssistedConnectivity: bridge peer accepts first connection request, drops subsequent until lock released.',
  {timeout: 15_000},
  async () => {
    peerConnectionRegistry.clear()
    const config = {appId: `pac-dedup-${Date.now()}`, rtcPolyfill: MockRTCPeerConnection}

    const roomAlice = createRoom(() => {}, () => {}, () => {})
    const roomBob = createRoom(() => {}, () => {}, () => {})
    const roomCharlie = createRoom(() => {}, () => {}, () => {})

    const pacAlice = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})
    const pacBob = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})
    const pacCharlie = new PeerAssistedConnectivity({gossipIntervalMs: 60_000})

    const roomId = 'dedup-room'
    const cleanupAlice = pacAlice.init(roomAlice, 'alice', config, roomId)
    const cleanupBob = pacBob.init(roomBob, 'bob', config, roomId)
    const cleanupCharlie = pacCharlie.init(roomCharlie, 'charlie', config, roomId)

    const receivedOffers: string[] = []

    try {
      const peerAB = new MockPeer()
      const peerBA = new MockPeer()
      linkPeers(peerAB, peerBA)

      roomAlice._injectPeer(peerAB, 'bob')
      roomBob._injectPeer(peerBA, 'alice')

      await waitFor(
        () => 'bob' in roomAlice.getPeers() && 'alice' in roomBob.getPeers()
      )

      const peerAC = new MockPeer()
      const peerCA = new MockPeer()
      linkPeers(peerAC, peerCA)

      roomAlice._injectPeer(peerAC, 'charlie')
      roomCharlie._injectPeer(peerCA, 'alice')

      await waitFor(
        () =>
          'charlie' in roomAlice.getPeers() &&
          'alice' in roomCharlie.getPeers()
      )

      const [, getBridgeCharlie] = roomCharlie.makeAction('@_pac_bridge')
      getBridgeCharlie((payload: {offer?: string; offerId?: string}) => {
        if (payload.offer) {
          receivedOffers.push(payload.offerId ?? payload.offer)
        }
      })

      const [sendBridgeBob] = roomBob.makeAction('@_pac_bridge')
      void sendBridgeBob(
        {from: 'bob', target: 'charlie', offer: 'offer-1', offerId: 'oid-1'},
        'alice'
      )

      await wait(50)

      void sendBridgeBob(
        {from: 'bob', target: 'charlie', offer: 'offer-2', offerId: 'oid-2'},
        'alice'
      )

      await wait(200)

      assert.ok(
        receivedOffers.includes('oid-1'),
        'first offer should be forwarded to charlie'
      )

      assert.ok(
        !receivedOffers.includes('oid-2'),
        'second offer for same pair should be dropped by bridge peer'
      )

      assert.equal(
        receivedOffers.filter(id => id === 'oid-1' || id === 'oid-2').length,
        1,
        'exactly one offer should reach charlie'
      )
    } finally {
      cleanupAlice()
      cleanupBob()
      cleanupCharlie()
      await roomAlice.leave().catch(() => {})
      await roomBob.leave().catch(() => {})
      await roomCharlie.leave().catch(() => {})
    }
  }
)


