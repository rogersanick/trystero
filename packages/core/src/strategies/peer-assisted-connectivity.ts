import {OfferPool, offerTtl} from '../offer-pool'
import initPeer from '../peer'
import {SharedPeerManager} from '../shared-peer'
import {
  handleOffer as signalHandleOffer,
  handleAnswer as signalHandleAnswer,
  handleCandidate as signalHandleCandidate,
  ensureOffer,
  getState,
  resetOfferState,
  updateStatus
} from '../signal-handler'
import {fromJson, noOp, resetTimer} from '../utils'
import type {
  BaseRoomConfig,
  InternalRoom,
  PeerHandle,
  RoomStrategy,
  Signal,
  SignalContext
} from '../types'

const gossipActionNs = '@_pac_gossip'
const bridgeActionNs = '@_pac_bridge'
const peerListenerId = '@_pac'
const defaultGossipIntervalMs = 2_000

const getSignalPairKey = (a: string, b: string): string =>
  a < b ? `${a}\0${b}` : `${b}\0${a}`

const identitySignal = (signal: Signal): Promise<Signal> =>
  Promise.resolve(signal)

type BridgePayload = {
  from: string
  target: string
  offer?: string
  answer?: string
  candidate?: string
  offerId?: string
}

type GossipPayload = {
  knownPeers: string[]
}

type PeerAssistedOptions = {
  gossipIntervalMs?: number
}

const makeBridgeSignalContext = (
  config: BaseRoomConfig,
  roomId: string,
  isLeaving: () => boolean,
  injectPeer: (peer: PeerHandle, peerId: string) => void
): SignalContext => {
  const bridgeRoomId = `pac-bridge:${roomId}`
  const pool = new OfferPool(() => initPeer(true, config))

  const ctx: SignalContext = {
    appId: config.appId,
    roomId: bridgeRoomId,
    config,
    peerStates: {},
    rootTopicPlaintext: bridgeRoomId,
    rootTopicP: Promise.resolve(bridgeRoomId),
    selfTopicP: Promise.resolve(bridgeRoomId),
    toPlain: identitySignal,
    toCipher: identitySignal,
    isLeaving,
    onJoinError: undefined,
    sharedPeers: new SharedPeerManager(),
    offerPool: pool,
    encryptOffer: async (peer: PeerHandle): Promise<string> => {
      const offer = await peer.getOffer(Date.now() - peer.created > offerTtl)

      if (!offer || offer.type !== 'offer') {
        throw new Error('failed to get bridge offer')
      }

      return offer.sdp
    },
    initPeer,
    connectPeer: (peer: PeerHandle, peerId: string) => {
      if (isLeaving()) {
        peer.destroy()
        return
      }

      const state = ctx.peerStates[peerId]

      if (state) {
        state.answeringExpiryTimer = resetTimer(state.answeringExpiryTimer)
        state.answeringPeer = null
        state.connectedPeer = peer
        resetOfferState(state, pool)
      }

      injectPeer(peer, peerId)
    },
    disconnectPeer: (peer: PeerHandle, peerId: string) => {
      if (isLeaving()) {
        return
      }

      const state = ctx.peerStates[peerId]

      if (state?.connectedPeer === peer) {
        state.connectedPeer = null
        updateStatus(state)
      }
    },
    attachSharedPeerToRoom: noOp as SignalContext['attachSharedPeerToRoom'],
    announceIntervals: [],
    announceIntervalMs: 0
  }

  return ctx
}

export class PeerAssistedConnectivity implements RoomStrategy {
  private gossipIntervalMs: number

  constructor(options?: PeerAssistedOptions) {
    this.gossipIntervalMs = options?.gossipIntervalMs ?? defaultGossipIntervalMs
  }

  init(
    room: InternalRoom,
    localId: string,
    config: BaseRoomConfig,
    roomId: string
  ): () => void {
    const connectedPeers = new Set<string>()
    const relayedSignalPairs = new Set<string>()
    let destroyed = false
    let gossipTimer: ReturnType<typeof setTimeout> | null = null

    const [sendGossip, getGossip] = room.makeAction<GossipPayload>(
      gossipActionNs
    )
    const [sendBridge, getBridge] = room.makeAction<BridgePayload>(
      bridgeActionNs
    )

    const bridgeCtx = makeBridgeSignalContext(
      config,
      roomId,
      () => destroyed,
      (peer, peerId) => room._injectPeer(peer, peerId)
    )

    const broadcastGossip = (): void => {
      if (destroyed || connectedPeers.size === 0) {
        return
      }

      void sendGossip({knownPeers: Array.from(connectedPeers)})
    }

    const scheduleGossip = (): void => {
      if (gossipTimer !== null) {
        clearTimeout(gossipTimer)
      }

      gossipTimer = setTimeout(() => {
        if (destroyed) {
          return
        }

        broadcastGossip()
        scheduleGossip()
      }, this.gossipIntervalMs)
    }

    const shouldInitiate = (remotePeerId: string): boolean =>
      localId < remotePeerId

    const makeBridgeSignalPeer = (
      targetPeerId: string,
      bridgePeerId: string
    ): ((peerTopic: string, signalJson: string) => void) =>
      (_peerTopic: string, signalJson: string): void => {
        const parsed = fromJson<Record<string, unknown>>(signalJson)

        void sendBridge(
          {
            from: localId,
            target: targetPeerId,
            ...(parsed['answer'] != null
              ? {answer: parsed['answer'] as string}
              : {}),
            ...(parsed['candidate'] != null
              ? {candidate: parsed['candidate'] as string}
              : {}),
            ...(parsed['offerId'] != null
              ? {offerId: parsed['offerId'] as string}
              : {})
          },
          bridgePeerId
        )
      }

    const initiateBridge = (
      targetPeerId: string,
      bridgePeerId: string
    ): void => {
      if (
        destroyed ||
        connectedPeers.has(targetPeerId) ||
        !shouldInitiate(targetPeerId)
      ) {
        return
      }

      const state = getState(bridgeCtx.peerStates, targetPeerId)

      if (state.offerPeer || state.answeringPeer || state.connectedPeer) {
        return
      }

      void ensureOffer(bridgeCtx, state, targetPeerId, 0)
        .then(offerInfo => {
          if (destroyed || connectedPeers.has(targetPeerId)) {
            return
          }

          state.offerSignalRelays[0] = signal => {
            if (destroyed || state.offerPeer !== offerInfo.peer) {
              return
            }

            const {type} = signal

            if (
              type !== 'offer' &&
              type !== 'answer' &&
              type !== 'candidate'
            ) {
              return
            }

            void sendBridge(
              {
                from: localId,
                target: targetPeerId,
                [type]: signal.sdp,
                offerId: offerInfo.offerId
              },
              bridgePeerId
            )
          }

          void sendBridge(
            {
              from: localId,
              target: targetPeerId,
              offer: offerInfo.offer,
              offerId: offerInfo.offerId
            },
            bridgePeerId
          )

          state.offerSignalBacklog.forEach(signal =>
            state.offerSignalRelays[0]?.(signal)
          )
        })
        .catch(() => {})
    }

    getGossip((data, senderPeerId) => {
      if (destroyed || !Array.isArray(data.knownPeers)) {
        return
      }

      for (const peerId of data.knownPeers) {
        if (peerId === localId || connectedPeers.has(peerId)) {
          continue
        }

        const state = bridgeCtx.peerStates[peerId]

        if (
          state?.offerPeer ||
          state?.answeringPeer ||
          state?.connectedPeer
        ) {
          continue
        }

        initiateBridge(peerId, senderPeerId)
      }
    })

    getBridge((payload, senderPeerId) => {
      if (destroyed) {
        return
      }

      if (payload.target === localId) {
        if (payload.offer) {
          if (connectedPeers.has(payload.from)) {
            return
          }

          void signalHandleOffer(
            bridgeCtx,
            0,
            payload.from,
            payload.offer,
            payload.offerId,
            false,
            makeBridgeSignalPeer(payload.from, senderPeerId)
          )
        } else if (payload.answer) {
          void signalHandleAnswer(
            bridgeCtx,
            0,
            payload.from,
            payload.answer,
            payload.offerId,
            undefined
          )
        } else if (payload.candidate) {
          void signalHandleCandidate(
            bridgeCtx,
            payload.from,
            payload.candidate,
            payload.offerId,
            undefined
          )
        }
      } else if (connectedPeers.has(payload.target)) {
        if (payload.offer) {
          const key = getSignalPairKey(payload.from, payload.target)

          if (relayedSignalPairs.has(key)) {
            return
          }

          relayedSignalPairs.add(key)
        } else if (payload.answer) {
          relayedSignalPairs.delete(
            getSignalPairKey(payload.from, payload.target)
          )
        }

        void sendBridge(payload, payload.target)
      }
    })

    room._addPeerJoinListener(
      peerListenerId,
      (peerId: string) => {
        connectedPeers.add(peerId)
        broadcastGossip()
      }
    )

    room._addPeerLeaveListener(
      peerListenerId,
      (peerId: string) => {
        connectedPeers.delete(peerId)

        for (const key of relayedSignalPairs) {
          if (key.startsWith(peerId + '\0') || key.endsWith('\0' + peerId)) {
            relayedSignalPairs.delete(key)
          }
        }

        const state = bridgeCtx.peerStates[peerId]

        if (state) {
          if (state.answeringPeer && !state.answeringPeer.isDead) {
            state.answeringPeer.destroy()
          }

          resetOfferState(state, bridgeCtx.offerPool)
          delete bridgeCtx.peerStates[peerId]
        }
      }
    )

    scheduleGossip()

    return () => {
      destroyed = true

      if (gossipTimer !== null) {
        clearTimeout(gossipTimer)
        gossipTimer = null
      }

      for (const peerId of Object.keys(bridgeCtx.peerStates)) {
        const state = bridgeCtx.peerStates[peerId]

        if (!state) {
          continue
        }

        if (state.answeringPeer && !state.answeringPeer.isDead) {
          state.answeringPeer.destroy()
        }

        resetOfferState(state, bridgeCtx.offerPool)
      }

      bridgeCtx.peerStates = {}
      connectedPeers.clear()
      relayedSignalPairs.clear()
      bridgeCtx.offerPool.destroy()
    }
  }
}
