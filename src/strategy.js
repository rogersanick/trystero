import {decrypt, encrypt, genKey, sha1} from './crypto.js'
import initPeer from './peer.js'
import room from './room.js'
import {
  all,
  alloc,
  fromJson,
  libName,
  mkErr,
  noOp,
  selfId,
  toJson,
  topicPath,
  watchOnline
} from './utils.js'

const poolSize = 20
const announceIntervalMs = 5_333
const offerTtl = 57_333

const candidateType = 'candidate'

export default ({init, subscribe, announce, trickle = false}) => {
  const occupiedRooms = {}

  let didInit = false
  let initPromises
  let offerPool
  let offerCleanupTimer
  let cleanupWatchOnline

  return (config, roomId, onJoinError) => {
    const {appId} = config
    const effectiveConfig = {...config, trickle}

    if (occupiedRooms[appId]?.[roomId]) {
      return occupiedRooms[appId][roomId]
    }

    const pendingOffers = {}
    const pendingAnswers = {}
    const connectedPeers = {}
    const pendingCandidates = {}
    const rootTopicPlaintext = topicPath(libName, appId, roomId)
    const rootTopicP = sha1(rootTopicPlaintext)
    const selfTopicP = sha1(topicPath(rootTopicPlaintext, selfId))
    const key = genKey(config.password || '', appId, roomId)

    const toCipher = async signal => {
      if (signal.type === candidateType) {
        return {
          type: candidateType,
          candidate: await encrypt(key, toJson(signal.candidate))
        }
      }
      return {
        type: signal.type,
        sdp: await encrypt(key, signal.sdp)
      }
    }

    const toPlain = async encrypted => {
      if (encrypted.type === candidateType) {
        return {
          type: candidateType,
          candidate: fromJson(await decrypt(key, encrypted.candidate))
        }
      }
      return {
        type: encrypted.type,
        sdp: await decrypt(key, encrypted.sdp)
      }
    }

    const makeOffer = () => initPeer(true, effectiveConfig)

    const queueIncomingCandidate = (peerId, relayId, plainCandidate) => {
      pendingCandidates[peerId] ||= []
      ;(pendingCandidates[peerId][relayId] ||= []).push(plainCandidate)
    }

    const flushIncomingCandidates = (peerId, relayId, targetPeer) => {
      const queued = pendingCandidates[peerId]?.[relayId]
      if (!queued?.length || !targetPeer || targetPeer.isDead) return
      delete pendingCandidates[peerId][relayId]
      queued.forEach(c => targetPeer.signal(c))
    }

    const signalOutgoingCandidate = async (topic, signalPeer, envelope) => {
      const candidate = await toCipher(envelope)
      signalPeer(topic, toJson({peerId: selfId, candidate}))
    }

    const connectPeer = (peer, peerId, relayId) => {
      if (connectedPeers[peerId]) {
        if (connectedPeers[peerId] !== peer) {
          peer.destroy()
        }
        return
      }

      connectedPeers[peerId] = peer
      onPeerConnect(peer, peerId)

      if (pendingAnswers[peerId]?.[relayId] === peer) {
        delete pendingAnswers[peerId][relayId]
      }
      pendingOffers[peerId]?.forEach((p, i) => {
        if (i !== relayId) {
          p.destroy()
        }
      })
      delete pendingOffers[peerId]
    }

    const disconnectPeer = (peer, peerId) => {
      if (connectedPeers[peerId] === peer) {
        delete connectedPeers[peerId]
      }
    }

    const prunePendingOffer = (peerId, relayId) => {
      if (connectedPeers[peerId]) {
        return
      }

      const offer = pendingOffers[peerId]?.[relayId]

      if (offer) {
        delete pendingOffers[peerId][relayId]
        offer.destroy()
      }
    }

    const getOffers = n => {
      offerPool.push(...alloc(n, makeOffer))

      return all(
        offerPool
          .splice(0, n)
          .map(peer =>
            peer.offerPromise.then(toCipher).then(offer => ({peer, offer}))
          )
      )
    }

    const handleJoinError = (peerId, sdpType) =>
      onJoinError?.({
        error: `incorrect password (${config.password}) when decrypting ${sdpType}`,
        appId,
        peerId,
        roomId
      })

    const handleMessage = relayId => async (topic, msg, signalPeer) => {
      const [rootTopic, selfTopic] = await all([rootTopicP, selfTopicP])

      if (topic !== rootTopic && topic !== selfTopic) {
        return
      }

      const {peerId, offer, answer, candidate, candidates, peer: msgPeer} =
        typeof msg === 'string' ? fromJson(msg) : msg

      if (peerId === selfId || connectedPeers[peerId]) {
        return
      }

      if (candidate || candidates) {
        const encryptedCandidates = candidate ? [candidate] : candidates
        const plainCandidates = []
        try {
          plainCandidates.push(...(await all(encryptedCandidates.map(toPlain))))
        } catch {
          return
        }
        const targetPeer =
          msgPeer ??
          pendingOffers[peerId]?.[relayId] ??
          pendingAnswers[peerId]?.[relayId]
        if (targetPeer && !targetPeer.isDead) {
          plainCandidates.forEach(c => targetPeer.signal(c))
        } else {
          plainCandidates.forEach(c => queueIncomingCandidate(peerId, relayId, c))
        }
        return
      }

      if (peerId && !offer && !answer) {
        if (pendingOffers[peerId]?.[relayId]) {
          return
        }

        const [[{peer, offer}], topic] = await all([
          getOffers(1),
          sha1(topicPath(rootTopicPlaintext, peerId))
        ])

        pendingOffers[peerId] ||= []
        pendingOffers[peerId][relayId] = peer

        setTimeout(
          () => prunePendingOffer(peerId, relayId),
          announceIntervals[relayId] * 0.9
        )

        peer.setHandlers({
          connect: () => connectPeer(peer, peerId, relayId),
          close: () => disconnectPeer(peer, peerId),
          signal: async envelope => {
            if (envelope.type === candidateType) {
              await signalOutgoingCandidate(topic, signalPeer, envelope)
            }
          }
        })

        signalPeer(topic, toJson({peerId: selfId, offer}))
      } else if (offer) {
        const myOffer = pendingOffers[peerId]?.[relayId]

        if (myOffer && selfId > peerId) {
          return
        }

        const peer = initPeer(false, effectiveConfig)
        peer.setHandlers({
          connect: () => connectPeer(peer, peerId, relayId),
          close: () => disconnectPeer(peer, peerId)
        })

        pendingAnswers[peerId] ||= []
        pendingAnswers[peerId][relayId] = peer

        let plainOffer

        try {
          plainOffer = await toPlain(offer)
        } catch {
          handleJoinError(peerId, 'offer')
          return
        }

        if (peer.isDead) {
          return
        }

        const [topic, answer] = await all([
          sha1(topicPath(rootTopicPlaintext, peerId)),
          peer.signal(plainOffer)
        ])

        peer.setHandlers({
          signal: async envelope => {
            if (envelope.type === candidateType) {
              await signalOutgoingCandidate(topic, signalPeer, envelope)
            }
          }
        })

        flushIncomingCandidates(peerId, relayId, peer)
        signalPeer(
          topic,
          toJson({peerId: selfId, answer: await toCipher(answer)})
        )
      } else if (answer) {
        let plainAnswer

        try {
          plainAnswer = await toPlain(answer)
        } catch (e) {
          handleJoinError(peerId, 'answer')
          return
        }

        let targetPeer = msgPeer ?? pendingOffers[peerId]?.[relayId]

        if (targetPeer) {
          targetPeer.setHandlers({
            connect: () => connectPeer(targetPeer, peerId, relayId),
            close: () => disconnectPeer(targetPeer, peerId)
          })
          await targetPeer.signal(plainAnswer)
          flushIncomingCandidates(peerId, relayId, targetPeer)
        } else {
          targetPeer = pendingAnswers[peerId]?.[relayId]
          if (targetPeer && !targetPeer.isDead) {
            await targetPeer.signal(plainAnswer)
            flushIncomingCandidates(peerId, relayId, targetPeer)
          }
        }
      }
    }

    if (!config) {
      throw mkErr('requires a config map as the first argument')
    }

    if (!appId && !config.firebaseApp) {
      throw mkErr('config map is missing appId field')
    }

    if (!roomId) {
      throw mkErr('roomId argument required')
    }

    if (!didInit) {
      const initRes = init(config)
      offerPool = alloc(poolSize, makeOffer)
      initPromises = Array.isArray(initRes) ? initRes : [initRes]
      didInit = true
      offerCleanupTimer = setInterval(
        () =>
          (offerPool = offerPool.filter(peer => {
            const shouldLive = Date.now() - peer.created < offerTtl

            if (!shouldLive) {
              peer.destroy()
            }

            return shouldLive
          })),
        offerTtl * 1.03
      )
      cleanupWatchOnline = config.manualRelayReconnection ? noOp : watchOnline()
    }

    const announceIntervals = initPromises.map(() => announceIntervalMs)
    const announceTimeouts = []

    const unsubFns = initPromises.map(async (relayP, i) =>
      subscribe(
        await relayP,
        await rootTopicP,
        await selfTopicP,
        handleMessage(i),
        getOffers
      )
    )

    all([rootTopicP, selfTopicP]).then(([rootTopic, selfTopic]) => {
      const queueAnnounce = async (relay, i) => {
        const ms = await announce(relay, rootTopic, selfTopic)

        if (typeof ms === 'number') {
          announceIntervals[i] = ms
        }

        announceTimeouts[i] = setTimeout(
          () => queueAnnounce(relay, i),
          announceIntervals[i]
        )
      }

      unsubFns.forEach(async (didSub, i) => {
        await didSub
        queueAnnounce(await initPromises[i], i)
      })
    })

    let onPeerConnect = noOp

    occupiedRooms[appId] ||= {}

    return (occupiedRooms[appId][roomId] = room(
      f => (onPeerConnect = f),
      id => delete connectedPeers[id],
      () => {
        delete occupiedRooms[appId][roomId]
        announceTimeouts.forEach(clearTimeout)
        unsubFns.forEach(async f => (await f)())
        clearInterval(offerCleanupTimer)
        cleanupWatchOnline()
        didInit = false
      }
    ))
  }
}
