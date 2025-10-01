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
  topicPath
} from './utils.js'

const poolSize = 20
const announceIntervalMs = 5_333
const offerTtl = 57_333
const offerType = 'offer'
const answerType = 'answer'

export default ({init, subscribe, announce, trickle = true}) => {
  const occupiedRooms = {}

  let didInit = false
  let initPromises
  let offerPool
  let offerCleanupTimer

  return (config, roomId, onJoinError) => {
    const {appId} = config

    if (occupiedRooms[appId]?.[roomId]) {
      return occupiedRooms[appId][roomId]
    }

    const pendingOffers = {}
    const connectedPeers = {}
    const rootTopicPlaintext = topicPath(libName, appId, roomId)
    const rootTopicP = sha1(rootTopicPlaintext)
    const selfTopicP = sha1(topicPath(rootTopicPlaintext, selfId))
    const key = genKey(config.password || '', appId, roomId)

    const encryptDescription = async signal => ({
      type: signal.type,
      sdp: await encrypt(key, signal.sdp)
    })

    const decryptDescription = async signal => ({
      type: signal.type,
      sdp: await decrypt(key, signal.sdp)
    })

    const encryptCandidate = async candidate => {
      if (candidate === null || candidate === undefined) {
        return candidate
      }

      return encrypt(key, toJson(candidate))
    }

    const decryptCandidate = async candidate => {
      if (candidate === null || candidate === undefined) {
        return candidate
      }

      return fromJson(await decrypt(key, candidate))
    }

    const peerConfig = {...config, trickle: config.trickle ?? trickle}

    const makeOffer = () => initPeer(true, peerConfig)

    const connectPeer = (peer, peerId, relayId) => {
      if (connectedPeers[peerId]) {
        if (connectedPeers[peerId] !== peer) {
          peer.destroy()
        }
        return
      }

      connectedPeers[peerId] = peer
      onPeerConnect(peer, peerId)

      pendingOffers[peerId]?.forEach((peer, i) => {
        if (i !== relayId) {
          peer.destroy()
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
            peer.offerPromise
              .then(encryptDescription)
              .then(offer => ({peer, offer}))
          )
      )
    }

    const formatSignal = async signal => {
      if ('candidate' in signal) {
        return {candidate: await encryptCandidate(signal.candidate)}
      }

      if (signal.type === offerType) {
        return {offer: await encryptDescription(signal)}
      }

      if (signal.type === answerType) {
        return {answer: await encryptDescription(signal)}
      }

      return null
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

      const message = typeof msg === 'string' ? fromJson(msg) : msg
      const {peerId, offer, answer, candidate, peer} = message

      if (candidate !== undefined) {
        if (peerId === selfId) {
          return
        }

        let plainCandidate

        try {
          plainCandidate = await decryptCandidate(candidate)
        } catch {
          handleJoinError(peerId, 'candidate')
          return
        }

        const targetPeer =
          peer?.isDead === false
            ? peer
            : connectedPeers[peerId] || pendingOffers[peerId]?.[relayId]

        if (targetPeer && !targetPeer.isDead) {
          targetPeer.signal({candidate: plainCandidate})
        }

        return
      }

      if (peerId === selfId || connectedPeers[peerId]) {
        return
      }

      if (peerId && !offer && !answer) {
        if (pendingOffers[peerId]?.[relayId]) {
          return
        }

        const [[{peer: pendingPeer, offer}], topic] = await all([
          getOffers(1),
          sha1(topicPath(rootTopicPlaintext, peerId))
        ])

        pendingOffers[peerId] ||= []
        pendingOffers[peerId][relayId] = pendingPeer

        setTimeout(
          () => prunePendingOffer(peerId, relayId),
          announceIntervals[relayId] * 0.9
        )

        signalPeer(topic, toJson({peerId: selfId, offer}))

        const sendSignal = async signal => {
          const payload = await formatSignal(signal)

          if (payload) {
            signalPeer(topic, toJson({peerId: selfId, ...payload}))
          }
        }

        pendingPeer.setHandlers({
          signal: signal => sendSignal(signal),
          connect: () => connectPeer(pendingPeer, peerId, relayId),
          close: () => disconnectPeer(pendingPeer, peerId)
        })
      } else if (offer) {
        const myOffer = pendingOffers[peerId]?.[relayId]

        if (myOffer && selfId > peerId) {
          return
        }

        const newPeer = initPeer(false, peerConfig)

        let plainOffer

        try {
          plainOffer = await decryptDescription(offer)
        } catch {
          handleJoinError(peerId, 'offer')
          return
        }

        if (newPeer.isDead) {
          return
        }

        const topic = await sha1(topicPath(rootTopicPlaintext, peerId))

        const sendSignal = async signal => {
          const payload = await formatSignal(signal)

          if (payload) {
            signalPeer(topic, toJson({peerId: selfId, ...payload}))
          }
        }

        newPeer.setHandlers({
          signal: signal => sendSignal(signal),
          connect: () => connectPeer(newPeer, peerId, relayId),
          close: () => disconnectPeer(newPeer, peerId)
        })

        await newPeer.signal(plainOffer)
      } else if (answer) {
        let plainAnswer

        try {
          plainAnswer = await decryptDescription(answer)
        } catch (e) {
          handleJoinError(peerId, 'answer')
          return
        }

        if (peer) {
          peer.setHandlers({
            connect: () => connectPeer(peer, peerId, relayId),
            close: () => disconnectPeer(peer, peerId)
          })

          peer.signal(plainAnswer)
        } else {
          const peer = pendingOffers[peerId]?.[relayId]

          if (peer && !peer.isDead) {
            peer.signal(plainAnswer)
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
      }
    ))
  }
}
