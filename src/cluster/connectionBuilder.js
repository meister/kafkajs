const Connection = require('../network/connection')
const { KafkaJSNonRetriableError } = require('../errors')
const shuffle = require('../utils/shuffle')

const validateBrokers = brokers => {
  if ((!brokers || brokers.length === 0) && typeof brokers !== 'function') {
    throw new KafkaJSNonRetriableError(`Failed to connect: expected brokers array and got nothing`)
  }
}

module.exports = ({
  socketFactory,
  brokers,
  ssl,
  sasl,
  clientId,
  requestTimeout,
  enforceRequestTimeout,
  connectionTimeout,
  maxInFlightRequests,
  retry,
  logger,
  instrumentationEmitter = null,
}) => {
  validateBrokers(brokers)

  let dynamicBrokers = false
  let shuffledBrokers

  if (typeof brokers === 'function') {
    dynamicBrokers = true
  } else {
    shuffledBrokers = shuffle(brokers)
  }

  const size = brokers.length
  let index = 0

  return {
    build: ({ host, port, rack } = {}) => {
      let wrapper

      if (dynamicBrokers) {
        wrapper = fn =>
          new Promise((resolve, reject) => {
            Promise.resolve(brokers())
              .then(brokerList => {
                const [seedHost, seedPort] = shuffle(brokerList)[0].split(':')
                host = seedHost
                port = Number(seedPort)

                resolve(fn(host, port))
              })
              .catch(e => reject(e))
          })
      } else {
        if (!host) {
          // Always rotate the seed broker
          const [seedHost, seedPort] = shuffledBrokers[index++ % size].split(':')
          host = seedHost
          port = Number(seedPort)
        }
        wrapper = fn => fn(host, port)
      }

      return wrapper(
        (host, port) =>
          new Connection({
            host,
            port,
            rack,
            ssl,
            sasl,
            clientId,
            socketFactory,
            connectionTimeout,
            requestTimeout,
            enforceRequestTimeout,
            maxInFlightRequests,
            instrumentationEmitter,
            retry,
            logger,
          })
      )
    },
  }
}
