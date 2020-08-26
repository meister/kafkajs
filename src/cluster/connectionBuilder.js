const Connection = require('../network/connection')
const { KafkaJSNonRetriableError, KafkaJSConnectionError } = require('../errors')
const shuffle = require('../utils/shuffle')

const validateBrokers = brokers => {
  if (!brokers || brokers.length === 0) {
    throw new KafkaJSNonRetriableError(`Failed to connect: expected brokers array and got nothing`)
  }
}

module.exports = ({
  socketFactory,
  discovery,
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
  if (discovery) {
    const getConnection = async () => {
      const discovered = await discovery()

      if (!discovered || !discovered.brokers || discovered.brokers.length === 0) {
        throw new KafkaJSConnectionError(`discovery failed, expected brokers as array!`)
      }

      // take random from fresh discovered brokers list
      const [host, port] = shuffle(discovered.brokers)[0].split(':')

      return new Connection({
        host,
        port: Number(port),
        ssl,
        sasl: discovered.sasl || sasl,
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
    }

    return {
      build: getConnection,
    }
  } else {
    validateBrokers(brokers)

    const shuffledBrokers = shuffle(brokers)
    const size = brokers.length
    let index = 0

    return {
      build: ({ host, port, rack } = {}) => {
        if (!host) {
          // Always rotate the seed broker
          const [seedHost, seedPort] = shuffledBrokers[index++ % size].split(':')
          host = seedHost
          port = Number(seedPort)
        }

        return new Connection({
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
      },
    }
  }
}
