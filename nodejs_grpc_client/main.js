import grpc from '@grpc/grpc-js'
import protoLoader from '@grpc/proto-loader'
import readline from 'node:readline'
import fs from 'node:fs'

const STOCK_PRICES_DATA = [
  { symbol: "ASDF4", price: 90 },
  { symbol: "PETR3", price: 100 },
  { symbol: "PETR4", price: 102 },
  { symbol: "TEST4", price: 150 }
]
const PROTO_PATH = '../stock_market/protos/stock_market.proto'
const SERVER_ADDR = 'localhost:8080'

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
})

const serviceConf = fs.readFileSync("service_config.json", 'utf-8')
const proto = grpc.loadPackageDefinition(packageDefinition).stock_market

function authInterceptor(options, next) {
  const requester = new grpc.RequesterBuilder()
    .withStart((metadata, listener, next) => {
      metadata.set('authorization', 'some-secret-token')
      next(metadata, listener)
    })
    .build()

  return new grpc.InterceptingCall(next(options), requester)
}

function unary(client, deadline) {
  client.getStockPrice({ symbol: 'BBAS3' }, { deadline }, (err, res) => {
    if (err) {
      console.error(err)
      return
    }
    console.log('Unary response ', res)
  })
}

function serverStreaming(client, deadline) {
  const serverStream = client.getStockPriceStreaming({ symbol: 'BBAS3' }, { deadline });

  serverStream.on('data', (res) => {
    console.log('Streaming response ', res)
  })

  serverStream.on('error', (err) => {
    console.error(err)
  })

  serverStream.on('status', (status) => {
    console.log('status', status)
  })

  serverStream.on('end', () => {
    console.log('server streaming ended')
  })
}

function clientStreaming(client, deadline) {
  const clientStream = client.updateStockPriceClientStreaming({ deadline }, (err, res) => {
    if (err) {
      console.error(err)
      return
    }

    console.log('Server streaming: ', res)
  });

  for (const msg of STOCK_PRICES_DATA) {
    clientStream.write(msg)
  }

  clientStream.end()
}

function bidirectionalStream(client) {
  const stream = client.getStockPriceBidirectionalStreaming()

  stream.on('data', res => console.log(res))
  stream.on('error', err => console.error(err))
  stream.on('end', () => console.log("streaming ended"))

  const rl = readline.createInterface({
    input: process.stdin,
    prompt: '<Digita a mensagem ou "sair" para sair>: ',
    output: process.stdout
  })

  rl.on('line', line => {
    if (line.toLowerCase() == "sair") {
      rl.close()
      return
    }

    stream.write({ symbol: line })
  })

  rl.on('close', () => stream.end())
}

async function main() {
  const client = new proto.StockPrice(SERVER_ADDR, grpc.credentials.createInsecure(), {
    'grpc.service_config': serviceConf,
    'interceptors': [authInterceptor]
  })

  const deadline = new Date()
  deadline.setSeconds(deadline.getSeconds() + 8)

  // unary(client, deadline)
  // serverStreaming(client, deadline)
  // clientStreaming(client, deadline)
  bidirectionalStream(client)

}

await main()