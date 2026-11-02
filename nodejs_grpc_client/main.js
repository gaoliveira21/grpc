import grpc from '@grpc/grpc-js'
import protoLoader from '@grpc/proto-loader'

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

const proto = grpc.loadPackageDefinition(packageDefinition).stock_market

function serverStreaming(client, deadline) {
  const body = { symbol: 'BBAS3' }

  client.getStockPrice(body, { deadline }, (err, res) => {
    if (err) {
      console.error(err)
      return
    }
    console.log('Unary response ', res)
  })

  const serverStream = client.getStockPriceStreaming(body, { deadline });

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

async function main() {
  const client = new proto.StockPrice(SERVER_ADDR, grpc.credentials.createInsecure())

  const deadline = new Date()
  deadline.setSeconds(deadline.getSeconds() + 8)

  serverStreaming(client, deadline)
  clientStreaming(client, deadline)

}

await main()