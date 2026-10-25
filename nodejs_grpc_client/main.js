import grpc from '@grpc/grpc-js'
import protoLoader from '@grpc/proto-loader'

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

async function main() {
  const client = new proto.StockPrice(SERVER_ADDR, grpc.credentials.createInsecure())

  const deadline = new Date()
  deadline.setSeconds(deadline.getSeconds() + 8)

  const body = { symbol: 'BBAS3' }

  client.getStockPrice(body, { deadline }, (err, res) => {
    if (err) {
      console.error(err)
      return
    }
    console.log('Unary response ', res)
  })

  const stream = client.getStockPriceStreaming(body, { deadline });

  stream.on('data', (res) => {
    console.log('Streaming response ', res)
  })

  stream.on('error', (err) => {
    console.error(err)
  })

  stream.on('status', (status) => {
    console.log('status', status)
  })

  stream.on('end', () => console.log('server streaming ended'))
}

await main()