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

  client.getStockPrice({ symbol: 'BBAS3' }, (err, res) => {
    if (err) {
      console.error(err)
      return
    }
    console.log(res)
  })
}

await main()