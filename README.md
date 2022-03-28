# ravencoin-asset-stats
`node index.js --port port_for_api --daemon http://user:pass@localhost:8766 --dir "/path/to/where/you/want/to/store/stuff"`

API can be queried via a GET request made from the designated path, or a JSONRPC 2.0 POST request to the root /

* Current Height
  - GET /currentHeight
  - POST / method:"currentHeight" params:\[\]

* Timestamp given a height
  - GET /timestamp/{height}
  - POST / method:"timestamp" params:\[height\]

* Closest block to some timestamp
  - GET /closest_block/{timestamp}
  - POST / method:"closest_block" params:\[timestamp\]

* First block containing some asset
  - GET /first_block/{asset}
  - POST / method:"first_block" params:\[asset\]

* Last block containing some asset
  - GET /last_block/{asset}
  - POST / method:"last_block" params:\[asset\]

* Stats for an asset over blocks
  - GET /blockframe/{asset}/{from block}/{to block}
  - POST / method:"blockframe" params:\[asset, from block, to block\]

* Stats for an asset over time
  - GET /timeframe/{asset}/{from time}/{to time}
  - POST / method:"timeframe" params:\[asset, from time, to time\]

* Change in asset stats over blocks
  - GET /blockdelta/{asset}/{from block}/{to block}
  - POST / method:"blockdelta" params:\[asset, from block, to block\]

* Change in asset stats over time
  - GET /timedelta/{asset}/{from time}/{to time}
  - POST / method:"timedelta" params:\[asset, from time, to time\]

* Get stats of an asset at a block
  - GET /stats/{asset}
  - POST / method:"stats" params:\[asset, height\]
