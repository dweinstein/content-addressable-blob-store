var blobs = require('./')
var AWS = require('aws-sdk')
var store = blobs({bucket: process.env.S3_BUCKET || 'mybucket', s3: new AWS.S3()})

var w = store.createWriteStream()

w.write('hello ')
w.write('world\n')

w.end(function() {
  console.log('blob written: '+w.key)
  store.createReadStream(w).pipe(process.stdout)
})
