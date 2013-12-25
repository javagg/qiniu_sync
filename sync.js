#!/usr/bin/env node

var argv = require('optimist').argv
var sh = require('execSync')
var fs = require('fs')
var path = require('path')
var Hash = require('hashish');
var qiniu = require('qiniu')

Array.prototype.diff = function(a) {
    return this.filter(function(i) {
        return !(a.indexOf(i) > -1)
    })
}
Array.prototype._toHash = function() {
    var obj = {}
    for (var i = 0; i < this.length; i++) {
        obj[i] = this[i]
    }
    return obj
}


var syncDir = argv.syncdir
qiniu.conf.ACCESS_KEY = 'B3K5BEmyIPWFGjXKI1kfZ1KNt8OJi3SwjyvT6PzB';
qiniu.conf.SECRET_KEY = '-mE9JUtGpFr90H_3UNh8wWW6cI5aKEwruv2nfgxf';

var bucket = argv.bucket
var qetag = '/usr/bin/qetag' || argv.qetagpath


function handleList(items) {
    remoteItems = {}
    items.forEach(function(item) {
        remoteItems[item.key] = item
    })

    var wrench = require("wrench");
    var files = wrench.readdirSyncRecursive(syncDir);
    localItems = {}
    files.forEach(function(f) {
        var absolute_path = path.resolve(syncDir, f)
        var stat = fs.statSync(absolute_path)
        if (stat.isDirectory()) return
        var fsize = stat.size
        var hash = sh.exec(qetag + " " + absolute_path).stdout.trim()
        localItems[f] = { key: f, hash: hash, fsize: fsize}
    })

    console.log(remoteItems)
    console.log(localItems)

    var updates = []
    var creates = []
    var deletes = []
    var noops = []
    Hash(localItems).forEach(function(item, key) {
        var remoteItem = remoteItems[key]
        if (!remoteItem) {
            creates.push(key)
        } else {
            if (remoteItem.hash !== item.hash) {
                updates.push(key)
            } else {
                noops.push(key)
            }
        }
    })

    console.log("updates:" + updates)
    console.log("noops:" + noops)
    console.log("creates:" + creates)
    var gets = []
    var puts = creates.concat(updates)
    var deletes = Hash(remoteItems).keys.diff(Hash(localItems).keys)
    console.log("puts: "+ puts)
    console.log("deletes: "+ deletes)

    doFileOps(puts, gets, deletes)
}

function doFileOps(puts, gets, deletes) {
    var putEntries = puts.map(function(i) {
        return new qiniu.rs.EntryPath(bucket, i);
    })

    var getEntries = gets.map(function(i) {
        return new qiniu.rs.EntryPath(bucket, i);
    })

    var deleteEntries = deletes.map(function(i) {
        return new qiniu.rs.EntryPath(bucket, i);
    })

    deleteEntries = deleteEntries._toHash()
    var client = new qiniu.rs.Client();
    client.batchDelete(deleteEntries, function(err, ret) {
        if (!err) {

            for (i in ret) {
                if (ret[i].code !== 200) {
//                    console.log("[delete failed]" + ret[i].data)
                } else {
                    console.log("[delete succeed]" + ret[i].data)
                }
            }
        } else {
            console.log(err);
        }
    });

    var extra = new qiniu.io.PutExtra();
    var putPolicy = new qiniu.rs.PutPolicy(bucket);
    puts.forEach(function(key) {
        putPolicy.scope = bucket + ":" + key
        var uptoken = putPolicy.token();
        var localFile = path.resolve(syncDir, key)
        qiniu.io.putFile(uptoken, key, localFile, extra, function(err, ret) {
            if(!err) {
                console.log("[put succeed] " + ret.key);
            } else {
                console.log("[put failed]" + err);
            }
        });
    })
}

qiniu.rsf.listPrefix(bucket, null, null, null, function(err, ret) {
    if (!err) {
        handleList(ret.items)
    } else {
        console.log(err)
    }
});

