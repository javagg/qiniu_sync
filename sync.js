#!/usr/bin/env node

var argv = require('optimist').argv
var sh = require('execSync')
var fs = require('fs')
var path = require('path')
var Hash = require('hashish');
var wrench = require("wrench");
var qiniu = require('qiniu')
var download = require('download')
var url = require('url')

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
var pull = argv.pull != undefined
var keypair = require(argv.keyfile)
qiniu.conf.ACCESS_KEY = keypair.access_key
qiniu.conf.SECRET_KEY = keypair.secret_key
var bucket = argv.bucket

var domain = JSON.parse(sh.exec("/usr/bin/qboxrsctl bucketinfo " + bucket).stdout)["bind_domains"][0]
var qetag = '/usr/bin/qetag' || argv.qetagpath

function compareStorage(local, remote, pull, callback) {
    var updates = []
    var creates = []
    var deletes = []
    var noops = []

    var gets = []
    var puts = []

    function compare(storage1, storage2) {
        Hash(storage1).forEach(function(item, key) {
            var remoteItem = storage2[key]
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
        deletes = Hash(storage2).keys.diff(Hash(storage1).keys)
    }

    if (pull) {
        compare(remote, local)
        gets = creates.concat(updates)
    } else {
        compare(local, remote)
        puts = creates.concat(updates)
    }
    callback(gets, puts, deletes, pull, noops)
}

function handleList(items) {
    remoteItems = {}
    items.forEach(function(item) {
        remoteItems[item.key] = item
    })

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

//    console.log(remoteItems)
//    console.log(localItems)

    compareStorage(localItems, remoteItems, pull, function(gets, puts, deletes, pull, noops) {
        console.log("gets:" + gets)
        console.log("puts:" + puts)
        console.log("deletes:" + deletes)
        console.log("noops:" + noops)
        console.log("pull:" + pull)
        doFileOps(gets, puts, deletes, pull)
    })
}

function doFileOps(gets, puts, deletes, pull) {
    // Perform deletes
    if (pull) {
        deletes.forEach(function(key) {
            key = path.normalize(key)
            var dirs = [key]
            var p = path.dirname(key)
            while ((p != '.') && (p != '/')) {
                dirs.push(p)
                p = path.dirname(p)
            }
//            dirs.forEach(function(dir) {
//                fs.unlinkSync(dir)
//            })
        })
    } else {
        var deleteEntries = deletes.map(function(i) {
            return new qiniu.rs.EntryPath(bucket, i);
        })._toHash()

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
    }

    // Perform puts
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

    function normalUrl(str) {
        var o = url.parse(str)
        var r = o.protocol
        r += "//"
        if (o.auth) r += o.auth
        r += o.host + o.pathname.replace("%2F", "/")
        return r
    }

    // Perform gets
    gets.forEach(function(key) {
        var baseUrl = qiniu.rs.makeBaseUrl(domain, key);
        var policy = new qiniu.rs.GetPolicy();
//        var dlUrl = normalUrl(policy.makeRequest(baseUrl))
        var dlUrl = normalUrl(baseUrl)
        var destDir = path.join(syncDir, path.dirname(key))
        download(dlUrl, destDir)
    })
}


qiniu.rsf.listPrefix(bucket, null, null, null, function(err, ret) {
    if (!err) {
        handleList(ret.items)
    } else {
        console.log(err)
    }
});

