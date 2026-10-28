// Package mongo 按区服把实体写到不同的数据库。
// 一个逻辑节点会同时处理很多区服，库名必须带上 serverId，避免串数据。
package mongo
