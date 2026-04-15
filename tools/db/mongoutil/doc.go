// Copyright © 2024 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package mongoutil 封装 Mongo 访问；经 mongoutil.Find/Aggregate 等发起的查询会记录条件（默认写入 logs 下 mongodb-query.*.log）。
//
// 查询条件日志：设置 MONGODB_QUERY_LOG_STDOUT=1 同时输出到应用日志（ZInfo，字段 coll/op/query/duration）；
// MONGODB_QUERY_LOG=off 关闭写文件；MONGODB_QUERY_LOG_DIR 指定文件目录；MONGODB_QUERY_LOG_RETAIN_COUNT 保留文件个数。
package mongoutil
