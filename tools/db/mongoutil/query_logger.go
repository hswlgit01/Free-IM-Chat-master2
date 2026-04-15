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

package mongoutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

var (
	queryLogger     *MongoQueryLogger
	queryLoggerOnce sync.Once
)

// MongoQueryLogger MongoDB 查询日志记录器
type MongoQueryLogger struct {
	logFile         *os.File
	mu              sync.Mutex
	enabled         bool
	logPath         string
	currentDate     string // 当前日志文件的日期
	retainFileCount int    // 保留的日志文件数量
}

// QueryLogEntry 查询日志条目
type QueryLogEntry struct {
	Timestamp     string `json:"timestamp"`
	Collection    string `json:"collection"`
	Operation     string `json:"operation"`
	CompleteQuery string `json:"complete_query"`
	Duration      string `json:"duration"`
	Error         string `json:"error,omitempty"`
}

var logDir = "../../../../logs/"

// InitMongoQueryLogger 初始化 MongoDB 查询日志记录器
func InitMongoQueryLogger() error {
	queryLoggerOnce.Do(func() {
		// 直接模仿 openim-chat-log.2025-07-13 的实现方式
		// 一天一个文件，不读配置文件

		// 创建目录
		if err := os.MkdirAll(logDir, 0755); err != nil {
			fmt.Printf("Failed to create MongoDB query log directory: %v\n", err)
			return
		}

		// 生成按天分割的文件名：mongodb-query.2025-07-13.log
		dateStr := time.Now().Format("2006-01-02")
		logFileName := fmt.Sprintf("mongodb-query.%s.log", dateStr)
		logPath := filepath.Join(logDir, logFileName)

		// 打开日志文件（追加模式）
		file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Failed to open MongoDB query log file: %v\n", err)
			return
		}

		// 从环境变量获取保留文件数量，默认为2
		retainCount := 2
		if envRetainCount := os.Getenv("MONGODB_QUERY_LOG_RETAIN_COUNT"); envRetainCount != "" {
			if count, err := fmt.Sscanf(envRetainCount, "%d", &retainCount); err != nil || count != 1 {
				fmt.Printf("Invalid MONGODB_QUERY_LOG_RETAIN_COUNT: %s, using default: 2\n", envRetainCount)
				retainCount = 2
			}
		}

		queryLogger = &MongoQueryLogger{
			logFile:         file,
			enabled:         true,
			logPath:         logPath,
			currentDate:     dateStr,
			retainFileCount: retainCount,
		}

		// 获取绝对路径用于显示
		absPath, _ := filepath.Abs(logPath)
		fmt.Printf("MongoDB查询日志已初始化，日志文件路径: %s\n", absPath)

		// 清理旧的日志文件
		queryLogger.cleanupOldLogFiles()

		// 写入初始化日志
		initEntry := QueryLogEntry{
			Timestamp:     time.Now().Format("2006-01-02 15:04:05.000"),
			Operation:     "LOGGER_INIT",
			CompleteQuery: fmt.Sprintf("# MongoDB查询日志已启动，日志文件: %s", absPath),
		}
		queryLogger.writeLog(initEntry)
	})

	return nil
}

// GetMongoQueryLogger 获取查询日志记录器实例
func GetMongoQueryLogger() *MongoQueryLogger {
	if queryLogger == nil {
		InitMongoQueryLogger()
	}
	return queryLogger
}

// LogQuery 记录查询日志
func (l *MongoQueryLogger) LogQuery(ctx context.Context, collection string, operation string, filter interface{}, pipeline interface{}, update interface{}, options interface{}, duration time.Duration, err error) {
	if l == nil || !l.enabled {
		return
	}

	// 构建完整的查询字符串，传入实际的集合名称
	completeQuery := buildCompleteQuery(collection, operation, filter, pipeline, update, options)

	// 将时间统一转换为毫秒格式
	durationMs := float64(duration.Nanoseconds()) / 1000000.0
	var durationStr string
	if durationMs >= 1000 {
		// 如果超过1秒，显示为秒
		durationStr = fmt.Sprintf("%.3fs", durationMs/1000)
	} else {
		// 显示为毫秒
		durationStr = fmt.Sprintf("%.3fms", durationMs)
	}

	if len(completeQuery) > 1500 {
		completeQuery = completeQuery[:1500] + "..."
	}

	entry := QueryLogEntry{
		Timestamp:     time.Now().Format("2006-01-02 15:04:05.000"),
		Collection:    collection,
		Operation:     operation,
		CompleteQuery: completeQuery,
		Duration:      durationStr,
	}

	if err != nil {
		entry.Error = err.Error()
	}

	l.writeLog(entry)
}

// writeLog 写入日志到文件
func (l *MongoQueryLogger) writeLog(entry QueryLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.logFile == nil {
		return
	}

	// 检查是否需要切换日志文件（跨天）
	currentDate := time.Now().Format("2006-01-02")
	if currentDate != l.currentDate {
		// 关闭当前文件
		l.logFile.Close()

		logFileName := fmt.Sprintf("mongodb-query.%s.log", currentDate)
		newLogPath := filepath.Join(logDir, logFileName)

		newFile, err := os.OpenFile(newLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Failed to open new MongoDB query log file: %v\n", err)
			return
		}

		// 更新到新文件
		l.logFile = newFile
		l.logPath = newLogPath
		l.currentDate = currentDate

		fmt.Printf("MongoDB查询日志已切换到新文件: %s\n", newLogPath)

		// 清理旧的日志文件
		l.cleanupOldLogFiles()

		// 写入文件切换日志
		switchEntry := QueryLogEntry{
			Timestamp:     time.Now().Format("2006-01-02 15:04:05.000"),
			Operation:     "FILE_SWITCH",
			CompleteQuery: fmt.Sprintf("# 日志文件已切换到: %s", newLogPath),
		}
		switchData, _ := json.Marshal(switchEntry)
		l.logFile.WriteString(string(switchData) + "\n")
	}

	// 将日志条目转换为 JSON
	logData, err := json.Marshal(entry)
	if err != nil {
		fmt.Printf("Failed to marshal log entry: %v\n", err)
		return
	}

	// 写入文件，每行一个 JSON
	_, err = l.logFile.WriteString(string(logData) + "\n")
	if err != nil {
		fmt.Printf("Failed to write to log file: %v\n", err)
		return
	}

	// 立即刷新到磁盘
	l.logFile.Sync()
}

// sanitizeFilter 清理过滤器，移除敏感信息
func sanitizeFilter(filter interface{}) interface{} {
	// 如果是 bson.M 或 map，处理敏感字段
	if filterMap, ok := filter.(bson.M); ok {
		sanitized := make(bson.M)
		for k, v := range filterMap {
			// 隐藏密码等敏感字段
			if k == "password" || k == "token" || k == "secret" {
				sanitized[k] = "***"
			} else {
				sanitized[k] = v
			}
		}
		return sanitized
	}
	return filter
}

// sanitizePipeline 清理聚合管道
func sanitizePipeline(pipeline interface{}) interface{} {
	// 对于聚合管道，保持原样（可以根据需要添加敏感信息处理）
	return pipeline
}

// sanitizeUpdate 清理更新数据
func sanitizeUpdate(update interface{}) interface{} {
	// 如果是 bson.M 或 map，处理敏感字段
	if updateMap, ok := update.(bson.M); ok {
		sanitized := make(bson.M)
		for k, v := range updateMap {
			if k == "$set" {
				if setMap, ok := v.(bson.M); ok {
					sanitizedSet := make(bson.M)
					for setK, setV := range setMap {
						if setK == "password" || setK == "token" || setK == "secret" {
							sanitizedSet[setK] = "***"
						} else {
							sanitizedSet[setK] = setV
						}
					}
					sanitized[k] = sanitizedSet
				} else {
					sanitized[k] = v
				}
			} else {
				sanitized[k] = v
			}
		}
		return sanitized
	}
	return update
}

// sanitizeOptions 清理选项
func sanitizeOptions(options interface{}) interface{} {
	// 保持选项原样
	return options
}

// Close 关闭日志记录器
func (l *MongoQueryLogger) Close() error {
	if l == nil || l.logFile == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	return l.logFile.Close()
}

// SetEnabled 启用或禁用查询日志
func (l *MongoQueryLogger) SetEnabled(enabled bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = enabled
}

// buildCompleteQuery 构建可以直接在MongoDB Shell中执行的查询字符串
func buildCompleteQuery(collection string, operation string, filter interface{}, pipeline interface{}, update interface{}, options interface{}) string {

	// 处理不同类型的查询，生成标准MongoDB Shell语法
	switch operation {
	case "FIND":
		return buildFindQuery(collection, filter, options)

	case "FIND_ONE":
		return buildFindOneQuery(collection, filter, options)

	case "FIND_PAGE", "FIND_PAGE_ONLY":
		return buildFindQuery(collection, filter, options)

	case "COUNT":
		return buildCountQuery(collection, filter)

	case "UPDATE_ONE":
		return buildUpdateOneQuery(collection, filter, update, options)

	case "UPDATE_MANY":
		return buildUpdateManyQuery(collection, filter, update, options)

	case "UPDATE_ONE_RESULT":
		return buildUpdateOneQuery(collection, filter, update, options)

	case "DELETE_ONE":
		return buildDeleteOneQuery(collection, filter, options)

	case "DELETE_MANY":
		return buildDeleteManyQuery(collection, filter, options)

	case "AGGREGATE":
		return buildAggregateQuery(collection, pipeline, options)

	case "INSERT_MANY":
		return buildInsertManyQuery(collection, options)

	case "FIND_ONE_AND_UPDATE":
		return buildFindOneAndUpdateQuery(collection, filter, update, options)

	default:
		return fmt.Sprintf("db.%s.%s()", collection, strings.ToLower(operation))
	}
}

// buildFindQuery 构建find查询
func buildFindQuery(collection string, filter interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.find(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	query += ")"

	// 处理options中的链式方法
	if options != nil {
		query += buildFindChainMethods(options)
	}

	return query
}

// buildFindOneQuery 构建findOne查询
func buildFindOneQuery(collection string, filter interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.findOne(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	// findOne可以有第二个参数projection
	if options != nil {
		query += buildFindOneOptions(options)
	}

	query += ")"

	return query
}

// buildCountQuery 构建count查询
func buildCountQuery(collection string, filter interface{}) string {
	query := fmt.Sprintf("db.%s.countDocuments(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	query += ")"

	return query
}

// buildUpdateOneQuery 构建updateOne查询
func buildUpdateOneQuery(collection string, filter interface{}, update interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.updateOne(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	query += ", "

	if update != nil {
		query += toJSONString(sanitizeUpdate(update))
	} else {
		query += "{}"
	}

	// 添加options参数
	if options != nil {
		optionsStr := toJSONString(sanitizeOptions(options))
		if optionsStr != "null" && optionsStr != "{}" {
			query += ", " + optionsStr
		}
	}

	query += ")"

	return query
}

// buildUpdateManyQuery 构建updateMany查询
func buildUpdateManyQuery(collection string, filter interface{}, update interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.updateMany(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	query += ", "

	if update != nil {
		query += toJSONString(sanitizeUpdate(update))
	} else {
		query += "{}"
	}

	// 添加options参数
	if options != nil {
		optionsStr := toJSONString(sanitizeOptions(options))
		if optionsStr != "null" && optionsStr != "{}" {
			query += ", " + optionsStr
		}
	}

	query += ")"

	return query
}

// buildDeleteOneQuery 构建deleteOne查询
func buildDeleteOneQuery(collection string, filter interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.deleteOne(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	query += ")"

	return query
}

// buildDeleteManyQuery 构建deleteMany查询
func buildDeleteManyQuery(collection string, filter interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.deleteMany(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	query += ")"

	return query
}

// buildAggregateQuery 构建aggregate查询
func buildAggregateQuery(collection string, pipeline interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.aggregate(", collection)

	if pipeline != nil {
		query += toJSONString(sanitizePipeline(pipeline))
	} else {
		query += "[]"
	}

	// 添加options参数
	if options != nil {
		optionsStr := toJSONString(sanitizeOptions(options))
		if optionsStr != "null" && optionsStr != "{}" {
			query += ", " + optionsStr
		}
	}

	query += ")"

	return query
}

// buildInsertManyQuery 构建insertMany查询
func buildInsertManyQuery(collection string, options interface{}) string {
	return fmt.Sprintf("db.%s.insertMany([...])", collection)
}

// buildFindOneAndUpdateQuery 构建findOneAndUpdate查询
func buildFindOneAndUpdateQuery(collection string, filter interface{}, update interface{}, options interface{}) string {
	query := fmt.Sprintf("db.%s.findOneAndUpdate(", collection)

	if filter != nil {
		query += toJSONString(sanitizeFilter(filter))
	} else {
		query += "{}"
	}

	query += ", "

	if update != nil {
		query += toJSONString(sanitizeUpdate(update))
	} else {
		query += "{}"
	}

	// 添加options参数
	if options != nil {
		optionsStr := toJSONString(sanitizeOptions(options))
		if optionsStr != "null" && optionsStr != "{}" {
			query += ", " + optionsStr
		}
	}

	query += ")"

	return query
}

// buildFindChainMethods 构建find查询的链式方法
func buildFindChainMethods(options interface{}) string {
	chainMethods := ""

	// 这里需要解析options中的具体字段
	// 由于options是interface{}，我们需要使用反射或类型断言来处理
	optionsStr := toJSONString(sanitizeOptions(options))

	// 尝试解析options JSON来构建链式方法
	if optionsStr != "null" && optionsStr != "{}" {
		// 简化处理：如果有options，添加一个通用的注释
		chainMethods += " /* with options: " + optionsStr + " */"
	}

	return chainMethods
}

// buildFindOneOptions 构建findOne的第二个参数
func buildFindOneOptions(options interface{}) string {
	optionsStr := toJSONString(sanitizeOptions(options))
	if optionsStr != "null" && optionsStr != "{}" {
		return ", " + optionsStr
	}
	return ""
}

// toJSONString 将对象转换为JSON字符串
func toJSONString(obj interface{}) string {
	if obj == nil {
		return "null"
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}

	return string(data)
}

// cleanupOldLogFiles 清理旧的日志文件，只保留最新的 retainFileCount 个文件
func (l *MongoQueryLogger) cleanupOldLogFiles() {
	// 获取日志目录中所有的mongodb-query日志文件
	pattern := filepath.Join(logDir, "mongodb-query.*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("Failed to list MongoDB query log files: %v\n", err)
		return
	}

	// 如果文件数量不超过保留数量，无需清理
	if len(files) <= l.retainFileCount {
		return
	}

	// 创建文件信息结构体用于排序
	type fileInfo struct {
		path    string
		modTime time.Time
	}

	var fileInfos []fileInfo
	for _, file := range files {
		if stat, err := os.Stat(file); err == nil {
			fileInfos = append(fileInfos, fileInfo{
				path:    file,
				modTime: stat.ModTime(),
			})
		}
	}

	// 按修改时间排序（旧的在前）
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].modTime.Before(fileInfos[j].modTime)
	})

	// 计算需要删除的文件数量
	deleteCount := len(fileInfos) - l.retainFileCount

	// 删除最旧的文件
	for i := 0; i < deleteCount; i++ {
		fileName := filepath.Base(fileInfos[i].path)
		if err := os.Remove(fileInfos[i].path); err != nil {
			fmt.Printf("Failed to delete old MongoDB query log file %s: %v\n", fileName, err)
		} else {
			fmt.Printf("已删除旧的MongoDB查询日志文件: %s (修改时间: %s)\n",
				fileName, fileInfos[i].modTime.Format("2006-01-02 15:04:05"))
		}
	}
}
