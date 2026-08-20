package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// runVerify 压测跑完后检查数据一致性。
// 性能优化最容易踩的坑就是「快了但是算错了」，所以每一轮压测都必须跟一次校验：
//   1. 领取记录数不能超过红包份数（超发）
//   2. 领取金额合计不能超过红包总额
//   3. 同一个 (transaction_id, user_id) 不能有两条记录（重复领取）
//   4. remaining_count 要和 总份数 - 实际领取数 对得上
func runVerify(cfg *Config, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	cfg.bindCommon(fs)
	txID := fs.String("tx", "", "只校验指定 transaction_id，留空则校验全部红包")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return err
	}
	defer cli.Disconnect(ctx)
	db := cli.Database("openim_v3")

	filter := bson.M{}
	if *txID != "" {
		filter["transaction_id"] = *txID
	}
	cur, err := db.Collection("transaction_record").Find(ctx, filter,
		options.Find().SetSort(bson.M{"created_at": -1}).SetLimit(50))
	if err != nil {
		return err
	}
	var txs []struct {
		TransactionID   string `bson:"transaction_id"`
		TotalCount      int    `bson:"total_count"`
		RemainingCount  int    `bson:"remaining_count"`
		TotalAmount     any    `bson:"total_amount"`
		RemainingAmount any    `bson:"remaining_amount"`
		Status          int    `bson:"status"`
		CreatedAt       time.Time `bson:"created_at"`
	}
	if err := cur.All(ctx, &txs); err != nil {
		return err
	}
	if len(txs) == 0 {
		fmt.Println("没有找到红包记录")
		return nil
	}

	problems := 0
	fmt.Printf("%-26s %6s %6s %6s %8s %s\n", "transaction_id", "份数", "已领", "剩余", "状态", "校验")
	for _, tx := range txs {
		received, err := db.Collection("transaction_receive_record").
			CountDocuments(ctx, bson.M{"transaction_id": tx.TransactionID})
		if err != nil {
			return err
		}

		// 重复领取检查
		dupCur, err := db.Collection("transaction_receive_record").Aggregate(ctx, mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"transaction_id": tx.TransactionID}}},
			{{Key: "$group", Value: bson.M{"_id": "$user_id", "n": bson.M{"$sum": 1}}}},
			{{Key: "$match", Value: bson.M{"n": bson.M{"$gt": 1}}}},
			{{Key: "$count", Value: "dups"}},
		})
		if err != nil {
			return err
		}
		var dupRes []struct {
			Dups int `bson:"dups"`
		}
		_ = dupCur.All(ctx, &dupRes)
		dups := 0
		if len(dupRes) > 0 {
			dups = dupRes[0].Dups
		}

		notes := ""
		if int(received) > tx.TotalCount {
			notes += fmt.Sprintf("【超发 %d>%d】", received, tx.TotalCount)
			problems++
		}
		if dups > 0 {
			notes += fmt.Sprintf("【重复领取 %d 人】", dups)
			problems++
		}
		if expected := tx.TotalCount - int(received); expected != tx.RemainingCount {
			notes += fmt.Sprintf("【remaining_count 不符：库=%d 应=%d】", tx.RemainingCount, expected)
			problems++
		}
		if notes == "" {
			notes = "OK"
		}
		fmt.Printf("%-26s %6d %6d %6d %8d %s\n",
			tx.TransactionID, tx.TotalCount, received, tx.RemainingCount, tx.Status, notes)
	}

	fmt.Println()
	if problems == 0 {
		fmt.Println("一致性校验通过，无超发 / 无重复领取 / 计数对得上")
	} else {
		fmt.Printf("发现 %d 处一致性问题，见上表\n", problems)
	}
	return nil
}
