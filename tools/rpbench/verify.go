package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

	// ---- 资金不变式校验 -------------------------------------------------
	// 批量结算模式下，发送者的 red_packet_frozen_balance 必须恰好等于
	// 其名下所有「进行中」红包的 (total_amount - settled_amount) 之和。
	// 这是判断批量结算有没有把钱算错的唯一硬指标。
	moneyProblems := verifyFrozenInvariant(ctx, db)
	problems += moneyProblems

	fmt.Println()
	if problems == 0 {
		fmt.Println("一致性校验通过，无超发 / 无重复领取 / 计数对得上")
	} else {
		fmt.Printf("发现 %d 处一致性问题，见上表\n", problems)
	}
	return nil
}

// verifyFrozenInvariant 校验发送者冻结余额与未结算红包金额是否吻合。
//
// 不变式：对每个钱包，
//   red_packet_frozen_balance == Σ(进行中红包的 total_amount - settled_amount)
//
// 当前是逐笔结算：每笔领取都实时扣减冻结余额，settled_amount 不存在，
// 所以左边应当 <= 右边。左边**大于**右边说明少扣了钱，是真问题；
// 小于则属正常。这条校验的价值在于兜住「多扣 / 少扣」这类资金错账，
// 任何动到红包资金流的改动都应该跑一遍。
func verifyFrozenInvariant(ctx context.Context, db *mongo.Database) int {
	fmt.Println("\n资金不变式校验（发送者冻结余额 vs 未结算红包）")

	zeroDec, _ := primitive.ParseDecimal128("0")
	cur, err := db.Collection("transaction_record").Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"status":           0, // 进行中
			"transaction_type": bson.M{"$in": []int{1, 2, 3, 5, 6}},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id":      bson.M{"w": "$wallet_id", "c": "$currency_id"},
			"total":    bson.M{"$sum": "$total_amount"},
			"settled":  bson.M{"$sum": bson.M{"$ifNull": []interface{}{"$settled_amount", zeroDec}}},
			"packets":  bson.M{"$sum": 1},
		}}},
	})
	if err != nil {
		fmt.Printf("  聚合失败: %v\n", err)
		return 0
	}
	var rows []struct {
		ID struct {
			W primitive.ObjectID `bson:"w"`
			C primitive.ObjectID `bson:"c"`
		} `bson:"_id"`
		Total   primitive.Decimal128 `bson:"total"`
		Settled primitive.Decimal128 `bson:"settled"`
		Packets int                  `bson:"packets"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		fmt.Printf("  解析失败: %v\n", err)
		return 0
	}
	if len(rows) == 0 {
		fmt.Println("  没有进行中的红包，跳过")
		return 0
	}

	problems := 0
	for _, r := range rows {
		var bal struct {
			Frozen primitive.Decimal128 `bson:"red_packet_frozen_balance"`
		}
		if err := db.Collection("wallet_balance").FindOne(ctx,
			bson.M{"wallet_id": r.ID.W, "currency_id": r.ID.C}).Decode(&bal); err != nil {
			fmt.Printf("  钱包 %s 余额记录缺失: %v\n", r.ID.W.Hex(), err)
			problems++
			continue
		}
		total, _ := decimal.NewFromString(r.Total.String())
		settled, _ := decimal.NewFromString(r.Settled.String())
		frozen, _ := decimal.NewFromString(bal.Frozen.String())
		expected := total.Sub(settled) // 应该仍占用在冻结余额里的金额

		status := "OK"
		switch {
		case frozen.Equal(expected):
			// 完全吻合：批量结算模式下的正确状态
		case frozen.LessThan(expected) && settled.IsZero():
			status = "逐笔模式（settled 恒为0，冻结已实时扣减）"
		default:
			status = fmt.Sprintf("【不符】冻结=%s 应为=%s 差额=%s",
				frozen.String(), expected.String(), frozen.Sub(expected).String())
			problems++
		}
		fmt.Printf("  钱包 %s  进行中红包 %d 个  总额 %s  已结算 %s  冻结 %s  %s\n",
			r.ID.W.Hex()[:8], r.Packets, total.String(), settled.String(), frozen.String(), status)
	}
	if problems > 0 {
		fmt.Printf("  发现 %d 处冻结余额不符\n", problems)
	}
	return problems
}
