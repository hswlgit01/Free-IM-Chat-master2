package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// 连接到MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:37017"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// 查询属性表，根据手机号找到用户ID
	attrColl := client.Database("freechat").Collection("attribute")
	var attribute struct {
		UserID       string `bson:"user_id"`
		Account      string `bson:"account"`
		PhoneNumber  string `bson:"phone_number"`
		RegisterType int32  `bson:"register_type"`
		Nickname     string `bson:"nickname"`
	}

	err = attrColl.FindOne(ctx, bson.M{"phone_number": "17777777777"}).Decode(&attribute)
	if err != nil {
		// 尝试通过account查询
		err = attrColl.FindOne(ctx, bson.M{"account": "17777777777"}).Decode(&attribute)
		if err != nil {
			log.Printf("查询用户失败: %v\n", err)
			return
		}
	}

	fmt.Println("找到用户ID:", attribute.UserID)
	fmt.Println("用户账号:", attribute.Account)
	fmt.Println("用户昵称:", attribute.Nickname)

	// 查询组织用户表
	orgUserColl := client.Database("freechat").Collection("organization_user")
	var orgUser struct {
		OrganizationId      interface{} `bson:"organization_id"`
		UserId              string      `bson:"user_id"`
		Role                string      `bson:"role"`
		ImServerUserId      string      `bson:"im_server_user_id"`
		InvitationCode      string      `bson:"invitation_code"`
		Level               int         `bson:"level"`
		AncestorPath        []string    `bson:"ancestor_path"`
		Level1Parent        string      `bson:"level1_parent"`
		Level2Parent        string      `bson:"level2_parent"`
		Level3Parent        string      `bson:"level3_parent"`
		TeamSize            int         `bson:"team_size"`
		DirectDownlineCount int         `bson:"direct_downline_count"`
		Inviter             string      `bson:"inviter"`
		InviterType         string      `bson:"inviter_type"`
		CreatedAt           time.Time   `bson:"created_at"`
	}

	err = orgUserColl.FindOne(ctx, bson.M{"user_id": attribute.UserID}).Decode(&orgUser)
	if err != nil {
		log.Printf("查询组织用户失败: %v\n", err)
		return
	}

	fmt.Println("\n组织用户信息:")
	fmt.Printf("组织ID: %v\n", orgUser.OrganizationId)
	fmt.Printf("用户ID: %s\n", orgUser.UserId)
	fmt.Printf("角色: %s\n", orgUser.Role)
	fmt.Printf("邀请码: %s\n", orgUser.InvitationCode)
	fmt.Printf("层级: %d\n", orgUser.Level)
	fmt.Printf("祖先路径: %v\n", orgUser.AncestorPath)
	fmt.Printf("一级上级: %s\n", orgUser.Level1Parent)
	fmt.Printf("二级上级: %s\n", orgUser.Level2Parent)
	fmt.Printf("三级上级: %s\n", orgUser.Level3Parent)
	fmt.Printf("团队规模: %d\n", orgUser.TeamSize)
	fmt.Printf("直接下级数: %d\n", orgUser.DirectDownlineCount)
	fmt.Printf("邀请者: %s\n", orgUser.Inviter)
	fmt.Printf("邀请者类型: %s\n", orgUser.InviterType)
	fmt.Printf("创建时间: %s\n", orgUser.CreatedAt)

	// 如果有上级，查询上级信息
	if len(orgUser.AncestorPath) > 0 {
		fmt.Println("\n上级信息:")
		for i, ancestorId := range orgUser.AncestorPath {
			var ancestor struct {
				UserId      string    `bson:"user_id"`
				Level       int       `bson:"level"`
				InviterType string    `bson:"inviter_type"`
				CreatedAt   time.Time `bson:"created_at"`
			}

			err = orgUserColl.FindOne(ctx, bson.M{"user_id": ancestorId}).Decode(&ancestor)
			if err != nil {
				log.Printf("查询上级 %s 失败: %v\n", ancestorId, err)
				continue
			}

			fmt.Printf("上级 #%d (Lv%d): %s (创建于 %s, 邀请类型: %s)\n",
				i+1, ancestor.Level, ancestorId, ancestor.CreatedAt, ancestor.InviterType)
		}
	}

	// 查询用户属性是否有上级链路
	var attr struct {
		UserID   string `bson:"user_id"`
		Account  string `bson:"account"`
		Nickname string `bson:"nickname"`
	}

	fmt.Println("\n上级用户属性:")
	for i, ancestorId := range orgUser.AncestorPath {
		err = attrColl.FindOne(ctx, bson.M{"user_id": ancestorId}).Decode(&attr)
		if err != nil {
			log.Printf("查询上级属性 %s 失败: %v\n", ancestorId, err)
			continue
		}

		fmt.Printf("上级 #%d: UserID=%s, Account=%s, Nickname=%s\n",
			i+1, ancestorId, attr.Account, attr.Nickname)
	}

	// 尝试查询邀请者相关信息
	if orgUser.Inviter != "" && orgUser.InviterType == "orgUser" {
		var inviter struct {
			UserId       string   `bson:"user_id"`
			Level        int      `bson:"level"`
			AncestorPath []string `bson:"ancestor_path"`
		}

		err = orgUserColl.FindOne(ctx, bson.M{"user_id": orgUser.Inviter}).Decode(&inviter)
		if err != nil {
			// 尝试通过邀请码查询邀请者
			err = orgUserColl.FindOne(ctx, bson.M{"invitation_code": orgUser.Inviter}).Decode(&inviter)
			if err != nil {
				log.Printf("查询邀请者失败: %v\n", err)
			} else {
				fmt.Println("\n邀请者信息 (通过邀请码):")
				fmt.Printf("UserID: %s, 层级: %d, 上级路径: %v\n",
					inviter.UserId, inviter.Level, inviter.AncestorPath)
			}
		} else {
			fmt.Println("\n邀请者信息 (通过用户ID):")
			fmt.Printf("UserID: %s, 层级: %d, 上级路径: %v\n",
				inviter.UserId, inviter.Level, inviter.AncestorPath)
		}
	}
}
