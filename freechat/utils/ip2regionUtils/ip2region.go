package ip2regionUtils

import (
	_ "embed"
	"errors"
	"net"
	"strings"

	"github.com/lionsoul2014/ip2region/binding/golang/xdb"
)

////go:embed db/*
//var Ip2RegionDB embed.FS

//go:embed ip2region.xdb
var Ip2RegionDB []byte

type SearchRespFormat struct {
	Country string `json:"country"` // 0

	Province string `json:"province"` // 2
	City     string `json:"city"`     // 3
}

func (s *SearchRespFormat) String() string {
	country := s.Country
	province := s.Province

	if country != "" && province != "" && s.City != "" {
		country = country + "-"
	}

	if province != "" && s.City != "" {
		province = province + "-"
	}

	return country + province + s.City
}

// GetCityByIP dawn 2026-07-03 异地登录限制：解析 IP 的市级归属地，取不到返回空字符串。
// ip 允许携带端口(ip:port)。解析失败/内网 IP 返回空，调用方据此决定不拦截，避免误锁。
func GetCityByIP(ip string) string {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(ip); err == nil && host != "" {
		ip = host
	}
	searcher, err := xdb.NewWithBuffer(Ip2RegionDB)
	if err != nil {
		return ""
	}
	defer searcher.Close()
	region, err := searcher.SearchByStr(ip)
	if err != nil {
		return ""
	}
	format, err := FormatSearchResp(region)
	if err != nil {
		return ""
	}
	return format.City
}

func FormatSearchResp(searchResp string) (*SearchRespFormat, error) {
	searchRespList := strings.Split(searchResp, "|")

	if len(searchRespList) < 4 {
		return nil, errors.New("ip format error: " + searchResp)
	}

	country := ""
	if searchRespList[0] != "0" {
		country = searchRespList[0]
	}

	province := ""
	if searchRespList[2] != "0" {
		province = searchRespList[2]
	}

	city := ""
	if searchRespList[3] != "0" {
		city = searchRespList[3]
	}

	res := &SearchRespFormat{
		Country:  country,
		Province: province,
		City:     city,
	}

	return res, nil
}
