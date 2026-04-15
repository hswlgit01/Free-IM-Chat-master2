package ip2regionUtils

import (
	_ "embed"
	"errors"
	"strings"
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
