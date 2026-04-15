package dto

// GetGroupsByOrgIDReq 获取群组列表请求
type GetGroupsByOrgIDReq struct {
	GroupName  string `json:"groupName"`
	Pagination struct {
		PageNumber int `json:"pageNumber"`
		ShowNumber int `json:"showNumber"`
	} `json:"pagination"`
}
