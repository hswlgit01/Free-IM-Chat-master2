package dto

type PostBalanceReq struct {
	PayPwd string `json:"pay_pwd" binding:"required"`
}
