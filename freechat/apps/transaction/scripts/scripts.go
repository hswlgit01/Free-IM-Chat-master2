package scripts

import (
	"embed"
)

//go:embed reserve_slot.lua confirm_reservation.lua
var ScriptsFS embed.FS

// GetReserveSlotScript 获取预留脚本内容
func GetReserveSlotScript() ([]byte, error) {
	return ScriptsFS.ReadFile("reserve_slot.lua")
}

// GetConfirmReservationScript 获取确认预留脚本内容
func GetConfirmReservationScript() ([]byte, error) {
	return ScriptsFS.ReadFile("confirm_reservation.lua")
}
