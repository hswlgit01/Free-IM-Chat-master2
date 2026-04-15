// Copyright © 2023 OpenIM open source community. All rights reserved.
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

package chat

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OrganizationUserRole string

const (
	OrganizationUserSuperAdminRole   OrganizationUserRole = "SuperAdmin"
	OrganizationUserBackendAdminRole OrganizationUserRole = "BackendAdmin"
	OrganizationUserGroupManagerRole OrganizationUserRole = "GroupManager"
	OrganizationUserTermManagerRole  OrganizationUserRole = "TermManager"
	OrganizationUserNormalRole       OrganizationUserRole = "Normal"
)

type OrganizationUserStatus string

const (
	OrganizationUserDisableStatus OrganizationUserStatus = "Disable"
	OrganizationUserEnableStatus  OrganizationUserStatus = "Enable"
)

type OrganizationUserRegisterType string

const (
	OrganizationUserRegisterTypeH5      OrganizationUserRegisterType = "h5"
	OrganizationUserRegisterTypeBackend OrganizationUserRegisterType = "backend"
)

type OrganizationUserInviterType string

const (
	OrganizationUserInviterTypeOrg     OrganizationUserInviterType = "org"
	OrganizationUserInviterTypeOrgUser OrganizationUserInviterType = "orgUser"
)

type OrganizationUserType string

const (
	OrganizationUserTypeUser         OrganizationUserType = "USER"
	OrganizationUserTypeOrganization OrganizationUserType = "ORGANIZATION"
)

type OrganizationUser struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrganizationId primitive.ObjectID     `bson:"organization_id" json:"organization_id"`
	ThirdUserId    string                 `bson:"third_user_id" json:"third_user_id"`
	UserId         string                 `bson:"user_id" json:"user_id"`
	UserType       OrganizationUserType   `bson:"user_type" json:"user_type"`
	Role           OrganizationUserRole   `bson:"role" json:"role"`
	Status         OrganizationUserStatus `bson:"status" json:"status"`

	ImServerUserId string                       `bson:"im_server_user_id" json:"im_server_user_id"`
	InvitationCode string                       `bson:"invitation_code" json:"invitation_code"`
	RegisterType   OrganizationUserRegisterType `bson:"register_type" json:"register_type"`

	Inviter     string                      `bson:"inviter" json:"inviter"`
	InviterType OrganizationUserInviterType `bson:"inviter_type" json:"inviter_type"`

	// Hierarchy fields
	AncestorPath        []string `bson:"ancestor_path" json:"ancestor_path"`                 // Full path of ancestors (user_ids)
	Level               int      `bson:"level" json:"level"`                                 // User's level in the hierarchy (1-based)
	Level1Parent        string   `bson:"level1_parent" json:"level1_parent"`                 // Direct parent (level 1 up)
	Level2Parent        string   `bson:"level2_parent" json:"level2_parent"`                 // Grandparent (level 2 up)
	Level3Parent        string   `bson:"level3_parent" json:"level3_parent"`                 // Great-grandparent (level 3 up)
	TeamSize            int      `bson:"team_size" json:"team_size"`                         // Total number of users in downline
	DirectDownlineCount int      `bson:"direct_downline_count" json:"direct_downline_count"` // Count of direct downline members

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (OrganizationUser) TableName() string {
	return "organization_user"
}

type OrganizationUserInterface interface {
	Create(ctx context.Context, organizationUser ...*OrganizationUser) error
	Find(ctx context.Context, userIds []string) ([]*OrganizationUser, error)
	FindByImServerUserIds(ctx context.Context, imServerUserIds []string) ([]*OrganizationUser, error)
	Take(ctx context.Context, userID string) (*OrganizationUser, error)
	TakeByOrgid(ctx context.Context, userID, orgid string) (*OrganizationUser, error)
	Update(ctx context.Context, userID string, data map[string]any) error
	Delete(ctx context.Context, userIDs []string) error

	// Hierarchy-related methods
	FindByInvitationCode(ctx context.Context, invitationCode string) (*OrganizationUser, error)
	FindDirectDownline(ctx context.Context, userID string, page, size int64) ([]*OrganizationUser, int64, error)
	FindByAncestor(ctx context.Context, ancestorID string, page, size int64) ([]*OrganizationUser, int64, error)
	UpdateHierarchy(ctx context.Context, userID string, hierarchyData map[string]any) error
	IncrementTeamSize(ctx context.Context, userIDs []string, increment int) error
	FindByLevel(ctx context.Context, level int, page, size int64) ([]*OrganizationUser, int64, error)
}
