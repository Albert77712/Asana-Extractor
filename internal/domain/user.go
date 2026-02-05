package domain

type User struct {
	BaseEntity
	Email       string   `json:"email"`
	DisplayName string   `json:"display_name"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
	OrgID       string   `json:"org_id"`
}

func (u User) GetType() string {
	return "user"
}