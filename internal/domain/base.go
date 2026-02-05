package domain

import "time"

type Identifiable interface {
	GetGUID() string
	GetType() string
}

type BaseEntity struct {
	GUID      string    `json:"guid"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (b BaseEntity) GetGUID() string {
	return b.GUID
}