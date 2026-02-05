package domain

type Project struct {
	BaseEntity
	Name        string   `json:"name"`
	Description string   `json:"description"`
	
}

func (p Project) GetType() string {
	return "project"
}