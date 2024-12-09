package db

import "testing"

func TestSelect(t *testing.T) {
	Query().
		Table("users").
		Select("id", "name").
		Get()
}
